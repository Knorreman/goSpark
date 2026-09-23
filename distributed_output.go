package spark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var ErrOutputCommitted = errors.New("output already committed")

// PartitionOutput identifies an immutable attempt file. Only the attempt
// referenced by _SUCCESS is part of the committed dataset.
type PartitionOutput struct {
	JobID       string `json:"job_id"`
	StageID     int    `json:"stage_id"`
	PartitionID int    `json:"partition_id"`
	Attempt     int    `json:"attempt"`
	Key         string `json:"key"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
}

type OutputManifest struct {
	Version    int               `json:"version"`
	JobID      string            `json:"job_id"`
	TaskName   string            `json:"task_name"`
	Partitions []PartitionOutput `json:"partitions"`
}

func attemptOutputKey(fs FileSystem, base, jobID string, stageID, partitionID, attempt int) string {
	return fs.Join(base, tempDirName, jobID, fmt.Sprintf("stage-%d", stageID),
		fmt.Sprintf("%s-attempt-%05d", PartitionFileName(partitionID), attempt))
}

func writeOutputPartition(ctx context.Context, root RDDAny, stage Stage, task Task) (result PartitionOutput, err error) {
	path := task.Job.Params["path"]
	if path == "" {
		return result, fmt.Errorf("save action requires params.path")
	}
	resolved := ResolvePath(path)
	fs := resolved.FS
	key := attemptOutputKey(fs, resolved.BasePath, task.JobID, stage.ID, task.PartitionID, task.Attempt)
	if err := fs.MkdirAll(fs.Join(resolved.BasePath, tempDirName, task.JobID, fmt.Sprintf("stage-%d", stage.ID)), 0755); err != nil {
		return result, err
	}
	part, ok := partitionByIndex(root, task.PartitionID)
	if !ok {
		return result, fmt.Errorf("partition %d not found", task.PartitionID)
	}
	w, err := fs.Create(key)
	if err != nil {
		return result, err
	}
	committed := false
	closed := false
	defer func() {
		if !closed {
			_ = w.Close()
		}
		if !committed {
			_ = fs.Remove(key)
		}
	}()
	h := sha256.New()
	writer := io.MultiWriter(w, h)
	iter := root.ComputeAny(part)
	var size int64
	var writeErr error
	for {
		if writeErr = ctx.Err(); writeErr != nil {
			break
		}
		v, more := iter()
		if !more {
			break
		}
		var n int
		n, writeErr = fmt.Fprintln(writer, v)
		size += int64(n)
		if writeErr != nil {
			break
		}
	}
	closeErr := w.Close()
	closed = true
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return result, writeErr
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	committed = true
	return PartitionOutput{JobID: task.JobID, StageID: stage.ID, PartitionID: task.PartitionID,
		Attempt: task.Attempt, Key: key, Bytes: size, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// CommitDistributedOutput verifies every selected attempt and publishes one
// conditional _SUCCESS manifest. It never renames/copies attempts into visible
// part-* names; readers must follow the manifest, not list _temporary.
func CommitDistributedOutput(path, jobID, taskName string, stageID, count int, outputs []PartitionOutput) (OutputManifest, error) {
	var empty OutputManifest
	if path == "" || jobID == "" || count < 0 || len(outputs) != count {
		return empty, fmt.Errorf("incomplete output: got %d want %d", len(outputs), count)
	}
	resolved := ResolvePath(path)
	fs := resolved.FS
	if err := fs.MkdirAll(resolved.BasePath, 0755); err != nil {
		return empty, err
	}
	byPart := make([]PartitionOutput, count)
	seen := make([]bool, count)
	for _, out := range outputs {
		if out.PartitionID < 0 || out.PartitionID >= count || seen[out.PartitionID] || out.Attempt < 0 || out.JobID != jobID || out.StageID != stageID || out.Bytes < 0 || len(out.SHA256) != 64 ||
			out.Key != attemptOutputKey(fs, resolved.BasePath, jobID, stageID, out.PartitionID, out.Attempt) {
			return empty, fmt.Errorf("invalid output for partition %d", out.PartitionID)
		}
		seen[out.PartitionID] = true
		r, err := fs.Open(out.Key)
		if err != nil {
			return empty, fmt.Errorf("missing attempt %d: %w", out.PartitionID, err)
		}
		h := sha256.New()
		n, readErr := io.Copy(h, r)
		closeErr := r.Close()
		if readErr != nil {
			return empty, readErr
		}
		if closeErr != nil {
			return empty, closeErr
		}
		if n != out.Bytes || hex.EncodeToString(h.Sum(nil)) != out.SHA256 {
			return empty, fmt.Errorf("corrupt attempt for partition %d", out.PartitionID)
		}
		byPart[out.PartitionID] = out
	}
	manifest := OutputManifest{Version: 1, JobID: jobID, TaskName: taskName, Partitions: byPart}
	data, err := json.Marshal(manifest)
	if err != nil {
		return empty, err
	}
	marker := fs.Join(resolved.BasePath, SuccessFileName)
	exclusive, ok := fs.(interface {
		CreateExclusive(string) (io.WriteCloser, error)
	})
	if !ok {
		return empty, fmt.Errorf("filesystem %s does not support conditional output commits", fs.Scheme())
	}
	w, err := exclusive.CreateExclusive(marker)
	if err == nil {
		_, writeErr := w.Write(data)
		if writeErr != nil {
			if abort, ok := w.(interface{ Abort() error }); ok {
				_ = abort.Abort()
			} else {
				_ = w.Close()
			}
		} else {
			writeErr = w.Close()
		}
		if writeErr == nil {
			return manifest, nil
		}
		err = writeErr
	}
	// An identical retry is idempotent; a different job may never replace the
	// first successful commit. Do not remove _SUCCESS after a failed Close: on
	// object stores it may already have become visible.
	r, openErr := fs.Open(marker)
	if openErr == nil {
		previous, readErr := io.ReadAll(r)
		r.Close()
		if readErr == nil && bytes.Equal(previous, data) {
			return manifest, nil
		}
		return empty, fmt.Errorf("%w: %s", ErrOutputCommitted, path)
	}
	return empty, fmt.Errorf("publish _SUCCESS: %w", err)
}

func ReadCommittedOutput(path string) (OutputManifest, error) {
	resolved := ResolvePath(path)
	r, err := resolved.FS.Open(resolved.FS.Join(resolved.BasePath, SuccessFileName))
	if err != nil {
		return OutputManifest{}, err
	}
	defer r.Close()
	var m OutputManifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return m, err
	}
	if m.Version != 1 {
		return m, fmt.Errorf("unsupported output version %d", m.Version)
	}
	return m, nil
}

// SelectedOutputs returns the accepted partition attempts in index order.
func SelectedOutputs(results []ExecResult, expected int) ([]PartitionOutput, error) {
	if len(results) != expected {
		return nil, fmt.Errorf("incomplete output results")
	}
	selected := make([]PartitionOutput, expected)
	for i, r := range results {
		if r.Output == nil || r.PartitionID != i {
			return nil, fmt.Errorf("missing output for partition %d", i)
		}
		selected[i] = *r.Output
	}
	return selected, nil
}
