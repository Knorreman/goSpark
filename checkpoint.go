package spark

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type checkpointManifest struct {
	Partitions []int `json:"partitions"`
}

func checkpointPartPath(dir string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("part-%d.gob", index))
}

// Checkpoint eagerly materializes every partition of rdd under the context's
// checkpoint directory and truncates its lineage. The returned path remains
// available after Context.Stop; the caller owns cleanup of checkpoint files.
// Call this between actions, not concurrently with actions on the same RDD.
func Checkpoint[T any](rdd *RDD[T]) (string, error) {
	rdd.checkpointRun.Lock()
	defer rdd.checkpointRun.Unlock()
	rdd.checkpointMu.RLock()
	path := rdd.checkpointPath
	rdd.checkpointMu.RUnlock()
	if path != "" {
		return path, nil
	}
	root := rdd.ctx.getCheckpointDir()
	if root == "" {
		return "", fmt.Errorf("checkpoint directory is not set")
	}
	// Materialize shuffle parents before writing any partition.
	computeShuffleStages(rdd)
	dir, err := os.MkdirTemp(root, "rdd-checkpoint-")
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(dir)
		}
	}()
	parts := rdd.Partitions()
	manifest := checkpointManifest{Partitions: make([]int, 0, len(parts))}
	for _, p := range parts {
		idx := p.Index()
		writer, err := newBucketWriter(checkpointPartPath(dir, idx))
		if err != nil {
			return "", err
		}
		iter := rdd.Compute(p)
		for {
			v, ok := iter()
			if !ok {
				break
			}
			encoded, err := DefaultCodec().Encode(v)
			if err != nil {
				_ = writer.suspend()
				return "", err
			}
			if len(encoded) > rdd.ctx.recordBytes() {
				_ = writer.suspend()
				return "", fmt.Errorf("checkpoint record %d exceeds MaxRecordBytes=%d", len(encoded), rdd.ctx.recordBytes())
			}
			if err := writer.append(encoded); err != nil {
				_ = writer.suspend()
				return "", err
			}
		}
		if _, err := writer.finish(); err != nil {
			return "", err
		}
		manifest.Partitions = append(manifest.Partitions, idx)
	}
	b, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0644); err != nil {
		return "", err
	}
	rdd.checkpointMu.Lock()
	rdd.checkpointPath = dir
	rdd.checkpointMu.Unlock()
	complete = true
	return dir, nil
}

// ReadCheckpoint builds a lineage-free RDD from a completed local checkpoint.
// In distributed jobs, construct this RDD in the registered factory on both
// driver and workers using the same shared filesystem path.
func ReadCheckpoint[T any](ctx *Context, dir string) (*RDD[T], error) {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var manifest checkpointManifest
	if err := json.Unmarshal(b, &manifest); err != nil {
		return nil, err
	}
	if manifest.Partitions == nil {
		return nil, fmt.Errorf("checkpoint %q has no partition list", dir)
	}
	for _, idx := range manifest.Partitions {
		if idx < 0 {
			return nil, fmt.Errorf("invalid checkpoint partition %d", idx)
		}
		if _, err := os.Stat(checkpointPartPath(dir, idx)); err != nil {
			return nil, err
		}
	}
	rdd := NewRDD(ctx, func() []Partition {
		parts := make([]Partition, len(manifest.Partitions))
		for i, idx := range manifest.Partitions {
			parts[i] = NewPartition(idx)
		}
		return parts
	}, nil, func(p Partition) Iterator[T] {
		return checkpointIterator[T](ctx, dir, p.Index())
	})
	return rdd, nil
}

func checkpointIterator[T any](ctx *Context, dir string, idx int) Iterator[T] {
	reader, err := openBucketFile(checkpointPartPath(dir, idx), DefaultCodec(), ctx.recordBytes())
	must(err)
	ctx.onClose(func() { _ = reader.Close() })
	return func() (T, bool) {
		v, err := reader.Next()
		if err == io.EOF {
			var zero T
			return zero, false
		}
		must(err)
		value, ok := v.(T)
		if !ok {
			must(fmt.Errorf("checkpoint type mismatch: %T", v))
		}
		return value, true
	}
}
