package spark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func init() {
	RegisterJob("save-wordcount", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		data := []string{"hello world", "hello spark", "world spark hello"}
		words := FlatMap(Parallelize(ctx, data, 2), func(s string) []string { return strings.Fields(s) })
		pairs := Map(words, func(w string) Pair[string, int] { return NewPair(w, 1) })
		return ReduceByKey(pairs, NewHashPartitioner(2), func(a, b int) int { return a + b }), nil
	})
}

type retryAfterOutput struct {
	TaskRunner
	failed bool
}

func (r *retryAfterOutput) Exec(task Task) (ExecResult, error) {
	res, err := r.TaskRunner.Exec(task)
	if err == nil && res.Output != nil && !r.failed {
		r.failed = true
		return ExecResult{}, fmt.Errorf("lost response after writing attempt")
	}
	return res, err
}

func checkSavedCounts(t *testing.T, uri string, manifest OutputManifest) {
	t.Helper()
	resolved := ResolvePath(uri)
	got := map[string]int{}
	for _, p := range manifest.Partitions {
		r, err := resolved.FS.Open(p.Key)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var key string
			var n int
			if _, err := fmt.Sscanf(line, "{%s %d}", &key, &n); err != nil {
				t.Fatal(err)
			}
			got[strings.TrimSpace(key)] += n
		}
	}
	if !reflect.DeepEqual(got, map[string]int{"hello": 3, "world": 2, "spark": 2}) {
		t.Fatalf("counts %v", got)
	}
}

func TestScheduleSaveRetrySelectsOnlyAcceptedAttempt(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "output")
	r := &retryAfterOutput{TaskRunner: localRunner{storeDir: t.TempDir()}}
	spec := JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": out}}
	m, err := ScheduleSave(spec, []TaskRunner{r})
	if err != nil {
		t.Fatal(err)
	}
	if !r.failed || len(m.Partitions) != 2 {
		t.Fatal("retry not exercised", m)
	}
	if m.Partitions[0].Attempt != 1 || m.Partitions[1].Attempt != 0 {
		t.Fatalf("selected wrong attempts: %+v", m.Partitions)
	}
	stored, err := ReadCommittedOutput(out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, m) {
		t.Fatalf("manifest mismatch: %+v %+v", stored, m)
	}
	checkSavedCounts(t, out, m)
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "part-") {
			t.Fatalf("uncommitted root output: %s", e.Name())
		}
	}
	if _, err := ScheduleSave(spec, []TaskRunner{r}); !errors.Is(err, ErrOutputCommitted) {
		t.Fatalf("second submission: %v", err)
	}
}

func TestScheduleSaveAcrossWorkerProcesses(t *testing.T) {
	out := filepath.Join(t.TempDir(), "output")
	m, err := ScheduleSave(JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": out}}, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	checkSavedCounts(t, out, m)
}

type rejectPartition struct{ TaskRunner }

func (r rejectPartition) Exec(t Task) (ExecResult, error) {
	if t.Job.Action == ActionSave && t.PartitionID == 1 {
		return ExecResult{}, fmt.Errorf("write denied")
	}
	return r.TaskRunner.Exec(t)
}

func TestScheduleSaveFailureDoesNotPublish(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "failed")
	spec := JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": dir}}
	_, err := ScheduleSaveContext(t.Context(), spec, []TaskRunner{rejectPartition{localRunner{storeDir: t.TempDir()}}}, ScheduleOpts{MaxAttempts: 2})
	if err == nil {
		t.Fatal("expected partition write failure")
	}
	if _, err := ReadCommittedOutput(dir); err == nil {
		t.Fatal("published partial output")
	}
}

func TestScheduleSaveCanceledBeforeCommit(t *testing.T) {
	uri := filepath.Join(t.TempDir(), "canceled")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := ScheduleSaveContext(ctx, JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": uri}},
		[]TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{OnTaskComplete: func(task Task, r ExecResult) error {
			if r.Output != nil {
				cancel()
			}
			return nil
		}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := ReadCommittedOutput(uri); err == nil {
		t.Fatal("published canceled job")
	}
}

type staleSaveRunner struct{ TaskRunner }

func (r staleSaveRunner) Exec(task Task) (ExecResult, error) {
	res, err := r.TaskRunner.Exec(task)
	if err == nil && res.Output != nil {
		res.Output.Attempt--
	}
	return res, err
}
func TestStaleSaveAttemptNotCommitted(t *testing.T) {
	uri := filepath.Join(t.TempDir(), "stale")
	_, err := ScheduleSaveContext(t.Context(), JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": uri}},
		[]TaskRunner{staleSaveRunner{localRunner{storeDir: t.TempDir()}}}, ScheduleOpts{MaxAttempts: 1})
	if err == nil || !strings.Contains(err.Error(), "stale or mismatched save attempt") {
		t.Fatalf("stale attempt accepted: %v", err)
	}
	if _, err := ReadCommittedOutput(uri); err == nil {
		t.Fatal("published stale attempt")
	}
}

func fixtureOutput(t *testing.T, fs FileSystem, base, job string, stage, p, attempt int, body string) PartitionOutput {
	t.Helper()
	key := attemptOutputKey(fs, base, job, stage, p, attempt)
	if err := fs.MkdirAll(fs.Join(base, tempDirName, job, fmt.Sprintf("stage-%d", stage)), 0755); err != nil {
		t.Fatal(err)
	}
	w, err := fs.Create(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return PartitionOutput{JobID: job, StageID: stage, PartitionID: p, Attempt: attempt, Key: key, Bytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
}

func TestConditionalCommitRejectsConcurrentJobs(t *testing.T) {
	for _, scheme := range []string{"local", "memory"} {
		t.Run(scheme, func(t *testing.T) {
			uri := filepath.Join(t.TempDir(), "out")
			if scheme == "memory" {
				bucket := "commit-concurrent"
				RegisterMemBucket(bucket)
				defer UnregisterMemBucket(bucket)
				uri = "s3://" + bucket + "/out"
			}
			resolved := ResolvePath(uri)
			first := fixtureOutput(t, resolved.FS, resolved.BasePath, "job-1", 1, 0, 0, "a\n")
			second := fixtureOutput(t, resolved.FS, resolved.BasePath, "job-2", 1, 0, 0, "b\n")
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i, out := range []PartitionOutput{first, second} {
				wg.Add(1)
				go func(i int, o PartitionOutput) {
					defer wg.Done()
					_, errs[i] = CommitDistributedOutput(uri, o.JobID, "task", 1, 1, []PartitionOutput{o})
				}(i, out)
			}
			wg.Wait()
			winner, loser := 0, 0
			for _, err := range errs {
				if err == nil {
					winner++
				} else if errors.Is(err, ErrOutputCommitted) {
					loser++
				} else {
					t.Fatal(err)
				}
			}
			if winner != 1 || loser != 1 {
				t.Fatalf("concurrent commit: %v", errs)
			}
			m, err := ReadCommittedOutput(uri)
			if err != nil {
				t.Fatal(err)
			}
			if m.JobID != "job-1" && m.JobID != "job-2" {
				t.Fatal(m)
			}
			_, err = CommitDistributedOutput(uri, m.JobID, "task", 1, 1, []PartitionOutput{map[string]PartitionOutput{"job-1": first, "job-2": second}[m.JobID]})
			if err != nil {
				t.Fatalf("idempotent commit: %v", err)
			}
		})
	}
}

func TestCommitRejectsCorruptionAndDuplicates(t *testing.T) {
	uri := filepath.Join(t.TempDir(), "out")
	fs := LocalFS{}
	a := fixtureOutput(t, fs, uri, "job", 1, 0, 0, "a\n")
	if _, err := CommitDistributedOutput(uri, "job", "task", 1, 2, []PartitionOutput{a, a}); err == nil {
		t.Fatal("duplicate partition accepted")
	}
	if err := os.WriteFile(a.Key, []byte("wrong"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := CommitDistributedOutput(uri, "job", "task", 1, 1, []PartitionOutput{a}); err == nil {
		t.Fatal("corrupt attempt accepted")
	}
	if _, err := ReadCommittedOutput(uri); err == nil {
		t.Fatal("corrupt output committed")
	}
}

func TestExclusiveMarkerVisibleOnlyAfterClose(t *testing.T) {
	fs := LocalFS{}
	marker := filepath.Join(t.TempDir(), SuccessFileName)
	w, err := fs.CreateExclusive(marker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("valid")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker visible before close: %v", err)
	}
	if err := w.(interface{ Abort() error }).Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("abort published marker")
	}
	w, err = fs.CreateExclusive(marker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("valid")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = fs.CreateExclusive(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second commit allowed: %v", err)
	}
}
