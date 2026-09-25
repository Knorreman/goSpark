package spark

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShuffleCleanupAfterSuccessAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			dir := t.TempDir()
			var jobID string
			var mapPath string
			_, err := ScheduleWith(JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2},
				[]TaskRunner{localRunner{storeDir: dir}}, ScheduleOpts{OnTaskComplete: func(task Task, res ExecResult) error {
					jobID = task.JobID
					if res.Manifest != nil {
						mapPath = res.Manifest.Location
						if _, err := os.Stat(mapPath); err != nil {
							t.Fatalf("shuffle removed before consumers finish: %v", err)
						}
					}
					if fail && res.Kind == StageResult {
						return errors.New("injected result failure")
					}
					return nil
				}})
			if fail != (err != nil) || jobID == "" || mapPath == "" {
				t.Fatalf("job=%s path=%s err=%v", jobID, mapPath, err)
			}
			if _, err := os.Stat(filepath.Join(dir, jobID)); !os.IsNotExist(err) {
				t.Fatalf("job files left behind: %v", err)
			}
		})
	}
}

func TestRemoteShuffleCleanupAndValidation(t *testing.T) {
	worker := startTestWorker(t)
	var jobID, mapPath string
	_, err := ScheduleWith(JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2},
		[]TaskRunner{worker}, ScheduleOpts{OnTaskComplete: func(task Task, res ExecResult) error {
			jobID = task.JobID
			if res.Manifest != nil {
				mapPath = res.Manifest.Location
			}
			return nil
		}})
	if err != nil || jobID == "" || mapPath == "" {
		t.Fatalf("job=%s path=%s err=%v", jobID, mapPath, err)
	}
	if _, err := os.Stat(mapPath); !os.IsNotExist(err) {
		t.Fatalf("remote shuffle not cleaned: %v", err)
	}
	if err := worker.CleanupJobContext(context.Background(), jobID); err != nil {
		t.Fatalf("cleanup should be idempotent: %v", err)
	}
	if err := worker.CleanupJobContext(context.Background(), "../other"); err == nil {
		t.Fatal("unsafe job ID accepted")
	}
}

func TestShuffleCleanupAfterCancellation(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var jobID string
	_, err := ScheduleContext(ctx, JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2},
		[]TaskRunner{localRunner{storeDir: dir}}, ScheduleOpts{OnTaskComplete: func(task Task, r ExecResult) error {
			if r.Manifest != nil {
				jobID = task.JobID
				cancel()
			}
			return nil
		}})
	if !errors.Is(err, context.Canceled) || jobID == "" {
		t.Fatalf("job=%q err=%v", jobID, err)
	}
	if _, err := os.Stat(filepath.Join(dir, jobID)); !os.IsNotExist(err) {
		t.Fatalf("canceled job files left behind: %v", err)
	}
}

func TestShuffleMapByteLimit(t *testing.T) {
	t.Setenv("GOSPARK_SHUFFLE_MAP_MAX_BYTES", "32")
	dir := t.TempDir()
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteTask(Task{JobID: "cap-test", Job: spec, StageID: plan.Stages[0].ID, PartitionID: 0, StoreDir: dir, Fingerprint: plan.Fingerprint})
	if err == nil || !strings.Contains(err.Error(), "shuffle map output exceeds") {
		t.Fatalf("expected map byte limit, got %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "cap-test", "s1"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed output left on disk: %v", entries)
	}
	t.Setenv("GOSPARK_SHUFFLE_MAP_MAX_BYTES", "1048576")
	result, err := ExecuteTask(Task{JobID: "cap-test", Job: spec, StageID: plan.Stages[0].ID, PartitionID: 0, StoreDir: dir, Fingerprint: plan.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, bucket := range result.Manifest.Buckets {
		size += bucket.Bytes
	}
	if size > 1048576 {
		t.Fatalf("published %d bytes above cap", size)
	}
}

func TestWorkerRejectsCleanupWhileTaskIsActive(t *testing.T) {
	started := make(chan struct{}, 1)
	jobID := "job-0123456789abcdef0123456789abcdef"
	RegisterJob("cleanup-blocking", func(ctx *Context, _ JobSpec) (RDDAny, error) {
		return NewRDD[int](ctx, func() []Partition { return NewPartitions(1) }, nil, func(Partition) Iterator[int] {
			return func() (int, bool) {
				started <- struct{}{}
				<-ctx.TaskContext().Done()
				return 0, false
			}
		}), nil
	})
	dir := t.TempDir()
	blockDir := NewDiskShuffleStore(dir).mapDir(jobID, 1, 0, 0)
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		t.Fatal(err)
	}
	srv, addr, err := ServeWorker(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	client := &WorkerClient{BaseURL: "http://" + addr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.ExecContext(ctx, Task{JobID: jobID, Job: JobSpec{TaskName: "cleanup-blocking", NumPartitions: 1}, StageID: 0})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("task never started")
	}
	req, err := http.NewRequest(http.MethodPost, client.BaseURL+"/cleanup/"+jobID, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("active job cleanup returned HTTP %d", resp.StatusCode)
	}
	if _, err := os.Stat(blockDir); err != nil {
		t.Fatalf("active job data removed: %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not stop")
	}
	cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := client.CleanupJobContext(cleanupCtx, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blockDir); !os.IsNotExist(err) {
		t.Fatalf("completed job data left behind: %v", err)
	}
}
