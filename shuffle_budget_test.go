package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestWorkerBudgetAcrossConcurrentMapAttempts(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	var sizes [2]int64
	for p := range sizes {
		result, err := ExecuteTask(Task{JobID: "size-probe", Job: spec, StageID: plan.Stages[0].ID, PartitionID: p, StoreDir: t.TempDir(), Fingerprint: plan.Fingerprint})
		if err != nil {
			t.Fatal(err)
		}
		for _, meta := range result.Manifest.Buckets {
			sizes[p] += meta.Bytes
		}
	}
	limit := sizes[0] + sizes[1] - 1
	t.Setenv("GOSPARK_SHUFFLE_DISK_BYTES", fmt.Sprint(limit))
	dir := t.TempDir()
	srv, addr, err := ServeWorker(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	client := &WorkerClient{BaseURL: "http://" + addr}
	jobID := "job-0123456789abcdef0123456789abcdef"
	var wg sync.WaitGroup
	results := make([]error, 2)
	for p := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, results[p] = client.Exec(Task{JobID: jobID, Job: spec, StageID: plan.Stages[0].ID, PartitionID: p, Fingerprint: plan.Fingerprint})
		}()
	}
	wg.Wait()
	successes, quotaErrors := 0, 0
	for _, err := range results {
		if err == nil {
			successes++
		} else if strings.Contains(err.Error(), "shuffle disk budget exceeded") {
			quotaErrors++
		} else {
			t.Fatalf("unexpected map error: %v", err)
		}
	}
	if successes != 1 || quotaErrors != 1 {
		t.Fatalf("expected one published map and one quota failure: %v", results)
	}
	if err := client.CleanupJobContext(t.Context(), jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, jobID)); !os.IsNotExist(err) {
		t.Fatalf("cleanup left map output: %v", err)
	}
	// Releasing a completed job's bytes lets a new job publish its output.
	if _, err := client.Exec(Task{JobID: "job-abcdef0123456789abcdef0123456789", Job: spec, StageID: plan.Stages[0].ID, PartitionID: 0, Fingerprint: plan.Fingerprint}); err != nil {
		t.Fatalf("quota not released after cleanup: %v", err)
	}
}

func TestWorkerBudgetCoversFetchScratchAndReleasesFailure(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	var upstream []MapOutputManifest
	source := t.TempDir()
	for p := 0; p < 2; p++ {
		res, err := ExecuteTask(Task{JobID: "fetch-budget", Job: spec, StageID: plan.Stages[0].ID, PartitionID: p, StoreDir: source, Fingerprint: plan.Fingerprint})
		if err != nil {
			t.Fatal(err)
		}
		upstream = append(upstream, *res.Manifest)
	}
	limit := upstream[0].Buckets[0].Bytes - 1
	t.Setenv("GOSPARK_SHUFFLE_DISK_BYTES", fmt.Sprint(limit))
	dir := t.TempDir()
	budget, err := newDiskBudget(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteTask(Task{JobID: "fetch-budget", Job: spec, StageID: plan.ResultStageID, PartitionID: 0, StoreDir: dir, Upstream: upstream, Fingerprint: plan.Fingerprint, budget: budget})
	if err == nil || !strings.Contains(err.Error(), "shuffle disk budget exceeded") {
		t.Fatalf("expected fetch quota failure, got %v", err)
	}
	if budget.used != 0 {
		t.Fatalf("failed fetch retained %d reserved bytes", budget.used)
	}
}

func TestWorkerBudgetCountsExistingFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orphan"), make([]byte, 20), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOSPARK_SHUFFLE_DISK_BYTES", "25")
	b, err := newDiskBudget(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.reserve(filepath.Join(dir, "next"), 6); err == nil {
		t.Fatal("ignored existing disk usage")
	}
	if err := b.removeFile(filepath.Join(dir, "orphan")); err != nil {
		t.Fatal(err)
	}
	if err := b.reserve(filepath.Join(dir, "next"), 6); err != nil {
		t.Fatalf("reclaimed bytes unavailable: %v", err)
	}
}

func TestWorkerBudgetReleasesFailedMapAndSortRuns(t *testing.T) {
	t.Setenv("GOSPARK_SHUFFLE_DISK_BYTES", "40")
	dir := t.TempDir()
	b, err := newDiskBudget(dir)
	if err != nil {
		t.Fatal(err)
	}
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteTask(Task{JobID: "map-budget", Job: spec, StageID: plan.Stages[0].ID, PartitionID: 0, StoreDir: dir, Fingerprint: plan.Fingerprint, budget: b})
	if err == nil || !strings.Contains(err.Error(), "shuffle disk budget exceeded") {
		t.Fatalf("expected map quota failure, got %v", err)
	}
	if b.used != 0 {
		t.Fatalf("failed map retained %d reserved bytes", b.used)
	}

	ctx := NewContext(&Config{AppName: "sort-budget"})
	ctx.disk.clear()
	ctx.disk.dir, err = os.MkdirTemp(dir, "scratch-")
	if err != nil {
		t.Fatal(err)
	}
	ctx.shuffleBudget = b
	ctx.onClose(func() { _ = b.removeDir(ctx.disk.dir) })
	func() {
		defer func() {
			v, ok := recover().(executionError)
			if !ok || !strings.Contains(v.err.Error(), "shuffle disk budget exceeded") {
				t.Errorf("expected sort quota failure, got %v", v)
			}
		}()
		input := &sliceStream{iter: SliceIterator([]any{"an encoded record larger than remaining disk", "another record"})}
		externalSort(ctx, input, func(a, b any) bool { return a.(string) < b.(string) })
	}()
	ctx.Stop()
	if b.used != 0 {
		t.Fatalf("failed sort retained %d reserved bytes", b.used)
	}
}
