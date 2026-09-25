package spark

import (
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestCheckpointTruncatesLineage(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	if _, err := Checkpoint(Parallelize(ctx, []int{1})); err == nil {
		t.Fatal("expected missing checkpoint directory error")
	}
	if err := ctx.SetCheckpointDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	parent := Map(Parallelize(ctx, []int{1, 2, 3, 4}, 2), func(n int) int {
		calls.Add(1)
		return n * 2
	})
	child := Map(parent, func(n int) int { return n + 1 })
	path, err := Checkpoint(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(path) || len(parent.Dependencies()) != 0 {
		t.Fatalf("checkpoint did not truncate lineage: %q, %v", path, parent.Dependencies())
	}
	if calls.Load() != 4 {
		t.Fatalf("materialization called parent %d times", calls.Load())
	}
	if got := Collect(child); !reflect.DeepEqual(got, []int{3, 5, 7, 9}) {
		t.Fatalf("checkpointed child = %v", got)
	}
	if got := Count(parent); got != 4 || calls.Load() != 4 {
		t.Fatalf("checkpoint recomputed parent: count=%d calls=%d", got, calls.Load())
	}
	if again, err := Checkpoint(parent); err != nil || again != path {
		t.Fatalf("repeat checkpoint = %q, %v", again, err)
	}
	ctx.Stop()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("checkpoint removed on Stop: %v", err)
	}
	other := newTestContext()
	defer other.Stop()
	reloaded, err := ReadCheckpoint[int](other, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := Collect(reloaded); !reflect.DeepEqual(got, []int{2, 4, 6, 8}) {
		t.Fatalf("reloaded checkpoint = %v", got)
	}
}

func TestCheckpointEmptyAndMissingPartition(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	if err := ctx.SetCheckpointDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	path, err := Checkpoint(EmptyRDD[int](ctx))
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := ReadCheckpoint[int](ctx, path)
	if err != nil || Count(reloaded) != 0 {
		t.Fatalf("empty checkpoint: %v", err)
	}
	path, err = Checkpoint(Parallelize(ctx, []int{1}, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(checkpointPartPath(path, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCheckpoint[int](ctx, path); err == nil {
		t.Fatal("expected missing partition error")
	}
}

func init() {
	RegisterJob("checkpoint-read", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		return ReadCheckpoint[int](ctx, spec.Params["checkpoint"])
	})
}

func TestCheckpointAcrossWorkerProcesses(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	if err := ctx.SetCheckpointDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	path, err := Checkpoint(Parallelize(ctx, []int{1, 2, 3, 4}, 2))
	if err != nil {
		t.Fatal(err)
	}
	spec := JobSpec{TaskName: "checkpoint-read", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"checkpoint": path}}
	w1 := startTestWorker(t)
	w2 := startTestWorker(t)
	records, err := Schedule(spec, []TaskRunner{w1, w2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, []any{1, 2, 3, 4}) {
		t.Fatalf("distributed checkpoint = %v", records)
	}
}
