package spark

import (
	"reflect"
	"strings"
	"testing"
)

func TestRandomSplitCompleteAndDeterministic(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	data := make([]int, 300)
	for i := range data {
		data[i] = i
	}
	parent := Parallelize(ctx, data, 7)
	weights := []float64{1, 2, 0, 3}
	first := RandomSplit(parent, weights, 42)
	second := RandomSplit(parent, weights, 42)
	seen := make(map[int]int)
	for i := range first {
		got := Collect(first[i])
		if !reflect.DeepEqual(got, Collect(first[i])) || !reflect.DeepEqual(got, Collect(second[i])) {
			t.Fatalf("split %d changed across runs", i)
		}
		if i == 2 && len(got) != 0 {
			t.Fatalf("zero-weight split has records: %v", got)
		}
		for _, record := range got {
			seen[record]++
		}
	}
	for _, record := range data {
		if seen[record] != 1 {
			t.Errorf("record %d appeared %d times", record, seen[record])
		}
	}
	duplicates := RandomSplit(Parallelize(ctx, []int{1, 1, 1, 2, 2}, 2), []float64{1, 1}, 42)
	counts := map[int]int{}
	for _, split := range duplicates {
		for _, record := range Collect(split) {
			counts[record]++
		}
	}
	if counts[1] != 3 || counts[2] != 2 {
		t.Fatalf("duplicate occurrence counts changed: %v", counts)
	}
}

func TestPipePerPartition(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	input := Parallelize(ctx, []string{"a", "b", "c", "d"}, 2)
	if got := Collect(Pipe(input, "cat")); !reflect.DeepEqual(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("cat returned %v", got)
	}
	if got := Collect(Pipe(input, "wc -l")); !reflect.DeepEqual(got, []string{"2", "2"}) {
		t.Fatalf("wc -l returned %v", got)
	}
}

func TestPipeNonzeroExitFailsTask(t *testing.T) {
	RegisterPipeline("pipe-failure-test", func(ctx *Context, _ JobSpec) (*RDD[string], error) {
		return Pipe(Parallelize(ctx, []string{"line"}, 1), "cat >/dev/null; echo failure >&2; exit 7"), nil
	})
	spec := JobSpec{TaskName: "pipe-failure-test", Action: ActionCollect, NumPartitions: 1}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExecuteTask(Task{Job: spec, StageID: plan.ResultStageID, PartitionID: 0, StoreDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "exit status 7") {
		t.Fatalf("expected process failure to fail task, got %v", err)
	}
}
