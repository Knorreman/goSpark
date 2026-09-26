package spark

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func init() {
	RegisterPipeline("multi-stage-check", func(ctx *Context, spec JobSpec) (*RDD[Pair[string, int]], error) {
		p := Parallelize(ctx, []Pair[string, int]{NewPair("a", 1), NewPair("b", 2), NewPair("a", 3)}, 2)
		r := ReduceByKey(p, NewHashPartitioner(3), func(a, b int) int { return a + b })
		r = Repartition(r, 4)
		return SortByKey(r, func(a, b string) bool { return a < b }, true, 2), nil
	})
	RegisterPipeline("cogroup-check", func(ctx *Context, spec JobSpec) (*RDD[string], error) {
		a := Parallelize(ctx, []Pair[int, int]{NewPair(1, 2), NewPair(1, 3)}, 2)
		b := Parallelize(ctx, []Pair[int, int]{NewPair(1, 4), NewPair(2, 5)}, 2)
		return Map(Cogroup(a, b, NewHashPartitioner(3)), func(p Pair[int, Pair[[]int, []int]]) string {
			sort.Ints(p.Value.Key)
			sort.Ints(p.Value.Value)
			return fmt.Sprint(p)
		}), nil
	})
}

func TestDistributedExactResults(t *testing.T) {
	w1, w2 := startTestWorker(t), startTestWorker(t)
	for _, tc := range []struct {
		name string
		want []string
	}{
		{"multi-stage-check", []string{"{a 4}", "{b 2}"}},
		{"cogroup-check", []string{"{1 {[2 3] [4]}}", "{2 {[] [5]}}"}},
		{"sched-join", []string{"{1 {a 10}}", "{1 {a 11}}", "{2 {b 20}}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := RunPipelineAny(JobSpec{TaskName: tc.name, Action: ActionCollect, NumPartitions: 2}, []TaskRunner{w1, w2})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, r := range recs {
				got = append(got, fmt.Sprint(r))
			}
			if tc.name != "multi-stage-check" {
				sort.Strings(got)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestTaskRejectsIncompleteShuffleAndGraphMismatch(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	task := Task{JobID: "check", Job: spec, StageID: plan.Stages[0].ID, StoreDir: t.TempDir(), Fingerprint: plan.Fingerprint}
	result, err := ExecuteTask(task)
	if err != nil {
		t.Fatal(err)
	}
	task.StageID = plan.ResultStageID
	task.Upstream = []MapOutputManifest{*result.Manifest}
	if _, err := ExecuteTask(task); err == nil {
		t.Fatal("accepted partial shuffle")
	}
	task.StageID = plan.Stages[0].ID
	task.Fingerprint = "different-driver-graph"
	if _, err := ExecuteTask(task); err == nil {
		t.Fatal("accepted mismatched graph")
	}
	task.Fingerprint = plan.Fingerprint
	task.PartitionID = -1
	if _, err := ExecuteTask(task); err == nil {
		t.Fatal("accepted invalid partition")
	}
}
