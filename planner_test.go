package spark

import (
	"strings"
	"sync/atomic"
	"testing"
)

func TestPlanJobStableAcrossContexts(t *testing.T) {
	RegisterPipeline("plan-wordcount", planWordCountJob)
	spec := JobSpec{
		TaskName:      "plan-wordcount",
		Action:        ActionCollect,
		NumPartitions: 2,
	}
	p1, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	if p1.Fingerprint != p2.Fingerprint {
		t.Fatalf("fingerprints differ: %s vs %s", p1.Fingerprint, p2.Fingerprint)
	}
	if err := VerifyPlan(p1, spec); err != nil {
		t.Fatal(err)
	}
	if len(p1.Stages) != 2 {
		t.Fatalf("expected 2 stages (shuffle_map + result), got %d: %+v", len(p1.Stages), p1.Stages)
	}
	kinds := map[StageKind]int{}
	for _, s := range p1.Stages {
		kinds[s.Kind]++
	}
	if kinds[StageShuffleMap] != 1 || kinds[StageResult] != 1 {
		t.Fatalf("unexpected stage kinds: %+v", p1.Stages)
	}
}

func TestPlanJobRejectsProtocolAndUnknowns(t *testing.T) {
	RegisterPipeline("plan-wordcount", planWordCountJob)
	_, err := PlanJob(JobSpec{TaskName: "plan-wordcount", ProtocolVersion: 99})
	if err == nil {
		t.Fatal("expected protocol mismatch error")
	}
	_, err = PlanJob(JobSpec{TaskName: "does-not-exist", Action: ActionCollect})
	if err == nil {
		t.Fatal("expected missing job error")
	}
	_, err = PlanJob(JobSpec{TaskName: "plan-wordcount", Action: "not-an-action"})
	if err == nil {
		t.Fatal("expected missing action error")
	}
}

func TestPlanJobDivergentConfig(t *testing.T) {
	RegisterPipeline("plan-wordcount", planWordCountJob)
	a, err := PlanJob(JobSpec{TaskName: "plan-wordcount", Action: ActionCollect, NumPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := PlanJob(JobSpec{TaskName: "plan-wordcount", Action: ActionCollect, NumPartitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint == b.Fingerprint {
		t.Fatal("expected different fingerprints for different partition counts")
	}
	c, err := PlanJob(JobSpec{
		TaskName:      "plan-wordcount",
		Action:        ActionCollect,
		NumPartitions: 2,
		Params:        map[string]string{"extra": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint == c.Fingerprint {
		t.Fatal("expected different fingerprints for different params")
	}
}

func TestPlanJoinTwoShuffleInputs(t *testing.T) {
	RegisterPipeline("plan-join", func(ctx *Context, spec JobSpec) (*RDD[Pair[int, Pair[string, int]]], error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 3
		}
		left := Parallelize(ctx, []Pair[int, string]{
			NewPair(1, "a"), NewPair(2, "b"), NewPair(3, "c"),
		}, 2)
		right := Parallelize(ctx, []Pair[int, int]{
			NewPair(1, 10), NewPair(2, 20), NewPair(4, 40),
		}, 2)
		return Join(left, right, NewHashPartitioner(np)), nil
	})
	plan, err := PlanJob(JobSpec{TaskName: "plan-join", Action: ActionCollect, NumPartitions: 3})
	if err != nil {
		t.Fatal(err)
	}
	maps := 0
	results := 0
	for _, s := range plan.Stages {
		switch s.Kind {
		case StageShuffleMap:
			maps++
		case StageResult:
			results++
		}
	}
	if maps != 2 {
		t.Fatalf("expected 2 shuffle_map stages for join, got %d: %+v", maps, plan.Stages)
	}
	if results != 1 {
		t.Fatalf("expected 1 result stage, got %d", results)
	}
	var result Stage
	for _, s := range plan.Stages {
		if s.ID == plan.ResultStageID {
			result = s
		}
	}
	if len(result.Parents) != 2 {
		t.Fatalf("result stage should have 2 parents, got %v", result.Parents)
	}
}

func TestPlanRDDDoesNotCompute(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	var computes atomic.Int64
	rdd := Parallelize(ctx, []int{1, 2, 3, 4}, 2)
	mapped := Map(rdd, func(x int) Pair[int, int] {
		computes.Add(1)
		return NewPair(x, 1)
	})
	reduced := ReduceByKey(mapped, NewHashPartitioner(2), func(a, b int) int { return a + b })
	_, err := PlanRDD(reduced, JobSpec{TaskName: "inline", Action: ActionCollect, NumPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if computes.Load() != 0 {
		t.Fatalf("planning computed %d records", computes.Load())
	}
}

func TestPlanParallelizeNoShuffle(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	rdd := Parallelize(ctx, []int{1, 2, 3}, 2)
	plan, err := PlanRDD(rdd, JobSpec{TaskName: "par", Action: ActionCollect})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Stages) != 1 || plan.Stages[0].Kind != StageResult {
		t.Fatalf("expected single result stage, got %+v", plan.Stages)
	}
	if len(plan.Stages[0].Parents) != 0 {
		t.Fatalf("expected no parents, got %v", plan.Stages[0].Parents)
	}
}

func planWordCountJob(ctx *Context, spec JobSpec) (*RDD[Pair[string, int]], error) {
	np := spec.NumPartitions
	if np <= 0 {
		np = 2
	}
	lines := []string{"hello world", "hello spark"}
	if v := spec.Params["lines"]; v != "" {
		lines = strings.Split(v, "|")
	}
	rdd := Parallelize(ctx, lines, np)
	words := FlatMap(rdd, func(line string) []string { return strings.Fields(line) })
	pairs := Map(words, func(w string) Pair[string, int] { return NewPair(w, 1) })
	return ReduceByKey(pairs, NewHashPartitioner(np), func(a, b int) int { return a + b }), nil
}
