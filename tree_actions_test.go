package spark

import (
	"reflect"
	"testing"
)

func treeTestRDD(ctx *Context) *RDD[int] {
	values := [][]int{{1, 2}, {}, {3, 4}}
	return NewRDD(ctx,
		func() []Partition { return []Partition{NewPartition(0), NewPartition(1), NewPartition(2)} },
		nil,
		func(part Partition) Iterator[int] { return SliceIterator(values[part.Index()]) },
	)
}

func treeSeq(acc map[string]int, n int) map[string]int {
	acc["sum"] += n
	acc["count"]++
	return acc
}

func treeComb(a, b map[string]int) map[string]int {
	for k, v := range b {
		a[k] += v
	}
	return a
}

func init() {
	RegisterJob("tree-test", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		if spec.Params["empty"] == "true" {
			return NewRDD(ctx,
				func() []Partition { return []Partition{NewPartition(0), NewPartition(1)} }, nil,
				func(Partition) Iterator[int] { return EmptyIterator[int]() }), nil
		}
		return treeTestRDD(ctx), nil
	})
	RegisterTreeAggregate("tree-test-aggregate", map[string]int{}, treeSeq, treeComb)
	RegisterTreeReduce("tree-test-reduce", func(a, b int) int { return a + b })
}

func TestTreeActionsLocal(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal})
	defer ctx.Stop()
	rdd := treeTestRDD(ctx)
	zero := map[string]int{}
	for i := 0; i < 2; i++ {
		if got := TreeAggregate(rdd, zero, treeSeq, treeComb); !reflect.DeepEqual(got, map[string]int{"sum": 10, "count": 4}) {
			t.Fatalf("aggregate = %v", got)
		}
	}
	if len(zero) != 0 {
		t.Fatalf("zero mutated: %v", zero)
	}
	if got, ok := TreeReduce(rdd, func(a, b int) int { return a + b }); !ok || got != 10 {
		t.Fatalf("reduce = %d, %v", got, ok)
	}
	empty := NewRDD(ctx, func() []Partition { return []Partition{NewPartition(0)} }, nil,
		func(Partition) Iterator[int] { return EmptyIterator[int]() })
	if got := TreeAggregate(empty, zero, treeSeq, treeComb); len(got) != 0 {
		t.Fatalf("empty aggregate = %v", got)
	}
	if _, ok := TreeReduce(empty, func(a, b int) int { return a + b }); ok {
		t.Fatal("empty reduction returned a value")
	}
}

func TestTreeActionsTwoWorkers(t *testing.T) {
	w1, w2 := startTestWorker(t), startTestWorker(t)
	for _, tc := range []struct {
		action string
		want   any
		sizes  []int
	}{
		{"tree-test-aggregate", map[string]int{"sum": 10, "count": 4}, []int{1, 1, 1}},
		{"tree-test-reduce", 10, []int{1, 0, 1}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			sizes := make([]int, 3)
			results, err := ScheduleWith(JobSpec{TaskName: "tree-test", Action: tc.action, NumPartitions: 3},
				[]TaskRunner{w1, w2}, ScheduleOpts{OnTaskComplete: func(task Task, res ExecResult) error {
					if res.Kind == StageResult {
						sizes[task.PartitionID] = len(res.Records)
					}
					return nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(sizes, tc.sizes) || len(results) != 1 || !reflect.DeepEqual(results[0], tc.want) {
				t.Fatalf("partials %v, result %v; want %v, %v", sizes, results, tc.sizes, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		action string
		want   []any
	}{
		{"tree-test-aggregate", []any{map[string]int{}}},
		{"tree-test-reduce", nil},
	} {
		got, err := Schedule(JobSpec{TaskName: "tree-test", Action: tc.action, Params: map[string]string{"empty": "true"}}, []TaskRunner{w1, w2})
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("empty %s: got %v, err %v; want %v", tc.action, got, err, tc.want)
		}
	}
}
