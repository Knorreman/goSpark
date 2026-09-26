package spark

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

var outerLeft = []Pair[int, string]{
	NewPair(1, "a"), NewPair(2, "left"), NewPair(1, "b"), NewPair(2, "left"), NewPair(4, ""),
}
var outerRight = []Pair[int, int]{
	NewPair(1, 10), NewPair(3, 30), NewPair(1, 11), NewPair(3, 31), NewPair(4, 0),
}

func init() {
	RegisterPipeline("sched-full-outer", func(ctx *Context, spec JobSpec) (*RDD[Pair[int, Pair[*string, *int]]], error) {
		left := Parallelize(ctx, outerLeft, 3)
		right := Parallelize(ctx, outerRight, 2)
		return FullOuterJoin(left, right, NewHashPartitioner(3)), nil
	})
	RegisterPipeline("sched-subtract-key", func(ctx *Context, spec JobSpec) (*RDD[Pair[int, string]], error) {
		left := Parallelize(ctx, outerLeft, 3)
		right := Parallelize(ctx, outerRight, 2)
		return SubtractByKey(left, right, NewHashPartitioner(3)), nil
	})
}

func outerRows(records []Pair[int, Pair[*string, *int]]) []string {
	rows := make([]string, 0, len(records))
	for _, p := range records {
		left, right := "nil", "nil"
		if p.Value.Key != nil {
			left = fmt.Sprintf("%q", *p.Value.Key)
		}
		if p.Value.Value != nil {
			right = fmt.Sprint(*p.Value.Value)
		}
		rows = append(rows, fmt.Sprintf("%d:%s:%s", p.Key, left, right))
	}
	sort.Strings(rows)
	return rows
}

func subtractRows(records []Pair[int, string]) []string {
	rows := make([]string, 0, len(records))
	for _, p := range records {
		rows = append(rows, fmt.Sprintf("%d:%q", p.Key, p.Value))
	}
	sort.Strings(rows)
	return rows
}

func TestFullOuterJoinAndSubtractByKey(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	partitioner := NewHashPartitioner(3)
	left := Parallelize(ctx, outerLeft, 3)
	right := Parallelize(ctx, outerRight, 2)
	wantOuter := []string{
		`1:"a":10`, `1:"a":11`, `1:"b":10`, `1:"b":11`,
		`2:"left":nil`, `2:"left":nil`, `3:nil:30`, `3:nil:31`, `4:"":0`,
	}
	sort.Strings(wantOuter)
	if got := outerRows(Collect(FullOuterJoin(left, right, partitioner))); !reflect.DeepEqual(got, wantOuter) {
		t.Fatalf("full outer join: got %v want %v", got, wantOuter)
	}
	wantSubtract := []string{`2:"left"`, `2:"left"`}
	if got := subtractRows(Collect(SubtractByKey(left, right, partitioner))); !reflect.DeepEqual(got, wantSubtract) {
		t.Fatalf("subtract by key: got %v want %v", got, wantSubtract)
	}

	emptyLeft := Parallelize(ctx, []Pair[int, string]{}, 2)
	emptyRight := Parallelize(ctx, []Pair[int, int]{}, 2)
	if got := outerRows(Collect(FullOuterJoin(emptyLeft, right, partitioner))); len(got) != len(outerRight) {
		t.Fatalf("empty left: got %v", got)
	}
	if got := outerRows(Collect(FullOuterJoin(left, emptyRight, partitioner))); len(got) != len(outerLeft) {
		t.Fatalf("empty right: got %v", got)
	}
	if got := Collect(SubtractByKey(emptyLeft, right, partitioner)); len(got) != 0 {
		t.Fatalf("empty left subtraction: got %v", got)
	}
	if got := subtractRows(Collect(SubtractByKey(left, emptyRight, partitioner))); len(got) != len(outerLeft) {
		t.Fatalf("empty right subtraction: got %v", got)
	}
}

func TestScheduleFullOuterJoinAndSubtractByKey(t *testing.T) {
	w1, w2 := startTestWorker(t), startTestWorker(t)
	for _, tc := range []struct {
		name string
		want []string
	}{
		{"sched-full-outer", []string{
			`1:"a":10`, `1:"a":11`, `1:"b":10`, `1:"b":11`,
			`2:"left":nil`, `2:"left":nil`, `3:nil:30`, `3:nil:31`, `4:"":0`,
		}},
		{"sched-subtract-key", []string{`2:"left"`, `2:"left"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := RunPipelineAny(JobSpec{TaskName: tc.name, Action: ActionCollect, NumPartitions: 3}, []TaskRunner{w1, w2})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			if tc.name == "sched-full-outer" {
				var rows []Pair[int, Pair[*string, *int]]
				for _, rec := range recs {
					rows = append(rows, rec.(Pair[int, Pair[*string, *int]]))
				}
				got = outerRows(rows)
			} else {
				var rows []Pair[int, string]
				for _, rec := range recs {
					rows = append(rows, rec.(Pair[int, string]))
				}
				got = subtractRows(rows)
			}
			sort.Strings(tc.want)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
