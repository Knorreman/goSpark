package spark

import (
	"reflect"
	"strings"
	"testing"
)

func init() {
	RegisterPipeline("broadcast-lookup", func(ctx *Context, spec JobSpec) (*RDD[Pair[string, int]], error) {
		table, err := ReadBroadcast[map[string]int](spec, "rates")
		if err != nil {
			return nil, err
		}
		lines := Parallelize(ctx, []string{"a", "b", "missing"}, spec.NumPartitions)
		return Map(lines, func(word string) Pair[string, int] {
			return NewPair(word, table[word])
		}), nil
	})
}

func TestBroadcastLookupAcrossWorkers(t *testing.T) {
	spec := JobSpec{TaskName: "broadcast-lookup", Action: ActionCollect, NumPartitions: 2}
	if err := AddBroadcast(&spec, "rates", map[string]int{"a": 3, "b": 5}); err != nil {
		t.Fatal(err)
	}
	recs, err := RunPipelineAny(spec, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range recs {
		p := rec.(Pair[string, int])
		got[p.Key] = p.Value
	}
	if !reflect.DeepEqual(got, map[string]int{"a": 3, "b": 5, "missing": 0}) {
		t.Fatalf("broadcast lookup = %v", got)
	}
}

func TestBroadcastRejectsDuplicatesAndOversize(t *testing.T) {
	spec := JobSpec{TaskName: "broadcast-lookup"}
	if err := AddBroadcast(&spec, "rates", map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := AddBroadcast(&spec, "rates", map[string]int{"b": 2}); err == nil {
		t.Fatal("duplicate broadcast accepted")
	}
	if err := AddBroadcast(&spec, "", 1); err == nil {
		t.Fatal("empty broadcast name accepted")
	}
	if err := AddBroadcast(&spec, "huge", strings.Repeat("x", maxBroadcastBytes+1)); err == nil {
		t.Fatal("oversized broadcast accepted")
	}
	if _, err := ReadBroadcast[map[string]int](spec, "missing"); err == nil {
		t.Fatal("missing broadcast decoded")
	}
	got, err := ReadBroadcast[map[string]int](spec, "rates")
	if err != nil || got["a"] != 1 {
		t.Fatalf("read = %v err=%v", got, err)
	}
}
