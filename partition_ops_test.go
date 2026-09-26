package spark

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestLookup(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 3})
	defer ctx.Stop()
	data := [][]Pair[string, int]{
		{NewPair("a", 1), NewPair("b", 2)},
		{NewPair("a", 3)},
		{NewPair("b", 4)},
	}
	var mu sync.Mutex
	var visited []int
	partitioner := NewHashPartitioner(3)
	makeRDD := func(partitioned bool) *RDD[Pair[string, int]] {
		localData := data
		var opts []RDDOpt[Pair[string, int]]
		if partitioned {
			localData = make([][]Pair[string, int], 3)
			for _, p := range []Pair[string, int]{NewPair("a", 1), NewPair("b", 2), NewPair("a", 3), NewPair("b", 4)} {
				idx := partitioner.GetPartition(p.Key)
				localData[idx] = append(localData[idx], p)
			}
			opts = append(opts, WithPartitioner[Pair[string, int]](partitioner))
		}
		return NewRDD(ctx, func() []Partition { return NewPartitions(3) }, nil,
			func(p Partition) Iterator[Pair[string, int]] {
				mu.Lock()
				visited = append(visited, p.Index())
				mu.Unlock()
				return SliceIterator(localData[p.Index()])
			}, opts...)
	}
	plain := makeRDD(false)
	if got := Lookup(plain, "a"); !reflect.DeepEqual(got, []int{1, 3}) {
		t.Fatalf("scan lookup: %v", got)
	}
	if !reflect.DeepEqual(visited, []int{0, 1, 2}) {
		t.Fatalf("scan visited %v", visited)
	}
	visited = nil
	partitioned := makeRDD(true)
	if got := Lookup(partitioned, "a"); !reflect.DeepEqual(got, []int{1, 3}) {
		t.Fatalf("partitioned lookup: %v", got)
	}
	if !reflect.DeepEqual(visited, []int{partitioner.GetPartition("a")}) {
		t.Fatalf("partitioned lookup visited %v", visited)
	}
	if got := Lookup(partitioned, "missing"); len(got) != 0 {
		t.Fatalf("missing key: %v", got)
	}
	shuffled := GroupByKey(plain, NewHashPartitioner(3))
	if got := Lookup(shuffled, "a"); !reflect.DeepEqual(got, [][]int{{1, 3}}) {
		t.Fatalf("shuffled lookup: %v", got)
	}
}

func TestForEachPartition(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal})
	defer ctx.Stop()
	rdd := NewRDD(ctx, func() []Partition { return NewPartitions(3) }, nil,
		func(p Partition) Iterator[int] { return SliceIterator([][]int{{1, 2}, {}, {3}}[p.Index()]) })
	var mu sync.Mutex
	var sizes []int
	ForEachPartition(rdd, func(it Iterator[int]) {
		n := len(CollectIterator(it))
		mu.Lock()
		sizes = append(sizes, n)
		mu.Unlock()
	})
	if len(sizes) != 3 {
		t.Fatalf("sizes: %v", sizes)
	}
	// Each partition, including the empty one, calls the callback exactly once.
	want := map[int]int{0: 1, 1: 1, 2: 1}
	for _, n := range sizes {
		want[n]--
	}
	if !reflect.DeepEqual(want, map[int]int{0: 0, 1: 0, 2: 0}) {
		t.Fatalf("sizes: %v", sizes)
	}
}

func TestZipWithIndex(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal})
	defer ctx.Stop()
	rdd := NewRDD(ctx, func() []Partition { return NewPartitions(4) }, nil,
		func(p Partition) Iterator[string] {
			return SliceIterator([][]string{{"a", "b"}, {}, {"c"}, {"d", "e"}}[p.Index()])
		})
	indexed := ZipWithIndex(rdd)
	want := []Pair[string, int64]{NewPair("a", int64(0)), NewPair("b", int64(1)), NewPair("c", int64(2)), NewPair("d", int64(3)), NewPair("e", int64(4))}
	for i := 0; i < 2; i++ {
		if got := Collect(indexed); !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: got %v want %v", i, got, want)
		}
	}
	if got := Collect(ZipWithIndex(EmptyRDD[string](ctx))); len(got) != 0 {
		t.Fatalf("empty indexed: %v", got)
	}
}

func TestZipPartitions(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal})
	defer ctx.Stop()
	inputs := []*RDD[int]{
		Parallelize(ctx, []int{1, 2, 3, 4}, 2),
		Parallelize(ctx, []int{10, 20, 30, 40}, 2),
		Parallelize(ctx, []int{100, 200, 300, 400}, 2),
	}
	zipped := ZipPartitions(inputs, func(iters []Iterator[int]) Iterator[int] {
		return func() (int, bool) {
			a, ok := iters[0]()
			if !ok {
				return 0, false
			}
			b, _ := iters[1]()
			c, _ := iters[2]()
			return a + b + c, true
		}
	})
	if got := Collect(zipped); !reflect.DeepEqual(got, []int{111, 222, 333, 444}) {
		t.Fatalf("zip: %v", got)
	}
	if got := Collect(ZipPartitions([]*RDD[int]{inputs[0], Filter(inputs[1], func(n int) bool { return n != 20 })},
		func(iters []Iterator[int]) Iterator[int] {
			return ChainIterators(iters)
		})); !reflect.DeepEqual(got, []int{1, 2, 10, 3, 4, 30, 40}) {
		t.Fatalf("uneven partition records: %v", got)
	}
	defer func() {
		if msg := fmt.Sprint(recover()); !strings.Contains(msg, "RDD 1 has 1 partitions, want 2") {
			t.Fatalf("mismatch panic: %s", msg)
		}
	}()
	ZipPartitions([]*RDD[int]{inputs[0], Parallelize(ctx, []int{1}, 1)}, func(iters []Iterator[int]) Iterator[int] { return iters[0] })
}

func init() {
	RegisterPipeline("zip-index-check", func(ctx *Context, spec JobSpec) (*RDD[Pair[int, int64]], error) {
		input := Parallelize(ctx, []int{10, 11, 12, 13, 14, 15, 16}, 3)
		return ZipWithIndex(Filter(input, func(n int) bool { return n != 12 && n != 13 })), nil
	})
}

func TestZipWithIndexTwoWorkers(t *testing.T) {
	w1, w2 := startTestWorker(t), startTestWorker(t)
	records, err := RunPipelineAny(JobSpec{TaskName: "zip-index-check", Action: ActionCollect, NumPartitions: 3}, []TaskRunner{w1, w2})
	if err != nil {
		t.Fatal(err)
	}
	want := []Pair[int, int64]{NewPair(10, int64(0)), NewPair(11, int64(1)), NewPair(14, int64(2)), NewPair(15, int64(3)), NewPair(16, int64(4))}
	if len(records) != len(want) {
		t.Fatalf("got %v want %v", records, want)
	}
	for i, rec := range records {
		if rec != want[i] {
			t.Fatalf("record %d: got %v want %v", i, rec, want[i])
		}
	}
}
