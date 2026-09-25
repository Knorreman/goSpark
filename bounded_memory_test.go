package spark

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExternalSortTinyBudget(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, ShuffleMemoryBytes: 512, MaxRecordBytes: 256})
	defer ctx.Stop()
	n := 1000
	i := n
	stream, count := externalSort(ctx, &sliceStream{iter: func() (any, bool) {
		if i == 0 {
			return nil, false
		}
		i--
		return i, true
	}}, func(a, b any) bool { return a.(int) < b.(int) })
	if count != n {
		t.Fatal(count)
	}
	for expected := 0; expected < n; expected++ {
		v, err := stream.Next()
		if err != nil || v.(int) != expected {
			t.Fatalf("%v %v expected %d", v, err, expected)
		}
	}
	if _, err := stream.Next(); err != io.EOF {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(ctx.disk.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("leaked merge runs: %v", entries)
	}
}

func TestStreamingShuffleManyBuckets(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, ShuffleMemoryBytes: 512})
	defer ctx.Stop()
	rdd := Parallelize(ctx, []Pair[int, int]{NewPair(0, 1), NewPair(18, 2), NewPair(0, 3)}, 1)
	sid := ctx.nextShuffleID()
	dep := NewShuffleDep(rdd, NewPartitionIdPassthrough(20), sid, false, nil, func(v any) any { return v.(Pair[int, int]).Key })
	m, err := writeStreamMap(ctx, NewDiskShuffleStore(t.TempDir()), dep, rdd.Partitions()[0], "job", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Buckets) != 20 {
		t.Fatal(m)
	}
	for _, b := range m.Buckets {
		r, err := openBucketFile(filepath.Join(m.Location, bucketName(b.ReduceID)), DefaultCodec(), ctx.recordBytes())
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for {
			_, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
		r.Close()
		if count != b.Records {
			t.Fatal(count, b)
		}
	}
}

func TestOversizedShuffleFrameRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad")
	data := make([]byte, 16)
	copy(data, shuffleMagic[:])
	binary.BigEndian.PutUint32(data[4:8], 1)
	binary.BigEndian.PutUint32(data[8:12], ^uint32(0))
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := openBucketFile(p, DefaultCodec(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("bad frame accepted: %v", err)
	}
}

func TestExternalSortPartitionOrderAndDuplicates(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 3, ShuffleMemoryBytes: 256})
	defer ctx.Stop()
	r := Parallelize(ctx, []Pair[int, string]{NewPair(3, "c"), NewPair(1, "a"), NewPair(2, "b"), NewPair(1, "a2")}, 3)
	for _, ascending := range []bool{true, false} {
		got := Collect(SortByKey(r, func(a, b int) bool { return a < b }, ascending, 3))
		want := []Pair[int, string]{NewPair(1, "a"), NewPair(1, "a2"), NewPair(2, "b"), NewPair(3, "c")}
		if !ascending {
			want = []Pair[int, string]{NewPair(3, "c"), NewPair(2, "b"), NewPair(1, "a"), NewPair(1, "a2")}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestJoinProducesPairsLazily(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, ShuffleMemoryBytes: 1024, MaxGroupBytes: 1 << 20})
	defer ctx.Stop()
	a := make([]Pair[int, int], 1000)
	for i := range a {
		a[i] = NewPair(1, i)
	}
	j := Join(Parallelize(ctx, a, 1), Parallelize(ctx, a, 1), NewHashPartitioner(1))
	if got := Count(j); got != 1000000 {
		t.Fatal(got)
	}
}

func TestGroupLimitIsTaskError(t *testing.T) {
	RegisterJob("bounded-group-limit", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		ctx.Config().MaxGroupBytes = 256
		p := make([]Pair[string, int], 100)
		for i := range p {
			p[i] = NewPair("hot", i)
		}
		return GroupByKey(Parallelize(ctx, p, 1), NewHashPartitioner(1)), nil
	})
	_, err := Schedule(JobSpec{TaskName: "bounded-group-limit", Action: ActionCollect, NumPartitions: 1}, []TaskRunner{localRunner{storeDir: t.TempDir()}})
	if err == nil || !strings.Contains(err.Error(), "MaxGroupBytes") {
		t.Fatalf("got %v", err)
	}
}

func TestTextFileRejectsOversizedLine(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, MaxRecordBytes: 64})
	defer ctx.Stop()
	p := filepath.Join(t.TempDir(), "large-line")
	if err := os.WriteFile(p, []byte(strings.Repeat("x", 10000)), 0600); err != nil {
		t.Fatal(err)
	}
	r := TextFile(ctx, p, 1)
	defer func() {
		v := recover()
		e, ok := v.(executionError)
		if !ok || !strings.Contains(e.err.Error(), "MaxRecordBytes") {
			t.Fatalf("expected record limit, got %v", v)
		}
	}()
	CollectIterator(r.Compute(r.Partitions()[0]))
}

func TestSkewedReduceAndGroupLimit(t *testing.T) {
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, ShuffleMemoryBytes: 512, MaxGroupBytes: 512})
	defer ctx.Stop()
	data := make([]Pair[string, int], 2000)
	for i := range data {
		data[i] = NewPair("hot", 1)
	}
	r := Parallelize(ctx, data, 1)
	sum := Collect(ReduceByKey(r, NewHashPartitioner(1), func(a, b int) int { return a + b }))
	if len(sum) != 1 || sum[0].Value != 2000 {
		t.Fatal(sum)
	}
	g := GroupByKey(r, NewHashPartitioner(1))
	computeShuffleStages(g)
	defer func() {
		v := recover()
		e, ok := v.(executionError)
		if !ok || !strings.Contains(e.err.Error(), "MaxGroupBytes") {
			t.Fatalf("wanted explicit skew limit, got %v", v)
		}
	}()
	CollectIterator(g.Compute(g.Partitions()[0]))
}

func TestBoundedLargeInput(t *testing.T) {
	if os.Getenv("GOSPARK_MEMORY_STRESS") != "1" {
		t.Skip("run in the memory-limit CI container")
	}
	const records = 40960 // 320 MiB payload, more than twice the container memory.
	ctx := NewContext(&Config{Master: MasterLocal, NumPartitions: 1, ShuffleMemoryBytes: 1 << 20, MaxRecordBytes: 32 << 10})
	defer ctx.Stop()
	payload := strings.Repeat("x", 8192)
	r := NewRDD[Pair[int, string]](ctx, func() []Partition { return NewPartitions(1) }, nil, func(Partition) Iterator[Pair[int, string]] {
		i := records
		return func() (Pair[int, string], bool) {
			if i == 0 {
				return Pair[int, string]{}, false
			}
			i--
			return NewPair(i, payload), true
		}
	})
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak.Load() {
					peak.Store(m.HeapAlloc)
				}
			}
		}
	}()
	defer func() { close(stop); <-done }()
	sorted := SortByKey(r, func(a, b int) bool { return a < b }, true, 1)
	computeShuffleStages(sorted)
	it := sorted.Compute(sorted.Partitions()[0])
	count := 0
	for {
		v, ok := it()
		if !ok {
			break
		}
		if v.Key != count || v.Value != payload {
			t.Fatalf("incorrect record %d", count)
		}
		count++
	}
	if count != records {
		t.Fatalf("got %d records", count)
	}
	// Many distinct keys plus one hot key; consume incrementally so neither
	// the generator nor the test's result collection can mask reducer growth.
	aggInput := NewRDD[Pair[int, boundedValue]](ctx, func() []Partition { return NewPartitions(1) }, nil, func(Partition) Iterator[Pair[int, boundedValue]] {
		i := 0
		return func() (Pair[int, boundedValue], bool) {
			if i == records {
				return Pair[int, boundedValue]{}, false
			}
			key := i
			if i >= records*3/4 {
				key = -1
			}
			i++
			return NewPair(key, boundedValue{Count: 1, Payload: payload}), true
		}
	})
	reduced := ReduceByKey(aggInput, NewHashPartitioner(1), func(a, b boundedValue) boundedValue { a.Count += b.Count; return a })
	computeShuffleStages(reduced)
	aggIter := reduced.Compute(reduced.Partitions()[0])
	keys, total := 0, 0
	for {
		p, ok := aggIter()
		if !ok {
			break
		}
		keys++
		total += p.Value.Count
		want := 1
		if p.Key == -1 {
			want = records / 4
		}
		if p.Value.Count != want || p.Value.Payload != payload {
			t.Fatalf("bad aggregated key %d count %d", p.Key, p.Value.Count)
		}
	}
	if keys != records*3/4+1 || total != records {
		t.Fatalf("aggregate keys=%d total=%d", keys, total)
	}
	t.Logf("input=%d MiB peakHeap=%d MiB", records*len(payload)>>20, peak.Load()>>20)
	fmt.Println("BOUNDED_MEMORY_SORT_AND_AGGREGATION_PASSED")
}

type boundedValue struct {
	Count   int
	Payload string
}
