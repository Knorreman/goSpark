package spark

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

var cachedInputCalls atomic.Int64
var reusableOutputCalls atomic.Int64

func init() {
	RegisterJob("reusable-output", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		return Map(Parallelize(ctx, []int{1, 2, 3, 4}, 4), func(v int) int {
			reusableOutputCalls.Add(1)
			return v * 10
		}), nil
	})
	RegisterJob("cached-fanout", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		input := Parallelize(ctx, []Pair[int, int]{NewPair(1, 10), NewPair(2, 20), NewPair(3, 30)}, 3)
		shared := Cache(Map(input, func(p Pair[int, int]) Pair[int, int] {
			cachedInputCalls.Add(1)
			return p
		}))
		left := ReduceByKey(shared, NewHashPartitioner(3), func(a, b int) int { return a + b })
		right := Map(GroupByKey(shared, NewHashPartitioner(3)), func(p Pair[int, []int]) Pair[int, int] {
			return NewPair(p.Key, len(p.Value))
		})
		return Join(left, right, NewHashPartitioner(3)), nil
	})
}

func TestScheduleCacheAcrossJobsAndWorkerReplacement(t *testing.T) {
	reusableOutputCalls.Store(0)
	dir := t.TempDir()
	srv, addr, err := ServeWorker(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	a := &WorkerClient{BaseURL: "http://" + addr}
	b := startCacheTestWorker(t)
	runners := []TaskRunner{a, b}
	cache := NewScheduleCache(runners)
	spec, err := cache.Persist("numbers", JobSpec{TaskName: "reusable-output", Action: ActionCollect, NumPartitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	run := func(want int64) {
		t.Helper()
		recs, err := Schedule(spec, runners)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(recs, []any{10, 20, 30, 40}) || reusableOutputCalls.Load() != want {
			t.Fatalf("records=%v computations=%d, want %d", recs, reusableOutputCalls.Load(), want)
		}
	}
	run(4)
	run(4)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, _, err := ServeWorker(dir, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	run(6) // Two partitions were on the replaced worker.
	if err := cache.Unpersist(context.Background(), "numbers"); err != nil {
		t.Fatal(err)
	}
	run(10)
	if err := cache.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleCacheFingerprintSeparatesInputs(t *testing.T) {
	reusableOutputCalls.Store(0)
	worker := startCacheTestWorker(t)
	cache := NewScheduleCache([]TaskRunner{worker})
	defer cache.Stop(context.Background())
	base := JobSpec{TaskName: "reusable-output", Action: ActionCollect, NumPartitions: 4}
	first, _ := cache.Persist("numbers", base)
	second := first
	second.Params = map[string]string{"version": "new"}
	for _, spec := range []JobSpec{first, second, first} {
		if _, err := Schedule(spec, []TaskRunner{worker}); err != nil {
			t.Fatal(err)
		}
	}
	if got := reusableOutputCalls.Load(); got != 8 {
		t.Fatalf("different fingerprint reused cached output: computations=%d", got)
	}
}

func TestScheduleCacheReusesPreShuffleInput(t *testing.T) {
	cachedInputCalls.Store(0)
	runners := []TaskRunner{startCacheTestWorker(t), startCacheTestWorker(t)}
	cache := NewScheduleCache(runners)
	spec, err := cache.Persist("fanout", JobSpec{TaskName: "cached-fanout", Action: ActionCollect, NumPartitions: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		recs, err := Schedule(spec, runners)
		if err != nil || len(recs) != 3 {
			t.Fatalf("schedule %d: records=%v err=%v", i, recs, err)
		}
	}
	if got := cachedInputCalls.Load(); got != 3 {
		t.Fatalf("pre-shuffle input computed %d times; want 3", got)
	}
	if err := cache.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := Schedule(spec, runners); err != nil {
		t.Fatal(err)
	}
	if got := cachedInputCalls.Load(); got != 6 {
		t.Fatalf("Stop retained cached input: computations=%d", got)
	}
}

type cacheWorker struct {
	*WorkerClient
	id     int
	mu     *sync.Mutex
	placed map[taskKey]int
}

type disappearingCacheWorker struct {
	*cacheWorker
	kill  func()
	dead  atomic.Bool
	stage int
}

func (r *disappearingCacheWorker) AliveContext(ctx context.Context) bool {
	return !r.dead.Load() && r.cacheWorker.AliveContext(ctx)
}

func (r *disappearingCacheWorker) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	res, err := r.cacheWorker.ExecContext(ctx, task)
	if err == nil && task.StageID == r.stage && task.PartitionID == 0 && r.dead.CompareAndSwap(false, true) {
		r.kill()
	}
	return res, err
}

func (r *cacheWorker) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	result, err := r.WorkerClient.ExecContext(ctx, task)
	if err == nil {
		r.mu.Lock()
		r.placed[taskKey{task.StageID, task.PartitionID}] = r.id
		r.mu.Unlock()
	}
	return result, err
}

func TestDistributedCacheReusedOnPreferredWorkers(t *testing.T) {
	cachedInputCalls.Store(0)
	a, b := startCacheTestWorker(t), startCacheTestWorker(t)
	var mu sync.Mutex
	placed := map[taskKey]int{}
	runners := []TaskRunner{
		&cacheWorker{a, 0, &mu, placed},
		&cacheWorker{b, 1, &mu, placed},
	}
	spec := JobSpec{TaskName: "cached-fanout", Action: ActionCollect, NumPartitions: 3}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	var cacheStage []Stage
	for _, st := range plan.Stages {
		if st.Kind == StageShuffleMap && len(st.CacheHints) == 3 && len(st.CacheHints[0]) != 0 {
			cacheStage = append(cacheStage, st)
		}
	}
	if len(cacheStage) != 2 {
		t.Fatalf("expected two cached input branches, got %+v", cacheStage)
	}
	recs, err := Schedule(spec, runners)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]Pair[int, int]{}
	for _, rec := range recs {
		p := rec.(Pair[int, Pair[int, int]])
		got[p.Key] = p.Value
	}
	if !reflect.DeepEqual(got, map[int]Pair[int, int]{1: NewPair(10, 1), 2: NewPair(20, 1), 3: NewPair(30, 1)}) {
		t.Fatalf("wrong result: %v", got)
	}
	if calls := cachedInputCalls.Load(); calls != 3 {
		t.Fatalf("shared input computed %d times; want one per partition", calls)
	}
	for part := 0; part < 3; part++ {
		first := placed[taskKey{cacheStage[0].ID, part}]
		second := placed[taskKey{cacheStage[1].ID, part}]
		if first != second {
			t.Fatalf("partition %d moved off its cached worker: %d -> %d", part, first, second)
		}
	}
}

func startCacheTestWorker(t *testing.T) *WorkerClient {
	t.Helper()
	srv, addr, err := ServeWorker(t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return &WorkerClient{BaseURL: "http://" + addr}
}

func TestShuffleDependentCacheNotSharedAcrossTasks(t *testing.T) {
	ctx := NewContext(&Config{AppName: "cache-lineage"})
	defer ctx.Stop()
	input := Parallelize(ctx, []Pair[int, int]{NewPair(1, 1)}, 1)
	stable := Cache(Map(input, func(p Pair[int, int]) Pair[int, int] { return p }))
	if !cacheAcrossTasks(stable) {
		t.Fatal("narrow-only input should be shareable")
	}
	shuffled := Cache(ReduceByKey(stable, NewHashPartitioner(1), func(a, b int) int { return a + b }))
	if cacheAcrossTasks(shuffled) || len(cacheHints(shuffled, 0)) != 0 {
		t.Fatal("shuffle-dependent cache cannot survive lost-map repair")
	}
	if fmt.Sprint(cacheHints(stable, 0)) == "[]" {
		t.Fatal("stable input missing locality hint")
	}
}

func TestCacheLocationDroppedAfterExecutorLoss(t *testing.T) {
	cachedInputCalls.Store(0)
	srv, addr, err := ServeWorker(t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	a := &WorkerClient{BaseURL: "http://" + addr}
	b := startCacheTestWorker(t)
	var mu sync.Mutex
	placed := map[taskKey]int{}
	spec := JobSpec{TaskName: "cached-fanout", Action: ActionCollect, NumPartitions: 3}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	first, second := plan.Stages[0], plan.Stages[1]
	dead := &disappearingCacheWorker{
		cacheWorker: &cacheWorker{a, 0, &mu, placed},
		kill:        func() { _ = srv.Close() },
		stage:       first.ID,
	}
	recs, err := Schedule(spec, []TaskRunner{dead, &cacheWorker{b, 1, &mu, placed}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("missing results after executor loss: %v", recs)
	}
	if !dead.dead.Load() || placed[taskKey{second.ID, 0}] != 1 {
		t.Fatalf("failed to evict dead cache location: %v", placed)
	}
	for _, rec := range recs {
		p := rec.(Pair[int, Pair[int, int]])
		if p.Value.Value != 1 || p.Value.Key != p.Key*10 {
			t.Fatalf("wrong result after recovery: %v", recs)
		}
	}
}

func TestUnpersistReportsCacheEviction(t *testing.T) {
	ctx := NewContext(&Config{AppName: "cache-eviction", NumPartitions: 1})
	defer ctx.Stop()
	ctx.distributedCache = newMemoryCache()
	rdd := Cache(Parallelize(ctx, []int{1, 2}, 1))
	if got := Collect(rdd); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	key := CachePartition{RDDID: rdd.ID(), PartitionID: 0}
	if !ctx.distributedCache.has(key.RDDID, key.PartitionID) {
		t.Fatal("cache not populated")
	}
	Unpersist(rdd)
	if ctx.distributedCache.has(key.RDDID, key.PartitionID) {
		t.Fatal("unpersist left worker cache behind")
	}
	cached, dropped := ctx.cacheUpdates()
	if len(cached) != 0 || !reflect.DeepEqual(dropped, []CachePartition{key}) {
		t.Fatalf("unexpected cache updates: added=%v removed=%v", cached, dropped)
	}
}

func TestReplacedWorkerInvalidatesCacheLocations(t *testing.T) {
	dir := t.TempDir()
	srv, addr, err := ServeWorker(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client := &WorkerClient{BaseURL: "http://" + addr}
	if !client.Alive() {
		t.Fatal("initial worker unavailable")
	}
	previous := client.WorkerID()
	key := CachePartition{RDDID: 2, PartitionID: 0}
	s := &lineageScheduler{
		ctx:            context.Background(),
		runners:        []TaskRunner{client},
		cacheLocations: map[CachePartition]map[int]bool{key: {0: true}},
		workerIDs:      map[int]string{0: previous},
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, _, err := ServeWorker(dir, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if index := s.pickWorkerIndexFor(nil, []CachePartition{key}); index != 0 {
		t.Fatalf("replacement unavailable: %d", index)
	}
	if client.WorkerID() == previous || len(s.cacheLocations) != 0 {
		t.Fatalf("stale location survived worker replacement: %v", s.cacheLocations)
	}
}

func TestDistributedCacheTwoProcesses(t *testing.T) {
	updates := 0
	recs, err := ScheduleWith(JobSpec{TaskName: "cached-fanout", Action: ActionCollect, NumPartitions: 3},
		[]TaskRunner{startTestWorker(t), startTestWorker(t)}, ScheduleOpts{OnTaskComplete: func(task Task, res ExecResult) error {
			if res.Manifest != nil && len(res.Cached) > 0 {
				updates++
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || updates == 0 {
		t.Fatalf("worker RPC lost cache locations or results: %v updates=%d", recs, updates)
	}
}
