package spark

import (
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestDiskShuffleRoundTrip(t *testing.T) {
	dir := t.TempDir()
	a := NewDiskShuffleStore(dir)
	b := NewDiskShuffleStore(dir)
	buckets := map[int][]any{
		0: {NewPair("a", 1), NewPair("c", 1)},
		1: {NewPair("b", 1)},
	}
	man, err := a.WriteMap("job1", 7, 0, 0, 2, buckets)
	if err != nil {
		t.Fatal(err)
	}
	got0, err := b.ReadBucket(man, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got0) != 2 {
		t.Fatalf("reduce 0: got %d records", len(got0))
	}
	got1, err := b.ReadBucket(man, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got1) != 1 {
		t.Fatalf("reduce 1: got %d records", len(got1))
	}
}

func TestDiskShuffleEmptyBucket(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskShuffleStore(dir)
	man, err := store.WriteMap("job1", 1, 0, 0, 2, map[int][]any{0: nil, 1: {NewPair("x", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := store.ReadBucket(man, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty bucket, got %v", empty)
	}
}

func TestDiskShuffleMissingAndTruncated(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskShuffleStore(dir)
	_, err := store.ReadBucket(MapOutputManifest{JobID: "j", ShuffleID: 1, MapID: 0, Location: filepath.Join(dir, "nope")}, 0)
	if err == nil {
		t.Fatal("expected missing file error")
	}
	man, err := store.WriteMap("job1", 1, 0, 0, 1, map[int][]any{0: {NewPair("a", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(man.Location, bucketName(0))
	if err := os.WriteFile(path, []byte("xxxx"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err = store.ReadBucket(man, 0)
	if err == nil {
		t.Fatal("expected truncated/bad magic error")
	}
}

func TestExecuteTaskWordCountNoRecompute(t *testing.T) {
	var computes atomic.Int64
	RegisterPipeline("exec-wc", func(ctx *Context, spec JobSpec) (*RDD[Pair[string, int]], error) {
		np := spec.NumPartitions
		lines := []string{"hello world", "hello spark", "world spark hello"}
		rdd := Parallelize(ctx, lines, np)
		words := FlatMap(rdd, func(line string) []string { return splitWords(line) })
		pairs := Map(words, func(w string) Pair[string, int] {
			computes.Add(1)
			return NewPair(w, 1)
		})
		return ReduceByKey(pairs, NewHashPartitioner(np), func(a, b int) int { return a + b }), nil
	})
	spec := JobSpec{TaskName: "exec-wc", Action: ActionCollect, NumPartitions: 2}
	ctx := NewContext(&Config{AppName: "exec-wc", Master: MasterLocal, NumPartitions: 2})
	defer ctx.Stop()
	rdd, _ := getJob("exec-wc")
	got, err := rdd(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{}
	for _, p := range Collect(got.(*RDD[Pair[string, int]])) {
		want[p.Key] = p.Value
	}
	mapCount := computes.Load()
	computes.Store(0)

	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()
	var maps []MapOutputManifest
	var shuffleStage, resultStage Stage
	for _, s := range plan.Stages {
		if s.Kind == StageShuffleMap {
			shuffleStage = s
		}
		if s.Kind == StageResult {
			resultStage = s
		}
	}
	for i := 0; i < shuffleStage.NumPartitions; i++ {
		res, err := ExecuteTask(Task{
			JobID:       "exec-wc",
			Job:         spec,
			StageID:     shuffleStage.ID,
			PartitionID: i,
			StoreDir:    storeDir,
		})
		if err != nil {
			t.Fatalf("map %d: %v", i, err)
		}
		maps = append(maps, *res.Manifest)
	}
	afterMaps := computes.Load()
	if afterMaps == 0 {
		t.Fatal("expected map tasks to compute records")
	}
	got2 := map[string]int{}
	for i := 0; i < resultStage.NumPartitions; i++ {
		res, err := ExecuteTask(Task{
			JobID:       "exec-wc",
			Job:         spec,
			StageID:     resultStage.ID,
			PartitionID: i,
			StoreDir:    storeDir,
			Upstream:    maps,
		})
		if err != nil {
			t.Fatalf("reduce %d: %v", i, err)
		}
		for _, rec := range res.Records {
			p := rec.(Pair[string, int])
			got2[p.Key] += p.Value
		}
	}
	if computes.Load() != afterMaps {
		t.Fatalf("reduce recomputed maps: before %d after %d", afterMaps, computes.Load())
	}
	if len(got2) != len(want) {
		t.Fatalf("keys: got %v want %v (local maps were %d)", got2, want, mapCount)
	}
	for k, v := range want {
		if got2[k] != v {
			t.Fatalf("key %s: got %d want %d", k, got2[k], v)
		}
	}
}

func TestExecuteResultRequiresUpstream(t *testing.T) {
	RegisterPipeline("exec-wc-up", func(ctx *Context, spec JobSpec) (*RDD[Pair[string, int]], error) {
		pairs := Map(Parallelize(ctx, []string{"a b"}, spec.NumPartitions), func(s string) Pair[string, int] {
			return NewPair(s, 1)
		})
		return ReduceByKey(pairs, NewHashPartitioner(spec.NumPartitions), func(a, b int) int { return a + b }), nil
	})
	spec := JobSpec{TaskName: "exec-wc-up", Action: ActionCollect, NumPartitions: 1}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	var result Stage
	for _, s := range plan.Stages {
		if s.Kind == StageResult {
			result = s
		}
	}
	_, err = ExecuteTask(Task{Job: spec, StageID: result.ID, PartitionID: 0, StoreDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected error when reduce has no upstream")
	}
}

func TestShuffleHTTPFetch(t *testing.T) {
	dir := t.TempDir()
	store := NewDiskShuffleStore(dir)
	man, err := store.WriteMap("jobh", 3, 0, 0, 1, map[int][]any{0: {NewPair("z", 9)}})
	if err != nil {
		t.Fatal(err)
	}
	srv, addr, err := ServeShuffle(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	recs, err := FetchShuffleBucket("http://"+addr, man, 0, DefaultCodec())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records", len(recs))
	}
	p := recs[0].(Pair[string, int])
	if p.Key != "z" || p.Value != 9 {
		t.Fatalf("got %+v", p)
	}
	resp, err := http.Get("http://" + addr + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health %d", resp.StatusCode)
	}
}
