package spark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func init() {
	RegisterJob("dir-input-wc", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		lines := TextFile(ctx, spec.Params["path"], spec.NumPartitions)
		words := FlatMap(lines, func(line string) []string {
			if line == "" {
				return nil
			}
			return []string{line}
		})
		pairs := Map(words, func(word string) Pair[string, int] { return NewPair(word, 1) })
		return ReduceByKey(pairs, NewHashPartitioner(spec.NumPartitions), func(a, b int) int { return a + b }), nil
	})
	RegisterJob("shipped-text-lines", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		return TextFile(ctx, spec.Params["path"], spec.NumPartitions), nil
	})
}

func TestTextFileDirectoryAndNestedFiles(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "a\nb\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "a\nc\n")
	writeInput(t, filepath.Join(dir, "_SUCCESS"), "ignore\n")
	writeInput(t, filepath.Join(dir, ".hidden"), "hidden\n")
	if err := os.Mkdir(filepath.Join(dir, "part"), 0755); err != nil {
		t.Fatal(err)
	}
	writeInput(t, filepath.Join(dir, "part", "c.txt"), "c\n")
	ctx := NewContext(&Config{AppName: "dir-input", Master: MasterLocal})
	defer ctx.Stop()
	got := Collect(TextFile(ctx, dir, 2))
	if !reflect.DeepEqual(countStrings(got), map[string]int{"a": 2, "b": 1, "c": 2}) {
		t.Fatalf("directory input = %v", got)
	}
	if len(TextFile(ctx, dir, 2).Partitions()) != 2 {
		t.Fatal("expected files to be packed into the requested partitions")
	}
}

func TestTextFileKeepsSingleObjectSeparateFromPrefix(t *testing.T) {
	bucket := fmt.Sprintf("text-prefix-%d", os.Getpid())
	store := RegisterMemBucket(bucket)
	defer UnregisterMemBucket(bucket)
	if err := store.Put("data/lines.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("data/lines.txt.extra", []byte("two\n")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("data/part-000/rows.txt", []byte("three\n")); err != nil {
		t.Fatal(err)
	}
	ctx := NewContext(&Config{AppName: "s3-input", Master: MasterLocal})
	defer ctx.Stop()
	one := Collect(TextFile(ctx, "s3://"+bucket+"/data/lines.txt", 2))
	if !reflect.DeepEqual(one, []string{"one"}) {
		t.Fatalf("single object included prefix siblings: %v", one)
	}
	all := Collect(TextFile(ctx, "s3://"+bucket+"/data/", 2))
	if !reflect.DeepEqual(countStrings(all), map[string]int{"one": 1, "two": 1, "three": 1}) {
		t.Fatalf("prefix input = %v", all)
	}
}

func TestDistributedDirectoryInput(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "alpha\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "alpha\nbeta\n")
	recs, err := Schedule(JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": dir},
	}, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range recs {
		p := rec.(Pair[string, int])
		got[p.Key] += p.Value
	}
	if !reflect.DeepEqual(got, map[string]int{"alpha": 2, "beta": 1}) {
		t.Fatalf("distributed directory input = %v", got)
	}
}

func TestDriverShippedSplitsIgnoreWorkerListing(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "alpha\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "alpha\nbeta\n")
	worker, _ := startKillableWorkerEnv(t, "GOSPARK_TEST_HIDE_TEXT_LISTING=1")
	runner := &splitCheckRunner{WorkerClient: worker}
	recs, err := Schedule(JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": dir},
	}, []TaskRunner{runner})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range recs {
		p := rec.(Pair[string, int])
		got[p.Key] += p.Value
	}
	if !reflect.DeepEqual(got, map[string]int{"alpha": 2, "beta": 1}) {
		t.Fatalf("shipped directory splits = %v", got)
	}
	if len(runner.splits) == 0 {
		t.Fatal("worker was not given shipped splits")
	}
	seen := map[string]InputSplit{}
	for _, split := range runner.splits {
		if split.Offset != 0 {
			t.Fatalf("directory file was byte-split: %+v", split)
		}
		if split.Length <= 0 || split.Path == "" {
			t.Fatalf("incomplete split: %+v", split)
		}
		seen[filepath.Base(split.Path)] = split
	}
	if seen["a.txt"].Length != int64(len("alpha\n")) || seen["b.txt"].Length != int64(len("alpha\nbeta\n")) {
		t.Fatalf("shipped splits = %+v", runner.splits)
	}
}

func TestDriverShippedByteRangeSplits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lines.txt")
	body := "alpha\nbeta\ngamma\ndelta\nepsilon\nzeta\n"
	writeInput(t, path, body)
	ctx := NewContext(&Config{AppName: "shipped-lines", Master: MasterLocal, NumPartitions: 3})
	defer ctx.Stop()
	var want []string
	for _, line := range Collect(TextFile(ctx, path, 3)) {
		want = append(want, line)
	}
	worker, _ := startKillableWorkerEnv(t, "GOSPARK_TEST_HIDE_TEXT_LISTING=1")
	recs, err := Schedule(JobSpec{
		TaskName: "shipped-text-lines", Action: ActionCollect, NumPartitions: 3,
		Params: map[string]string{"path": path},
	}, []TaskRunner{&splitCheckRunner{WorkerClient: worker}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, rec := range recs {
		got = append(got, rec.(string))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shipped byte ranges = %v want %v", got, want)
	}
}

func TestShippedSplitMissingFileFailsTask(t *testing.T) {
	dir := t.TempDir()
	gone := filepath.Join(dir, "a.txt")
	writeInput(t, gone, "alpha\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "beta\n")
	runner := &deleteOnceRunner{inner: localRunner{storeDir: t.TempDir()}, path: gone}
	_, err := ScheduleWith(JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": dir},
	}, []TaskRunner{runner}, ScheduleOpts{MaxAttempts: 1})
	if err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("expected missing file to fail the task, got %v", err)
	}
}

func TestDriverShippedS3PrefixSplits(t *testing.T) {
	bucket := fmt.Sprintf("ship-s3-%d", os.Getpid())
	store := RegisterMemBucket(bucket)
	defer UnregisterMemBucket(bucket)
	if err := store.Put("data/a.txt", []byte("alpha\n")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("data/b.txt", []byte("alpha\nbeta\n")); err != nil {
		t.Fatal(err)
	}
	runner := &hideListingRunner{inner: localRunner{storeDir: t.TempDir()}}
	recs, err := Schedule(JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": "s3://" + bucket + "/data/"},
	}, []TaskRunner{runner})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range recs {
		p := rec.(Pair[string, int])
		got[p.Key] += p.Value
	}
	if !reflect.DeepEqual(got, map[string]int{"alpha": 2, "beta": 1}) {
		t.Fatalf("shipped S3 splits = %v", got)
	}
	if len(runner.splits) != 2 {
		t.Fatalf("expected two object splits, got %+v", runner.splits)
	}
}

func TestShippedSplitsMatchHiddenListing(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "alpha\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "beta\n")
	spec := JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": dir},
	}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.InputSplits) == 0 {
		t.Fatal("driver plan captured no splits")
	}
	hideTextListing = func(string) bool { return true }
	defer func() { hideTextListing = nil }()
	hidden, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	if hidden.Fingerprint == plan.Fingerprint {
		t.Fatal("hidden listing matched a plan that was not shipped")
	}
	spec.InputSplits = plan.InputSplits
	shipped, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	if shipped.Fingerprint != plan.Fingerprint {
		t.Fatalf("shipped splits fingerprint %s != %s", shipped.Fingerprint, plan.Fingerprint)
	}
}

type splitCheckRunner struct {
	*WorkerClient
	mu     sync.Mutex
	splits []InputSplit
}

func (r *splitCheckRunner) Exec(task Task) (ExecResult, error) {
	return r.ExecContext(context.Background(), task)
}

func (r *splitCheckRunner) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	if len(task.Job.InputSplits) == 0 {
		return ExecResult{}, fmt.Errorf("missing shipped input splits")
	}
	r.mu.Lock()
	r.splits = append([]InputSplit(nil), task.Job.InputSplits...)
	r.mu.Unlock()
	return r.WorkerClient.ExecContext(ctx, task)
}

type hideListingRunner struct {
	inner  TaskRunner
	mu     sync.Mutex
	splits []InputSplit
}

func (h *hideListingRunner) Exec(task Task) (ExecResult, error) {
	return h.ExecContext(context.Background(), task)
}

func (h *hideListingRunner) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	if len(task.Job.InputSplits) == 0 {
		return ExecResult{}, fmt.Errorf("missing shipped input splits")
	}
	h.mu.Lock()
	h.splits = append([]InputSplit(nil), task.Job.InputSplits...)
	h.mu.Unlock()
	prev := hideTextListing
	hideTextListing = func(string) bool { return true }
	defer func() { hideTextListing = prev }()
	return h.inner.(ContextTaskRunner).ExecContext(ctx, task)
}

type deleteOnceRunner struct {
	inner TaskRunner
	path  string
	once  sync.Once
}

func (r *deleteOnceRunner) Exec(task Task) (ExecResult, error) {
	return r.ExecContext(context.Background(), task)
}

func (r *deleteOnceRunner) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	if len(task.Job.InputSplits) == 0 {
		return ExecResult{}, fmt.Errorf("missing shipped input splits")
	}
	r.once.Do(func() { _ = os.Remove(r.path) })
	return r.inner.(ContextTaskRunner).ExecContext(ctx, task)
}

func writeInput(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

func countStrings(values []string) map[string]int {
	out := map[string]int{}
	for _, value := range values {
		out[value]++
	}
	return out
}
