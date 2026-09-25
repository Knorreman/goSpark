package spark

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	registerSchedWC()
	if os.Getenv("GOSPARK_TEST_HIDE_TEXT_LISTING") == "1" {
		hideTextListing = func(string) bool { return true }
	}
	if os.Getenv("GOSPARK_WORKER_SERVE") == "1" {
		store := os.Getenv("GOSPARK_STORE")
		srv, addr, err := ServeWorker(store, "127.0.0.1:0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "worker serve: %v\n", err)
			os.Exit(1)
		}
		_ = srv
		fmt.Printf("LISTEN=%s\n", addr)
		os.Stdout.Sync()
		select {}
	}
	os.Exit(m.Run())
}

func registerSchedWC() {
	RegisterJob("sched-join", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		left := Parallelize(ctx, []Pair[int, string]{
			NewPair(1, "a"), NewPair(2, "b"), NewPair(3, "c"),
		}, np)
		right := Parallelize(ctx, []Pair[int, int]{
			NewPair(1, 10), NewPair(1, 11), NewPair(2, 20),
		}, np)
		return Join(left, right, NewHashPartitioner(np)), nil
	})
	RegisterJob("sched-wc", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		lines := []string{"hello world", "hello spark", "world spark hello"}
		rdd := Parallelize(ctx, lines, np)
		words := FlatMap(rdd, func(line string) []string { return splitWords(line) })
		pairs := Map(words, func(w string) Pair[string, int] { return NewPair(w, 1) })
		return ReduceByKey(pairs, NewHashPartitioner(np), func(a, b int) int { return a + b }), nil
	})
}

func TestScheduleLocalMatchesCollect(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	ctx := NewContext(&Config{AppName: "sched-wc", Master: MasterLocal, NumPartitions: 2})
	defer ctx.Stop()
	factory, _ := GetJob("sched-wc")
	rdd, err := factory(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{}
	for _, p := range Collect(rdd.(*RDD[Pair[string, int]])) {
		want[p.Key] = p.Value
	}
	gotRecs, err := Schedule(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range gotRecs {
		p := rec.(Pair[string, int])
		got[p.Key] += p.Value
	}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: got %d want %d", k, got[k], v)
		}
	}
}

func TestScheduleTwoProcessesHTTPShuffle(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	ctx := NewContext(&Config{AppName: "sched-wc", Master: MasterLocal, NumPartitions: 2})
	defer ctx.Stop()
	factory, _ := GetJob("sched-wc")
	rdd, err := factory(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{}
	for _, p := range Collect(rdd.(*RDD[Pair[string, int]])) {
		want[p.Key] = p.Value
	}

	w1 := startTestWorker(t)
	w2 := startTestWorker(t)
	gotRecs, err := Schedule(spec, []TaskRunner{w1, w2})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range gotRecs {
		p, ok := rec.(Pair[string, int])
		if !ok {
			t.Fatalf("unexpected record %T %v", rec, rec)
		}
		got[p.Key] += p.Value
	}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: got %d want %d", k, got[k], v)
		}
	}
}

func startTestWorker(t *testing.T) *WorkerClient {
	c, _ := startKillableWorker(t)
	return c
}

func startKillableWorker(t *testing.T) (*WorkerClient, func()) {
	return startKillableWorkerEnv(t)
}

func startKillableWorkerEnv(t *testing.T, extraEnv ...string) (*WorkerClient, func()) {
	t.Helper()
	store := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "GOSPARK_WORKER_SERVE=1", "GOSPARK_STORE="+store)
	cmd.Env = append(cmd.Env, extraEnv...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	done := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		if sc.Scan() {
			done <- sc.Text()
			return
		}
		done <- ""
	}()
	var line string
	select {
	case line = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for worker LISTEN")
	}
	if !strings.HasPrefix(line, "LISTEN=") {
		t.Fatalf("expected LISTEN= got %q", line)
	}
	addr := strings.TrimPrefix(line, "LISTEN=")
	return &WorkerClient{BaseURL: "http://" + addr}, func() { _ = cmd.Process.Kill() }
}

type failOnceRunner struct {
	once  sync.Once
	inner TaskRunner
}

func (f *failOnceRunner) Exec(task Task) (ExecResult, error) {
	failed := false
	f.once.Do(func() { failed = true })
	if failed {
		return ExecResult{}, fmt.Errorf("injected failure")
	}
	return f.inner.Exec(task)
}

func TestScheduleRetriesThenSucceeds(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	inner := localRunner{storeDir: t.TempDir()}
	got, err := ScheduleWith(spec, []TaskRunner{&failOnceRunner{inner: inner}}, ScheduleOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected records after retry")
	}
}

func TestLatestManifestsPicksHigherAttempt(t *testing.T) {
	got := latestManifests([]MapOutputManifest{
		{ShuffleID: 1, MapID: 0, Attempt: 0, JobID: "old"},
		{ShuffleID: 1, MapID: 0, Attempt: 2, JobID: "new"},
		{ShuffleID: 1, MapID: 1, Attempt: 0, JobID: "m1"},
		{ShuffleID: 2, MapID: 0, Attempt: 0, JobID: "s2"},
	})
	if len(got) != 3 {
		t.Fatalf("len=%d %+v", len(got), got)
	}
}

func TestCombineMapOutput(t *testing.T) {
	dep := NewShuffleDep(nil, NewHashPartitioner(1), 1, true, &AggregatorAny{
		CreateCombiner: func(v any) any { return v },
		MergeValue:     func(c, v any) any { return c.(int) + v.(int) },
		MergeCombiners: func(a, b any) any { return a.(int) + b.(int) },
	}, func(item any) any {
		return item.(Pair[string, int]).Key
	})
	items := []any{NewPair("a", 1), NewPair("a", 2), NewPair("b", 3)}
	got := combineMapOutput(items, dep)
	if len(got) != 2 {
		t.Fatalf("expected 2 combined, got %d %v", len(got), got)
	}
	sums := map[string]int{}
	for _, rec := range got {
		p := rec.(Pair[string, int])
		sums[p.Key] = p.Value
	}
	if sums["a"] != 3 || sums["b"] != 3 {
		t.Fatalf("got %v", sums)
	}
}

type deadRunner struct{}

func (deadRunner) Exec(Task) (ExecResult, error) { return ExecResult{}, fmt.Errorf("dead") }
func (deadRunner) Alive() bool                   { return false }

func TestScheduleSkipsDeadWorker(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	got, err := Schedule(spec, []TaskRunner{deadRunner{}, localRunner{storeDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected records")
	}
}

func TestScheduleJoinMatchesCollect(t *testing.T) {
	spec := JobSpec{TaskName: "sched-join", Action: ActionCollect, NumPartitions: 2}
	ctx := NewContext(&Config{AppName: "sched-join", Master: MasterLocal, NumPartitions: 2})
	defer ctx.Stop()
	factory, _ := GetJob("sched-join")
	rdd, err := factory(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	want := Collect(rdd.(*RDD[Pair[int, Pair[string, int]]]))
	gotRecs, err := Schedule(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}, localRunner{storeDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotRecs) != len(want) {
		plan, _ := PlanJob(spec)
		t.Fatalf("join scheduled %d collect %d stages=%+v", len(gotRecs), len(want), plan.Stages)
	}
}
