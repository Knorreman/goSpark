package spark

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type rendezvousRunner struct {
	inner   TaskRunner
	started chan<- int
	release <-chan struct{}
	mu      sync.Mutex
	active  int
	peak    int
}

func (r *rendezvousRunner) Exec(task Task) (ExecResult, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.peak {
		r.peak = r.active
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()
	if task.StageID == 0 {
		r.started <- task.PartitionID
		select {
		case <-r.release:
		case <-time.After(3 * time.Second):
			return ExecResult{}, fmt.Errorf("map task waited for another worker")
		}
	}
	return r.inner.Exec(task)
}

func TestScheduleRunsIndependentPartitionsOnSeparateWorkers(t *testing.T) {
	started := make(chan int, 2)
	release := make(chan struct{})
	a := &rendezvousRunner{inner: localRunner{storeDir: t.TempDir()}, started: started, release: release}
	b := &rendezvousRunner{inner: localRunner{storeDir: t.TempDir()}, started: started, release: release}
	type outcome struct {
		recs []any
		err  error
	}
	finished := make(chan outcome, 1)
	go func() {
		recs, err := RunPipelineAny(JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{a, b})
		finished <- outcome{recs, err}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("both map partitions were not running concurrently")
		}
	}
	close(release)
	select {
	case got := <-finished:
		if got.err != nil {
			t.Fatal(got.err)
		}
		counts := map[string]int{}
		for _, rec := range got.recs {
			p := rec.(Pair[string, int])
			counts[p.Key] += p.Value
		}
		if fmt.Sprint(counts["hello"], counts["world"], counts["spark"]) != "3 2 2" {
			t.Fatalf("incorrect result: %v", counts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("schedule did not complete")
	}
	if a.peak != 1 || b.peak != 1 {
		t.Fatalf("expected one concurrent task per runner, peaks %d and %d", a.peak, b.peak)
	}
}

func TestParallelScheduleCancelWaitsForTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	ended := make(chan struct{}, 2)
	runner := &cancellableRunner{started: started, ended: ended}
	finished := make(chan error, 1)
	go func() {
		_, err := RunPipelineAnyContext(ctx, JobSpec{TaskName: "sched-wc", NumPartitions: 2}, []TaskRunner{runner, runner}, ScheduleOpts{})
		finished <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("tasks did not start")
		}
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ended:
		default:
			t.Fatal("schedule returned before an in-flight task stopped")
		}
	}
}

type cancellableRunner struct {
	started chan<- struct{}
	ended   chan<- struct{}
}

func (r *cancellableRunner) Exec(Task) (ExecResult, error) {
	return ExecResult{}, errors.New("use context runner")
}
func (r *cancellableRunner) ExecContext(ctx context.Context, _ Task) (ExecResult, error) {
	r.started <- struct{}{}
	<-ctx.Done()
	r.ended <- struct{}{}
	return ExecResult{}, ctx.Err()
}
