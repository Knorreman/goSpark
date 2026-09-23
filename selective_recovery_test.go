package spark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSelectiveRecoveryKeepsHealthyMaps(t *testing.T) {
	counts := map[taskKey]int{}
	attempts := map[taskKey]int{}
	removed := false
	spec := JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	first := plan.Stages[0]
	got, err := ScheduleWith(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{
		MaxRecoveries: 4,
		OnTaskComplete: func(task Task, res ExecResult) error {
			k := taskKey{task.StageID, task.PartitionID}
			if counts[k] > 0 && task.Attempt <= attempts[k] {
				return fmt.Errorf("attempt reused")
			}
			counts[k]++
			attempts[k] = task.Attempt
			if !removed && task.StageID == first.ID && task.PartitionID == 0 {
				removed = true
				return os.RemoveAll(res.Manifest.Location)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[{a 4} {b 2}]" {
		t.Fatal(got)
	}
	if counts[taskKey{first.ID, 0}] != 2 || counts[taskKey{first.ID, 1}] != 1 {
		t.Fatalf("healthy maps recomputed: %v", counts)
	}
}

func TestRecoveryInvalidatesAlreadyCollectedResults(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, _ := PlanJob(spec)
	var manifest *MapOutputManifest
	counts := map[int]int{}
	removed := false
	got, err := ScheduleWith(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{OnTaskComplete: func(task Task, res ExecResult) error {
		if res.Manifest != nil && manifest == nil {
			m := *res.Manifest
			manifest = &m
		}
		if task.StageID == plan.ResultStageID {
			counts[task.PartitionID]++
			if !removed {
				removed = true
				return os.RemoveAll(manifest.Location)
			}
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	sums := map[string]int{}
	for _, v := range got {
		p := v.(Pair[string, int])
		sums[p.Key] += p.Value
	}
	if !reflect.DeepEqual(sums, map[string]int{"hello": 3, "world": 2, "spark": 2}) {
		t.Fatal(sums)
	}
	if counts[0] != 2 {
		t.Fatalf("accepted descendant not invalidated: %v", counts)
	}
}

func TestRecoveryBudgetExhaustion(t *testing.T) {
	_, err := ScheduleWith(JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{MaxRecoveries: 1, OnTaskComplete: func(_ Task, r ExecResult) error {
		if r.Manifest != nil {
			return os.RemoveAll(r.Manifest.Location)
		}
		return nil
	}})
	if err == nil || !strings.Contains(err.Error(), "recovery limit") {
		t.Fatalf("got %v", err)
	}
}

func TestInFlightHeartbeatCancelsRequest(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	var taskStarted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if taskStarted.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				w.WriteHeader(http.StatusOK)
			}
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		taskStarted.Store(true)
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer srv.Close()
	c := WorkerClient{BaseURL: srv.URL, Timeout: time.Second, HeartbeatInterval: 10 * time.Millisecond}
	_, err := c.Exec(Task{})
	if err == nil || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("got %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("task never started")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("request not canceled")
	}
}

func TestCancellationStopsWorkerIteration(t *testing.T) {
	var calls atomic.Int64
	RegisterJob("cancel-iteration", func(ctx *Context, _ JobSpec) (RDDAny, error) {
		return NewRDD[int](ctx, func() []Partition { return NewPartitions(1) }, nil, func(Partition) Iterator[int] {
			return func() (int, bool) {
				calls.Add(1)
				select {
				case <-ctx.TaskContext().Done():
				case <-time.After(time.Millisecond):
				}
				return 1, true
			}
		}), nil
	})
	srv, addr, err := ServeWorker(t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	c := WorkerClient{BaseURL: "http://" + addr, Timeout: 30 * time.Millisecond}
	_, err = c.Exec(Task{JobID: "cancel", Job: JobSpec{TaskName: "cancel-iteration", NumPartitions: 1}, StageID: 0})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	n := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if n == 0 || calls.Load() != n {
		t.Fatalf("worker still computing: %d -> %d", n, calls.Load())
	}
}

func TestScheduleCanceledBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ScheduleContext(ctx, JobSpec{TaskName: "sched-wc"}, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

type staleMapRunner struct{ inner TaskRunner }

func (r staleMapRunner) Exec(task Task) (ExecResult, error) {
	result, err := r.inner.Exec(task)
	if err == nil && result.Manifest != nil {
		result.Manifest.Attempt--
	}
	return result, err
}

func TestRejectsStaleMapAttempt(t *testing.T) {
	_, err := ScheduleWith(JobSpec{TaskName: "sched-wc", NumPartitions: 2}, []TaskRunner{staleMapRunner{inner: localRunner{storeDir: t.TempDir()}}}, ScheduleOpts{MaxAttempts: 1, MaxRecoveries: 1})
	if err == nil || !strings.Contains(err.Error(), "stale or mismatched map attempt") {
		t.Fatalf("stale completion accepted: %v", err)
	}
}
