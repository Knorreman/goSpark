package spark

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type loseOutputRunner struct {
	dir  string
	lost bool
	ids  map[string]bool
}

func (r *loseOutputRunner) Exec(task Task) (ExecResult, error) {
	r.ids[task.JobID] = true
	// Delete already-published map outputs as a later shuffle starts.
	if !r.lost && len(task.Upstream) > 0 {
		r.lost = true
		if err := os.RemoveAll(r.dir); err != nil {
			return ExecResult{}, err
		}
	}
	task.StoreDir = r.dir
	return ExecuteTask(task)
}

func TestRecoveryRepairsMultiStageDAG(t *testing.T) {
	r := &loseOutputRunner{dir: t.TempDir(), ids: map[string]bool{}}
	got, err := ScheduleWith(JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{r}, ScheduleOpts{MaxAttempts: 1, MaxRecoveries: 4})
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for _, v := range got {
		values = append(values, fmt.Sprint(v))
	}
	if !reflect.DeepEqual(values, []string{"{a 4}", "{b 2}"}) {
		t.Fatal(values)
	}
	if len(r.ids) != 1 {
		t.Fatalf("expected selective recovery in one execution, got %d", len(r.ids))
	}
}

func TestWorkerRPCDeadline(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer s.Close()
	c := WorkerClient{BaseURL: s.URL, Timeout: 10 * time.Millisecond}
	if _, err := c.Exec(Task{}); err == nil {
		t.Fatal("hung worker accepted")
	}
}

type killAfterMap struct {
	client *WorkerClient
	kill   func()
	killed bool
}

func (r *killAfterMap) Alive() bool { return !r.killed && r.client.Alive() }
func (r *killAfterMap) Exec(task Task) (ExecResult, error) {
	res, err := r.client.Exec(task)
	if err == nil && res.Manifest != nil && !r.killed {
		r.killed = true
		r.kill()
	}
	return res, err
}
func TestRecoveryAfterWorkerProcessDeath(t *testing.T) {
	a, kill := startKillableWorker(t)
	b := startTestWorker(t)
	r := &killAfterMap{client: a, kill: kill}
	counts := make(map[taskKey]int)
	ids := make(map[string]bool)
	var mu sync.Mutex
	got, err := ScheduleWith(JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{r, b}, ScheduleOpts{MaxAttempts: 1, MaxRecoveries: 2, OnTaskComplete: func(task Task, res ExecResult) error {
		mu.Lock()
		defer mu.Unlock()
		counts[taskKey{task.StageID, task.PartitionID}]++
		ids[task.JobID] = true
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || fmt.Sprint(got[0]) != "{a 4}" || fmt.Sprint(got[1]) != "{b 2}" {
		t.Fatal(got)
	}
	if len(ids) != 1 || counts[taskKey{0, 0}] != 2 || counts[taskKey{0, 1}] != 1 {
		t.Fatalf("expected selective recovery in one job, ids=%v maps=%v", ids, counts)
	}
}

func TestWorkerReportsTypedFetchFailure(t *testing.T) {
	spec := JobSpec{TaskName: "sched-wc", Action: ActionCollect, NumPartitions: 2}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var maps []MapOutputManifest
	for p := 0; p < 2; p++ {
		r, err := ExecuteTask(Task{JobID: "fetch-test", Job: spec, StageID: plan.Stages[0].ID, PartitionID: p, StoreDir: dir, Fingerprint: plan.Fingerprint})
		if err != nil {
			t.Fatal(err)
		}
		maps = append(maps, *r.Manifest)
	}
	if err := os.Remove(filepath.Join(maps[1].Location, bucketName(0))); err != nil {
		t.Fatal(err)
	}
	srv, addr, err := ServeWorker(dir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	client := &WorkerClient{BaseURL: "http://" + addr}
	_, err = client.Exec(Task{JobID: "fetch-test", Job: spec, StageID: plan.ResultStageID, PartitionID: 0, Upstream: maps, Fingerprint: plan.Fingerprint})
	var fetch *FetchError
	if !errors.As(err, &fetch) || fetch.ShuffleID != maps[1].ShuffleID || fetch.MapID != 1 || fetch.JobID != "fetch-test" {
		t.Fatalf("expected typed missing map output, got %v", err)
	}
}
