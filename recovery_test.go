package spark

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
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

func TestRecoveryReplaysMultiStageDAG(t *testing.T) {
	r := &loseOutputRunner{dir: t.TempDir(), ids: map[string]bool{}}
	got, err := ScheduleWith(JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{r}, ScheduleOpts{MaxAttempts: 1, MaxRecoveries: 1})
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
	if len(r.ids) != 2 {
		t.Fatalf("expected two isolated executions, got %d", len(r.ids))
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
	got, err := ScheduleWith(JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2}, []TaskRunner{r, b}, ScheduleOpts{MaxAttempts: 1, MaxRecoveries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || fmt.Sprint(got[0]) != "{a 4}" || fmt.Sprint(got[1]) != "{b 2}" {
		t.Fatal(got)
	}
}
