package spark

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type TaskRunner interface {
	Exec(Task) (ExecResult, error)
}
type ContextTaskRunner interface {
	ExecContext(context.Context, Task) (ExecResult, error)
}
type localRunner struct{ storeDir string }

func (r localRunner) Exec(task Task) (ExecResult, error) {
	return r.ExecContext(context.Background(), task)
}
func (r localRunner) ExecContext(ctx context.Context, task Task) (ExecResult, error) {
	task.StoreDir = r.storeDir
	return ExecuteTaskContext(ctx, task)
}

func WaitForWorkers(urls []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, raw := range urls {
		for {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(raw, "/")+"/health", nil)
			if err != nil {
				return err
			}
			resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for %s: %w", raw, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}

type ScheduleOpts struct {
	MaxAttempts int
	// MaxRecoveries bounds lost-map repairs across the job (zero defaults to 2).
	MaxRecoveries int
	// OnTaskComplete runs synchronously after accepting a result. Useful for
	// observability and deterministic fault-injection tests. Do not mutate it.
	OnTaskComplete func(Task, ExecResult) error
	OnRepair       func(FetchError)
}

func Schedule(spec JobSpec, runners []TaskRunner) ([]any, error) {
	return ScheduleWith(spec, runners, ScheduleOpts{})
}
func ScheduleWith(spec JobSpec, runners []TaskRunner, opts ScheduleOpts) ([]any, error) {
	return ScheduleContext(context.Background(), spec, runners, opts)
}

func ScheduleContext(ctx context.Context, spec JobSpec, runners []TaskRunner, opts ScheduleOpts) ([]any, error) {
	if spec.Action == ActionSave {
		return nil, fmt.Errorf("use ScheduleSaveContext for distributed save")
	}
	results, _, _, err := scheduleResults(ctx, spec, runners, opts)
	if err != nil {
		return nil, err
	}
	var records []any
	for _, r := range results {
		records = append(records, r.Records...)
	}
	return records, nil
}

func ScheduleSave(spec JobSpec, runners []TaskRunner) (OutputManifest, error) {
	return ScheduleSaveContext(context.Background(), spec, runners, ScheduleOpts{})
}

// ScheduleSaveContext runs the same stage/recovery scheduler as Collect but
// publishes one _SUCCESS manifest only after all selected attempts are verified.
func ScheduleSaveContext(ctx context.Context, spec JobSpec, runners []TaskRunner, opts ScheduleOpts) (OutputManifest, error) {
	if spec.Action != ActionSave || spec.Params["path"] == "" {
		return OutputManifest{}, fmt.Errorf("save requires action=save and params.path")
	}
	if _, err := ReadCommittedOutput(spec.Params["path"]); err == nil {
		return OutputManifest{}, ErrOutputCommitted
	}
	results, jobID, stageID, err := scheduleResults(ctx, spec, runners, opts)
	if err != nil {
		return OutputManifest{}, err
	}
	selected, err := SelectedOutputs(results, len(results))
	if err != nil {
		return OutputManifest{}, err
	}
	if err := ctx.Err(); err != nil {
		return OutputManifest{}, err
	}
	return CommitDistributedOutput(spec.Params["path"], jobID, spec.TaskName, stageID, len(results), selected)
}

func scheduleResults(ctx context.Context, spec JobSpec, runners []TaskRunner, opts ScheduleOpts) ([]ExecResult, string, int, error) {
	if len(runners) == 0 {
		return nil, "", 0, fmt.Errorf("no workers")
	}
	if opts.MaxAttempts < 0 || opts.MaxRecoveries < 0 {
		return nil, "", 0, fmt.Errorf("negative retry limit")
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	if opts.MaxRecoveries == 0 {
		opts.MaxRecoveries = 2
	}
	plan, err := PlanJob(spec)
	if err != nil {
		return nil, "", 0, err
	}
	if len(orderedStages(plan)) != len(plan.Stages) {
		return nil, "", 0, fmt.Errorf("invalid stage DAG")
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return nil, "", 0, err
	}
	s := &lineageScheduler{ctx: ctx, spec: spec, plan: plan, runners: runners, opts: opts, jobID: fmt.Sprintf("job-%x", id), accepted: map[taskKey]ExecResult{}, attempts: map[taskKey]int{}}
	result, _ := stageByID(plan, plan.ResultStageID)
	// A repair invalidates affected descendants, including previously accepted
	// result partitions. Revisit them before returning any records to the caller.
	for {
		for p := 0; p < result.NumPartitions; p++ {
			if _, err = s.ensure(result, p); err != nil {
				return nil, "", 0, err
			}
		}
		var results []ExecResult
		complete := true
		for p := 0; p < result.NumPartitions; p++ {
			r, ok := s.accepted[taskKey{result.ID, p}]
			if !ok {
				complete = false
				break
			}
			results = append(results, r)
		}
		if complete {
			return results, s.jobID, result.ID, nil
		}
	}
}

type taskKey struct{ stage, part int }
type lineageScheduler struct {
	ctx             context.Context
	spec            JobSpec
	plan            *JobPlan
	runners         []TaskRunner
	opts            ScheduleOpts
	jobID           string
	worker, repairs int
	accepted        map[taskKey]ExecResult
	attempts        map[taskKey]int
}

func (s *lineageScheduler) ensure(stage Stage, partition int) (ExecResult, error) {
	k := taskKey{stage.ID, partition}
	if r, ok := s.accepted[k]; ok {
		return r, nil
	}
	failures := 0
	for {
		if err := s.ctx.Err(); err != nil {
			return ExecResult{}, err
		}
		var upstream []MapOutputManifest
		beforeRepair := s.repairs
		for _, id := range stage.Parents {
			parent, ok := stageByID(s.plan, id)
			if !ok {
				return ExecResult{}, fmt.Errorf("missing parent stage %d", id)
			}
			for p := 0; p < parent.NumPartitions; p++ {
				r, err := s.ensure(parent, p)
				if err != nil {
					return ExecResult{}, err
				}
				upstream = append(upstream, *r.Manifest)
			}
		}
		if s.repairs != beforeRepair {
			continue
		} // Rebuild a consistent parent snapshot.
		task := Task{JobID: s.jobID, Job: s.spec, StageID: stage.ID, PartitionID: partition, Attempt: s.attempts[k], Upstream: upstream, Fingerprint: s.plan.Fingerprint}
		s.attempts[k]++ // Never reuse paths, including after selective recomputation.
		runner := s.pickWorker()
		var res ExecResult
		var err error
		if runner == nil {
			return res, fmt.Errorf("no healthy workers")
		}
		if r, ok := runner.(ContextTaskRunner); ok {
			res, err = r.ExecContext(s.ctx, task)
		} else {
			res, err = runner.Exec(task)
		}
		if s.ctx.Err() != nil {
			return ExecResult{}, s.ctx.Err()
		}
		if err == nil {
			err = validateTaskResult(task, stage, res)
		}
		if err == nil {
			s.accepted[k] = res
			if s.opts.OnTaskComplete != nil {
				if err = s.opts.OnTaskComplete(task, res); err != nil {
					return ExecResult{}, err
				}
			}
			return res, nil
		}
		var fetch *FetchError
		if errors.As(err, &fetch) {
			if err = s.invalidate(fetch, stage); err != nil {
				return ExecResult{}, err
			}
			continue
		}
		failures++
		if failures >= s.opts.MaxAttempts {
			return ExecResult{}, fmt.Errorf("stage %d partition %d retries exhausted: %w", stage.ID, partition, err)
		}
	}
}

func validateTaskResult(task Task, stage Stage, res ExecResult) error {
	if res.Kind != stage.Kind || res.PartitionID != task.PartitionID {
		return fmt.Errorf("mismatched task result")
	}
	if stage.Kind == StageShuffleMap {
		m := res.Manifest
		if m == nil || m.JobID != task.JobID || m.ShuffleID != stage.ShuffleID || m.MapID != task.PartitionID || m.Attempt != task.Attempt {
			return fmt.Errorf("stale or mismatched map attempt")
		}
	}
	if stage.Kind == StageResult && task.Job.Action == ActionSave {
		out := res.Output
		if out == nil || out.JobID != task.JobID || out.StageID != task.StageID || out.PartitionID != task.PartitionID || out.Attempt != task.Attempt {
			return fmt.Errorf("stale or mismatched save attempt")
		}
	}
	return nil
}

func (s *lineageScheduler) pickWorker() TaskRunner {
	for i := 0; i < len(s.runners); i++ {
		r := s.runners[s.worker%len(s.runners)]
		s.worker++
		if h, ok := r.(interface{ AliveContext(context.Context) bool }); ok {
			if !h.AliveContext(s.ctx) {
				continue
			}
		} else if h, ok := r.(interface{ Alive() bool }); ok && !h.Alive() {
			continue
		}
		return r
	}
	return nil
}

func (s *lineageScheduler) invalidate(e *FetchError, consumer Stage) error {
	if e.JobID != s.jobID {
		return fmt.Errorf("foreign shuffle failure")
	}
	var source Stage
	found := false
	for _, id := range consumer.Parents {
		p, _ := stageByID(s.plan, id)
		if p.ShuffleID == e.ShuffleID {
			source = p
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("unexpected shuffle failure %d", e.ShuffleID)
	}
	k := taskKey{source.ID, e.MapID}
	r, ok := s.accepted[k]
	if !ok || r.Manifest == nil || r.Manifest.Attempt != e.Attempt {
		return fmt.Errorf("stale shuffle failure")
	}
	if s.repairs >= s.opts.MaxRecoveries {
		return fmt.Errorf("recovery limit exhausted: %w", e)
	}
	s.repairs++
	delete(s.accepted, k)
	if s.opts.OnRepair != nil {
		s.opts.OnRepair(*e)
	}
	// Narrow partition mappings are not yet encoded in the stage plan. Conservatively
	// invalidate all descendant partitions, while retaining sibling maps and branches.
	affected := map[int]bool{source.ID: true}
	for _, st := range orderedStages(s.plan) {
		if st.ID == source.ID {
			continue
		}
		for _, parent := range st.Parents {
			if affected[parent] {
				affected[st.ID] = true
				for p := 0; p < st.NumPartitions; p++ {
					delete(s.accepted, taskKey{st.ID, p})
				}
				break
			}
		}
	}
	return nil
}

func orderedStages(plan *JobPlan) []Stage {
	done := map[int]bool{}
	var out []Stage
	for len(out) < len(plan.Stages) {
		progress := false
		for _, s := range plan.Stages {
			if done[s.ID] {
				continue
			}
			ready := true
			for _, p := range s.Parents {
				if !done[p] {
					ready = false
					break
				}
			}
			if ready {
				out = append(out, s)
				done[s.ID] = true
				progress = true
			}
		}
		if !progress {
			break
		}
	}
	return out
}
func stageByID(plan *JobPlan, id int) (Stage, bool) {
	for _, s := range plan.Stages {
		if s.ID == id {
			return s, true
		}
	}
	return Stage{}, false
}
func latestManifests(maps []MapOutputManifest) []MapOutputManifest {
	type key struct{ shuffle, mapID int }
	best := map[key]MapOutputManifest{}
	var order []key
	for _, m := range maps {
		k := key{m.ShuffleID, m.MapID}
		old, ok := best[k]
		if !ok {
			order = append(order, k)
		}
		if !ok || m.Attempt > old.Attempt {
			best[k] = m
		}
	}
	out := make([]MapOutputManifest, 0, len(order))
	for _, k := range order {
		out = append(out, best[k])
	}
	return out
}
