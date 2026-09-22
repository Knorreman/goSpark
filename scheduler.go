package spark

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type TaskRunner interface {
	Exec(task Task) (ExecResult, error)
}

type localRunner struct {
	storeDir string
}

func (r localRunner) Exec(task Task) (ExecResult, error) {
	task.StoreDir = r.storeDir
	return ExecuteTask(task)
}

func WaitForWorkers(urls []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for _, raw := range urls {
		u := strings.TrimRight(raw, "/") + "/health"
		for {
			resp, err := client.Get(u)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for worker %s: %v", u, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

type ScheduleOpts struct {
	MaxAttempts   int
	MaxRecoveries int
}

func Schedule(spec JobSpec, runners []TaskRunner) ([]any, error) {
	return ScheduleWith(spec, runners, ScheduleOpts{})
}

func ScheduleWith(spec JobSpec, runners []TaskRunner, opts ScheduleOpts) ([]any, error) {
	if opts.MaxRecoveries < 0 {
		return nil, fmt.Errorf("negative recovery limit")
	}
	if opts.MaxRecoveries == 0 {
		opts.MaxRecoveries = 2
	}
	var last error
	for generation := 0; generation <= opts.MaxRecoveries; generation++ {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return nil, err
		}
		result, err := scheduleGeneration(spec, runners, opts, fmt.Sprintf("job-%x", id))
		if err == nil {
			return result, nil
		}
		last = err
	}
	return nil, fmt.Errorf("recovery limit exhausted: %w", last)
}

func scheduleGeneration(spec JobSpec, runners []TaskRunner, opts ScheduleOpts, jobID string) ([]any, error) {
	if len(runners) == 0 {
		return nil, fmt.Errorf("no workers")
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	plan, err := PlanJob(spec)
	if err != nil {
		return nil, err
	}
	ordered := orderedStages(plan)
	stageOut := make(map[int][]MapOutputManifest)
	var result []any
	wi := 0
	for _, st := range ordered {
		var upstream []MapOutputManifest
		for _, pid := range st.Parents {
			upstream = append(upstream, stageOut[pid]...)
		}
		if st.Kind == StageShuffleMap {
			var maps []MapOutputManifest
			for p := 0; p < st.NumPartitions; p++ {
				res, err := execWithRetry(runners, &wi, opts.MaxAttempts, Task{
					Fingerprint: plan.Fingerprint,
					JobID:       jobID,
					Job:         spec,
					StageID:     st.ID,
					PartitionID: p,
					Upstream:    latestManifests(upstream),
				})
				if err != nil {
					return nil, fmt.Errorf("shuffle map stage %d partition %d: %w", st.ID, p, err)
				}
				if res.Manifest == nil {
					return nil, fmt.Errorf("shuffle map stage %d partition %d missing manifest", st.ID, p)
				}
				maps = append(maps, *res.Manifest)
			}
			stageOut[st.ID] = latestManifests(maps)
			continue
		}
		for p := 0; p < st.NumPartitions; p++ {
			res, err := execWithRetry(runners, &wi, opts.MaxAttempts, Task{
				Fingerprint: plan.Fingerprint,
				JobID:       jobID,
				Job:         spec,
				StageID:     st.ID,
				PartitionID: p,
				Upstream:    latestManifests(upstream),
			})
			if err != nil {
				return nil, fmt.Errorf("result stage %d partition %d: %w", st.ID, p, err)
			}
			result = append(result, res.Records...)
		}
	}
	return result, nil
}

func orderedStages(plan *JobPlan) []Stage {
	done := make(map[int]bool)
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
			if !ready {
				continue
			}
			out = append(out, s)
			done[s.ID] = true
			progress = true
		}
		if !progress {
			break
		}
	}
	return out
}

func execWithRetry(runners []TaskRunner, wi *int, maxAttempts int, task Task) (ExecResult, error) {
	var last error
	for a := 0; a < maxAttempts; a++ {
		task.Attempt = a
		r := pickLiveRunner(runners, wi)
		res, err := r.Exec(task)
		if err == nil {
			return res, nil
		}
		last = err
	}
	return ExecResult{}, last
}

func pickLiveRunner(runners []TaskRunner, wi *int) TaskRunner {
	n := len(runners)
	for i := 0; i < n; i++ {
		r := runners[*wi%n]
		*wi++
		if a, ok := r.(interface{ Alive() bool }); ok && !a.Alive() {
			continue
		}
		return r
	}
	r := runners[*wi%n]
	*wi++
	return r
}

func stageByID(plan *JobPlan, id int) (Stage, bool) {
	for _, s := range plan.Stages {
		if s.ID == id {
			return s, true
		}
	}
	return Stage{}, false
}

func manifestReachable(m MapOutputManifest) bool {
	_, err := readUpstreamBucket(NewDiskShuffleStore(""), m, 0)
	return err == nil
}

func refreshDeadMapOutputs(runners []TaskRunner, wi *int, maxAttempts int, spec JobSpec, jobID string, mapStage Stage, maps []MapOutputManifest, mapUpstream []MapOutputManifest) ([]MapOutputManifest, error) {
	out := make([]MapOutputManifest, 0, len(maps))
	for _, m := range latestManifests(maps) {
		if manifestReachable(m) {
			out = append(out, m)
			continue
		}
		res, err := execWithRetry(runners, wi, maxAttempts, Task{
			JobID:       jobID,
			Job:         spec,
			StageID:     mapStage.ID,
			PartitionID: m.MapID,
			Upstream:    mapUpstream,
		})
		if err != nil {
			return nil, fmt.Errorf("recompute map %d: %w", m.MapID, err)
		}
		if res.Manifest == nil {
			return nil, fmt.Errorf("recompute map %d missing manifest", m.MapID)
		}
		out = append(out, *res.Manifest)
	}
	return out, nil
}

func latestManifests(maps []MapOutputManifest) []MapOutputManifest {
	if len(maps) == 0 {
		return maps
	}
	type key struct{ shuffle, mapID int }
	best := make(map[key]MapOutputManifest, len(maps))
	order := make([]key, 0, len(maps))
	for _, m := range maps {
		k := key{m.ShuffleID, m.MapID}
		prev, ok := best[k]
		if !ok {
			order = append(order, k)
			best[k] = m
			continue
		}
		if m.Attempt > prev.Attempt {
			best[k] = m
		}
	}
	out := make([]MapOutputManifest, 0, len(best))
	for _, k := range order {
		out = append(out, best[k])
	}
	return out
}
