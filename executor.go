package spark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type Task struct {
	budget      *diskBudget
	jobCache    *memoryCache
	JobID       string
	Job         JobSpec
	StageID     int
	PartitionID int
	Attempt     int
	StoreDir    string
	Upstream    []MapOutputManifest
	Fingerprint string
	Sample      bool
	RangeBounds []byte
}

type ExecResult struct {
	PartitionID  int
	Kind         StageKind
	Manifest     *MapOutputManifest
	Records      []any
	Output       *PartitionOutput
	Cached       []CachePartition
	Dropped      []CachePartition
	Samples      []any
	Accumulators map[string]accumulatorValue
}

func ExecuteTask(task Task) (ExecResult, error) {
	return ExecuteTaskContext(context.Background(), task)
}

func ExecuteTaskContext(execution context.Context, task Task) (result ExecResult, taskErr error) {
	defer func() {
		if v := recover(); v != nil {
			if c, ok := v.(taskCanceled); ok {
				result = ExecResult{}
				taskErr = c.err
			} else if e, ok := v.(executionError); ok {
				result = ExecResult{}
				taskErr = e.err
			} else {
				panic(v)
			}
		}
	}()
	if err := execution.Err(); err != nil {
		return ExecResult{}, err
	}
	if task.StoreDir == "" {
		return ExecResult{}, fmt.Errorf("task StoreDir is required")
	}
	plan, err := PlanJob(task.Job)
	if err != nil {
		return ExecResult{}, err
	}
	if task.Job.ProtocolVersion != 0 && plan.Fingerprint != "" && task.Job.ImageDigest != "" {
		if err := VerifyPlan(plan, task.Job); err != nil {
			return ExecResult{}, err
		}
	}
	var stage *Stage
	for i := range plan.Stages {
		if plan.Stages[i].ID == task.StageID {
			stage = &plan.Stages[i]
			break
		}
	}
	if stage == nil {
		return ExecResult{}, fmt.Errorf("stage %d not in plan", task.StageID)
	}
	if task.PartitionID < 0 || task.PartitionID >= stage.NumPartitions {
		return ExecResult{}, fmt.Errorf("invalid partition %d for stage %d", task.PartitionID, stage.ID)
	}
	if task.Fingerprint != "" && task.Fingerprint != plan.Fingerprint {
		return ExecResult{}, fmt.Errorf("driver/worker graph fingerprint mismatch")
	}
	factory, ok := GetJob(task.Job.TaskName)
	if !ok {
		return ExecResult{}, fmt.Errorf("job %q not registered", task.Job.TaskName)
	}
	ctx := NewContext(&Config{
		AppName:       task.Job.TaskName,
		Master:        MasterLocal,
		NumPartitions: task.Job.NumPartitions,
	})
	defer ctx.Stop()
	ctx.prepareAccumulators(task.Job)
	ctx.distributedCache = task.jobCache
	if task.budget != nil {
		ctx.disk.clear()
		path, err := os.MkdirTemp(task.StoreDir, "scratch-")
		if err != nil {
			return ExecResult{}, err
		}
		ctx.disk.dir = path
		ctx.shuffleBudget = task.budget
		ctx.onClose(func() { _ = task.budget.removeDir(path) })
	}
	ctx.execution = execution
	ctx.installInputSplits(task.Job.InputSplits)
	rdd, err := factory(ctx, task.Job)
	if err != nil {
		return ExecResult{}, err
	}
	jobID := task.JobID
	if jobID == "" {
		jobID = task.Job.TaskName
	}
	store := NewDiskShuffleStore(task.StoreDir)
	store.budget = task.budget
	store.codec = cancelCodec{ctx: execution, RecordCodec: store.codec}
	actual, err := PlanRDD(rdd, task.Job)
	if err != nil || actual.Fingerprint != plan.Fingerprint {
		return ExecResult{}, fmt.Errorf("job factory changed graph during reconstruction")
	}
	for _, parentID := range stage.Parents {
		parent, _ := stageByID(plan, parentID)
		seen := make(map[int]bool)
		for _, m := range task.Upstream {
			if m.ShuffleID != parent.ShuffleID {
				continue
			}
			if m.JobID != jobID || m.MapID < 0 || m.MapID >= parent.NumPartitions || seen[m.MapID] {
				return ExecResult{}, fmt.Errorf("invalid or duplicate manifest for shuffle %d map %d", m.ShuffleID, m.MapID)
			}
			seen[m.MapID] = true
			buckets := make(map[int]bool)
			for _, b := range m.Buckets {
				if b.ReduceID < 0 || b.ReduceID >= parent.NumReducers || buckets[b.ReduceID] {
					return ExecResult{}, fmt.Errorf("invalid bucket manifest")
				}
				buckets[b.ReduceID] = true
			}
			if len(buckets) != parent.NumReducers {
				return ExecResult{}, fmt.Errorf("missing bucket manifest")
			}
		}
		if len(seen) != parent.NumPartitions {
			return ExecResult{}, fmt.Errorf("missing outputs for shuffle %d: got %d want %d", parent.ShuffleID, len(seen), parent.NumPartitions)
		}
	}
	stageRoot := rdd
	if stage.Kind == StageShuffleMap {
		dep := findShuffleDep(rdd, stage.ShuffleID)
		if dep == nil {
			return ExecResult{}, fmt.Errorf("shuffle %d not found", stage.ShuffleID)
		}
		stageRoot = dep.Parent()
	}
	if err := installUpstream(ctx, *stage, task, store, requiredShuffleBuckets(stageRoot, task.PartitionID)); err != nil {
		return ExecResult{}, err
	}
	if task.Sample {
		if stage.Kind != StageShuffleMap {
			return ExecResult{}, fmt.Errorf("sampling requires a shuffle-map stage")
		}
		dep := findShuffleDep(rdd, stage.ShuffleID)
		if dep == nil || dep.sortLess == nil {
			return ExecResult{}, fmt.Errorf("stage %d is not a range sort", stage.ID)
		}
		part, ok := partitionByIndex(dep.Parent(), task.PartitionID)
		if !ok {
			return ExecResult{}, fmt.Errorf("sampling partition %d not found", task.PartitionID)
		}
		samples, err := sampleSortPartition(ctx, dep, part)
		if err != nil {
			return ExecResult{}, err
		}
		return withCacheUpdates(ctx, ExecResult{PartitionID: task.PartitionID, Kind: StageSample, Samples: samples}), nil
	}
	switch stage.Kind {
	case StageShuffleMap:
		dep := findShuffleDep(rdd, stage.ShuffleID)
		if dep != nil && dep.sortLess != nil && dep.partitioner.NumPartitions() > 1 {
			if len(task.RangeBounds) == 0 {
				return ExecResult{}, fmt.Errorf("range sort task missing sampled boundaries")
			}
			bounds, err := decodeRecordSlice(task.RangeBounds)
			if err != nil {
				return ExecResult{}, err
			}
			dep.partitioner.(*RangePartitioner).SetRangeBounds(bounds)
		}
		man, err := executeShuffleMap(ctx, rdd, *stage, task, jobID, store)
		if err != nil {
			return ExecResult{}, err
		}
		return withCacheUpdates(ctx, ExecResult{PartitionID: task.PartitionID, Kind: StageShuffleMap, Manifest: &man}), nil
	case StageResult:
		if task.Job.Action == ActionSave {
			out, err := writeOutputPartition(execution, rdd, *stage, task)
			if err != nil {
				return ExecResult{}, err
			}
			return withCacheUpdates(ctx, ExecResult{PartitionID: task.PartitionID, Kind: StageResult, Output: &out}), nil
		}
		if action, ok := getTreeAction(task.Job.Action); ok {
			part, found := partitionByIndex(rdd, task.PartitionID)
			if !found {
				return ExecResult{}, fmt.Errorf("result partition %d not found", task.PartitionID)
			}
			partial, valid := action.partial(rdd.ComputeAny(part))
			res := ExecResult{PartitionID: task.PartitionID, Kind: StageResult}
			if valid {
				res.Records = []any{partial}
			}
			return withCacheUpdates(ctx, res), nil
		}
		recs, err := executeResult(ctx, rdd, *stage, task, store)
		if err != nil {
			return ExecResult{}, err
		}
		return withCacheUpdates(ctx, ExecResult{PartitionID: task.PartitionID, Kind: StageResult, Records: recs}), nil
	default:
		return ExecResult{}, fmt.Errorf("unknown stage kind %q", stage.Kind)
	}
}

func withCacheUpdates(ctx *Context, res ExecResult) ExecResult {
	res.Cached, res.Dropped = ctx.cacheUpdates()
	res.Accumulators = ctx.accumulatorUpdates()
	return res
}

func executeShuffleMap(_ *Context, root RDDAny, stage Stage, task Task, jobID string, store *DiskShuffleStore) (MapOutputManifest, error) {
	dep := findShuffleDep(root, stage.ShuffleID)
	if dep == nil {
		return MapOutputManifest{}, fmt.Errorf("shuffle %d not found", stage.ShuffleID)
	}
	parent := dep.Parent()
	part, ok := partitionByIndex(parent, task.PartitionID)
	if !ok {
		return MapOutputManifest{}, fmt.Errorf("partition not found")
	}
	manifest, err := writeStreamMap(root.Ctx(), store, dep, part, jobID, task.PartitionID, task.Attempt)
	if err == nil {
		if canceled := root.Ctx().TaskContext().Err(); canceled != nil {
			_ = os.RemoveAll(manifest.Location)
			return MapOutputManifest{}, canceled
		}
	}
	return manifest, err
}

func executeResult(ctx *Context, root RDDAny, stage Stage, task Task, store *DiskShuffleStore) ([]any, error) {
	if len(stage.Parents) > 0 && len(task.Upstream) == 0 {
		return nil, fmt.Errorf("result stage requires upstream map outputs")
	}
	task.Upstream = latestManifests(task.Upstream)
	part, ok := partitionByIndex(root, task.PartitionID)
	if !ok {
		return nil, nil
	}
	iter := root.ComputeAny(part)
	var recs []any
	for {
		v, more := iter()
		if !more {
			break
		}
		recs = append(recs, v)
	}
	return recs, nil
}

// requiredShuffleBuckets walks narrow dependencies from the task's partition
// until it reaches a shuffle boundary. At a boundary, the current partition
// selects the reducer bucket, except for single-bucket global shuffles.
func requiredShuffleBuckets(root RDDAny, partition int) map[int]map[int]bool {
	type visit struct{ rdd, partition int }
	seen := map[visit]bool{}
	want := map[int]map[int]bool{}
	var walk func(RDDAny, int)
	walk = func(rdd RDDAny, pid int) {
		v := visit{rdd.ID(), pid}
		if seen[v] {
			return
		}
		seen[v] = true
		for _, dep := range rdd.Dependencies() {
			if dep == nil || dep.Parent() == nil {
				continue
			}
			if dep.DepType() == DepShuffle {
				sh, ok := dep.(*ShuffleDep)
				if !ok || sh.GetPartitioner() == nil {
					continue
				}
				n := sh.GetPartitioner().NumPartitions()
				if want[sh.ShuffleID()] == nil {
					want[sh.ShuffleID()] = map[int]bool{}
				}
				if n == 1 {
					want[sh.ShuffleID()][0] = true
				} else if pid >= 0 && pid < n {
					want[sh.ShuffleID()][pid] = true
				} else {
					for rid := 0; rid < n; rid++ {
						want[sh.ShuffleID()][rid] = true
					}
				}
				continue
			}
			for _, parent := range dep.GetParents(pid) {
				walk(dep.Parent(), parent)
			}
		}
	}
	walk(root, partition)
	return want
}

func installUpstream(ctx *Context, stage Stage, task Task, store *DiskShuffleStore, required map[int]map[int]bool) error {
	if len(stage.Parents) == 0 {
		return nil
	}
	byShuffle := make(map[int][]MapOutputManifest)
	for _, m := range task.Upstream {
		byShuffle[m.ShuffleID] = append(byShuffle[m.ShuffleID], m)
	}
	for shuffleID, maps := range byShuffle {
		if len(maps) == 0 {
			return fmt.Errorf("missing map outputs for shuffle %d", shuffleID)
		}
		needed, mapped := required[shuffleID]
		manager, ok := ctx.ShuffleManager().(*fileShuffleManager)
		if !ok {
			return fmt.Errorf("task requires file-backed shuffle manager")
		}
		for _, m := range maps {
			paths := make(map[int]string)
			for _, bucket := range m.Buckets {
				if mapped && !needed[bucket.ReduceID] {
					continue
				}
				file, err := cacheShuffleBucket(ctx, store, m, bucket)
				if err != nil {
					return &FetchError{JobID: m.JobID, ShuffleID: m.ShuffleID, MapID: m.MapID, Attempt: m.Attempt, Reason: err.Error()}
				}
				paths[bucket.ReduceID] = file
			}
			manager.install(shuffleID, m.MapID, paths)
		}
	}
	return nil
}

func readUpstreamBucket(store *DiskShuffleStore, m MapOutputManifest, reduceID int) ([]any, error) {
	return readUpstreamBucketContext(context.Background(), store, m, reduceID)
}

func readUpstreamBucketContext(ctx context.Context, store *DiskShuffleStore, m MapOutputManifest, reduceID int) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.BaseURL != "" {
		return FetchShuffleBucketContext(ctx, m.BaseURL, m, reduceID, store.codec)
	}
	if m.Location != "" {
		path := filepath.Join(m.Location, bucketName(reduceID))
		if _, err := os.Stat(path); err == nil {
			return store.ReadBucket(m, reduceID)
		}
	}
	if m.BaseURL != "" {
		return FetchShuffleBucket(m.BaseURL, m, reduceID, DefaultCodec())
	}
	return store.ReadBucket(m, reduceID)
}

func partitionByIndex(rdd RDDAny, index int) (Partition, bool) {
	for _, p := range rdd.Partitions() {
		if p.Index() == index {
			return p, true
		}
	}
	return nil, false
}

func findShuffleDep(rdd RDDAny, shuffleID int) *ShuffleDep {
	seen := make(map[int]bool)
	stack := []RDDAny{rdd}
	for len(stack) > 0 {
		n := len(stack) - 1
		cur := stack[n]
		stack = stack[:n]
		if seen[cur.ID()] {
			continue
		}
		seen[cur.ID()] = true
		for _, dep := range cur.Dependencies() {
			if dep == nil || dep.Parent() == nil {
				continue
			}
			if sd, ok := dep.(*ShuffleDep); ok && sd.ShuffleID() == shuffleID {
				return sd
			}
			stack = append(stack, dep.Parent())
		}
	}
	return nil
}
