package spark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type Task struct {
	JobID       string
	Job         JobSpec
	StageID     int
	PartitionID int
	Attempt     int
	StoreDir    string
	Upstream    []MapOutputManifest
	Fingerprint string
}

type ExecResult struct {
	PartitionID int
	Kind        StageKind
	Manifest    *MapOutputManifest
	Records     []any
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
	ctx.execution = execution
	rdd, err := factory(ctx, task.Job)
	if err != nil {
		return ExecResult{}, err
	}
	jobID := task.JobID
	if jobID == "" {
		jobID = task.Job.TaskName
	}
	store := NewDiskShuffleStore(task.StoreDir)
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
	if err := installUpstream(ctx, *stage, task, store); err != nil {
		return ExecResult{}, err
	}
	switch stage.Kind {
	case StageShuffleMap:
		man, err := executeShuffleMap(ctx, rdd, *stage, task, jobID, store)
		if err != nil {
			return ExecResult{}, err
		}
		return ExecResult{PartitionID: task.PartitionID, Kind: StageShuffleMap, Manifest: &man}, nil
	case StageResult:
		recs, err := executeResult(ctx, rdd, *stage, task, store)
		if err != nil {
			return ExecResult{}, err
		}
		return ExecResult{PartitionID: task.PartitionID, Kind: StageResult, Records: recs}, nil
	default:
		return ExecResult{}, fmt.Errorf("unknown stage kind %q", stage.Kind)
	}
}

func executeShuffleMap(_ *Context, root RDDAny, stage Stage, task Task, jobID string, store *DiskShuffleStore) (MapOutputManifest, error) {
	dep := findShuffleDep(root, stage.ShuffleID)
	if dep == nil {
		return MapOutputManifest{}, fmt.Errorf("shuffle %d not found", stage.ShuffleID)
	}
	parent := dep.Parent()
	part, ok := partitionByIndex(parent, task.PartitionID)
	spills := make(map[int]*spillAcc, stage.NumReducers)
	for i := 0; i < stage.NumReducers; i++ {
		spills[i] = &spillAcc{}
	}
	spillDir := filepath.Join(task.StoreDir, "_spill", jobID, fmt.Sprintf("s%d-m%d-a%d", stage.ShuffleID, task.PartitionID, task.Attempt))
	defer os.RemoveAll(spillDir)
	codec := store.codec
	if ok {
		iter := parent.ComputeAny(part)
		partitioner := dep.GetPartitioner()
		for {
			item, more := iter()
			if !more {
				break
			}
			key := dep.ExtractKey(item)
			rid := 0
			if key != nil && partitioner != nil {
				rid = partitioner.GetPartition(key)
			}
			if err := spills[rid].add(item, spillDir, codec); err != nil {
				return MapOutputManifest{}, err
			}
		}
	}
	buckets := make(map[int][]any, stage.NumReducers)
	for rid, acc := range spills {
		recs, err := acc.collect(codec)
		if err != nil {
			return MapOutputManifest{}, err
		}
		buckets[rid] = combineMapOutput(recs, dep)
	}
	manifest, err := store.WriteMap(jobID, stage.ShuffleID, task.PartitionID, task.Attempt, stage.NumReducers, buckets)
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

func installUpstream(ctx *Context, stage Stage, task Task, store *DiskShuffleStore) error {
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
		ctx.ShuffleManager().RegisterShuffle(shuffleID)
		for _, m := range maps {
			buckets := make(map[int][]any)
			for _, bucket := range m.Buckets {
				recs, err := readUpstreamBucketContext(ctx.TaskContext(), store, m, bucket.ReduceID)
				if err != nil {
					return &FetchError{JobID: m.JobID, ShuffleID: m.ShuffleID, MapID: m.MapID, Attempt: m.Attempt, Reason: err.Error()}
				}
				buckets[bucket.ReduceID] = recs
			}
			ctx.ShuffleManager().WriteMapOutput(shuffleID, m.MapID, buckets)
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
