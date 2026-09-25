package spark

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

type StageKind string

const (
	StageResult     StageKind = "result"
	StageShuffleMap StageKind = "shuffle_map"
)

type Stage struct {
	ID            int                `json:"id"`
	Kind          StageKind          `json:"kind"`
	RDDID         int                `json:"rdd_id"`
	RDDName       string             `json:"rdd_name,omitempty"`
	NumPartitions int                `json:"num_partitions"`
	ShuffleID     int                `json:"shuffle_id,omitempty"`
	NumReducers   int                `json:"num_reducers,omitempty"`
	Parents       []int              `json:"parents"`
	CacheHints    [][]CachePartition `json:"cache_hints,omitempty"`
}

type JobPlan struct {
	Spec          JobSpec `json:"spec"`
	Fingerprint   string  `json:"fingerprint"`
	Stages        []Stage `json:"stages"`
	ResultStageID int     `json:"result_stage_id"`
}

func PlanJob(spec JobSpec) (*JobPlan, error) {
	ensureBuiltinActions()
	spec = spec.withDefaults()
	if spec.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version %d (want %d)", spec.ProtocolVersion, ProtocolVersion)
	}
	if spec.TaskName == "" {
		return nil, fmt.Errorf("job spec missing task name")
	}
	factory, ok := GetJob(spec.TaskName)
	if !ok {
		return nil, fmt.Errorf("job %q not registered", spec.TaskName)
	}
	if spec.Action != "" {
		if _, ok := GetAction(spec.Action); !ok {
			return nil, fmt.Errorf("action %q not registered", spec.Action)
		}
	}
	ctx := NewContext(&Config{
		AppName:       spec.TaskName,
		Master:        MasterLocal,
		NumPartitions: spec.NumPartitions,
	})
	defer ctx.Stop()
	rdd, err := factory(ctx, spec)
	if err != nil {
		return nil, err
	}
	if rdd == nil {
		return nil, fmt.Errorf("job %q returned nil RDD", spec.TaskName)
	}
	return PlanRDD(rdd, spec)
}

func PlanRDD(rdd RDDAny, spec JobSpec) (*JobPlan, error) {
	spec = spec.withDefaults()
	if rdd == nil {
		return nil, fmt.Errorf("cannot plan nil RDD")
	}
	p := &planner{
		shuffleStages: make(map[int]int),
		stages:        make([]Stage, 0),
	}
	parents := p.parentStages(rdd)
	resultID := p.nextID
	p.nextID++
	result := Stage{
		ID:            resultID,
		Kind:          StageResult,
		RDDID:         rdd.ID(),
		RDDName:       rdd.Name(),
		NumPartitions: len(rdd.Partitions()),
		Parents:       uniqueInts(parents),
	}
	p.stages = append(p.stages, result)
	for i := range p.stages {
		stage := &p.stages[i]
		root := rdd
		if stage.Kind == StageShuffleMap {
			dep := findShuffleDep(rdd, stage.ShuffleID)
			if dep == nil {
				return nil, fmt.Errorf("shuffle %d not found", stage.ShuffleID)
			}
			root = dep.Parent()
		}
		if !containsShareableCache(root) {
			continue
		}
		var hints [][]CachePartition
		for part := 0; part < stage.NumPartitions; part++ {
			needed := cacheHints(root, part)
			if len(needed) == 0 && hints == nil {
				continue
			}
			if hints == nil {
				hints = make([][]CachePartition, stage.NumPartitions)
			}
			hints[part] = needed
		}
		stage.CacheHints = hints
	}
	plan := &JobPlan{
		Spec:          spec,
		Stages:        p.stages,
		ResultStageID: resultID,
	}
	plan.Fingerprint = fingerprintPlan(plan)
	return plan, nil
}

func containsShareableCache(root RDDAny) bool {
	seen := map[int]bool{}
	var walk func(RDDAny) bool
	walk = func(rdd RDDAny) bool {
		if seen[rdd.ID()] {
			return false
		}
		seen[rdd.ID()] = true
		if cached, ok := rdd.(interface{ cachedStorage() StorageLevel }); ok {
			level := cached.cachedStorage()
			if (level == StorageMemory || level == StorageMemoryAndDisk) && cacheAcrossTasks(rdd) {
				return true
			}
		}
		for _, dep := range rdd.Dependencies() {
			if dep != nil && dep.DepType() == DepNarrow && dep.Parent() != nil && walk(dep.Parent()) {
				return true
			}
		}
		return false
	}
	return walk(root)
}

// cacheHints follows only narrow dependencies: a cached ancestor behind a
// shuffle boundary is not consumed by this task's partition.
func cacheHints(root RDDAny, partition int) []CachePartition {
	type visit struct{ rdd, partition int }
	seen := map[visit]bool{}
	var hints []CachePartition
	var walk func(RDDAny, int)
	walk = func(rdd RDDAny, pid int) {
		key := visit{rdd.ID(), pid}
		if seen[key] {
			return
		}
		seen[key] = true
		if cached, ok := rdd.(interface{ cachedStorage() StorageLevel }); ok {
			level := cached.cachedStorage()
			if (level == StorageMemory || level == StorageMemoryAndDisk) && cacheAcrossTasks(rdd) {
				hints = append(hints, CachePartition{RDDID: rdd.ID(), PartitionID: pid})
			}
		}
		for _, dep := range rdd.Dependencies() {
			if dep != nil && dep.DepType() == DepNarrow && dep.Parent() != nil {
				for _, parent := range dep.GetParents(pid) {
					walk(dep.Parent(), parent)
				}
			}
		}
	}
	walk(root, partition)
	sort.Slice(hints, func(i, j int) bool {
		if hints[i].RDDID != hints[j].RDDID {
			return hints[i].RDDID < hints[j].RDDID
		}
		return hints[i].PartitionID < hints[j].PartitionID
	})
	return hints
}

// Shuffle-dependent cached values need lineage-version invalidation before
// they can safely outlive one task. Narrow-only cached input is stable across
// retries and can be shared across tasks on the same worker.
func cacheAcrossTasks(root RDDAny) bool {
	seen := map[int]bool{}
	var walk func(RDDAny) bool
	walk = func(rdd RDDAny) bool {
		if seen[rdd.ID()] {
			return true
		}
		seen[rdd.ID()] = true
		for _, dep := range rdd.Dependencies() {
			if dep == nil || dep.Parent() == nil {
				continue
			}
			if dep.DepType() == DepShuffle || !walk(dep.Parent()) {
				return false
			}
		}
		return true
	}
	return walk(root)
}

func VerifyPlan(expected *JobPlan, spec JobSpec) error {
	got, err := PlanJob(spec)
	if err != nil {
		return err
	}
	if expected == nil {
		return fmt.Errorf("expected plan is nil")
	}
	if got.Fingerprint != expected.Fingerprint {
		return fmt.Errorf("graph fingerprint mismatch: got %s want %s", got.Fingerprint, expected.Fingerprint)
	}
	return nil
}

type planner struct {
	shuffleStages map[int]int
	stages        []Stage
	nextID        int
}

func (p *planner) parentStages(rdd RDDAny) []int {
	var ids []int
	seen := make(map[int]bool)
	stack := []RDDAny{rdd}
	started := false
	for len(stack) > 0 {
		n := len(stack) - 1
		cur := stack[n]
		stack = stack[:n]
		if started {
			if seen[cur.ID()] {
				continue
			}
			seen[cur.ID()] = true
		} else {
			started = true
		}
		deps := cur.Dependencies()
		for _, dep := range deps {
			if dep == nil || dep.Parent() == nil {
				continue
			}
			if dep.DepType() == DepShuffle {
				sd, ok := dep.(*ShuffleDep)
				if !ok {
					continue
				}
				ids = append(ids, p.getShuffleMapStage(sd))
				continue
			}
			stack = append(stack, dep.Parent())
		}
	}
	return ids
}

func (p *planner) getShuffleMapStage(dep *ShuffleDep) int {
	if id, ok := p.shuffleStages[dep.ShuffleID()]; ok {
		return id
	}
	parent := dep.Parent()
	parentIDs := p.parentStages(parent)
	id := p.nextID
	p.nextID++
	numReducers := 0
	if part := dep.GetPartitioner(); part != nil {
		numReducers = part.NumPartitions()
	}
	st := Stage{
		ID:            id,
		Kind:          StageShuffleMap,
		RDDID:         parent.ID(),
		RDDName:       parent.Name(),
		NumPartitions: len(parent.Partitions()),
		ShuffleID:     dep.ShuffleID(),
		NumReducers:   numReducers,
		Parents:       uniqueInts(parentIDs),
	}
	p.stages = append(p.stages, st)
	p.shuffleStages[dep.ShuffleID()] = id
	return id
}

func uniqueInts(ids []int) []int {
	if len(ids) == 0 {
		return []int{}
	}
	seen := make(map[int]bool, len(ids))
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func fingerprintPlan(plan *JobPlan) string {
	h := sha256.New()
	spec := plan.Spec
	fmt.Fprintf(h, "proto=%d\n", spec.ProtocolVersion)
	fmt.Fprintf(h, "task=%s\n", spec.TaskName)
	fmt.Fprintf(h, "action=%s\n", spec.Action)
	fmt.Fprintf(h, "np=%d\n", spec.NumPartitions)
	fmt.Fprintf(h, "image=%s\n", spec.ImageDigest)
	if spec.Params != nil {
		keys := make([]string, 0, len(spec.Params))
		for k := range spec.Params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "param.%s=%s\n", k, spec.Params[k])
		}
	}
	fmt.Fprintf(h, "result=%d\n", plan.ResultStageID)
	stages := append([]Stage(nil), plan.Stages...)
	sort.Slice(stages, func(i, j int) bool { return stages[i].ID < stages[j].ID })
	for _, s := range stages {
		fmt.Fprintf(h, "stage=%d,%s,rdd=%d,parts=%d,shuf=%d,red=%d,parents=%s,name=%s\n",
			s.ID, s.Kind, s.RDDID, s.NumPartitions, s.ShuffleID, s.NumReducers, joinInts(s.Parents), s.RDDName)
		fmt.Fprintf(h, "cached=%v\n", s.CacheHints)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func joinInts(ids []int) string {
	if len(ids) == 0 {
		return ""
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}
