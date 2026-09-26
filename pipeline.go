package spark

import (
	"context"
	"fmt"
)

type pipelineReducer struct {
	partial RDDAny
	merge   func([]any) (any, error)
}

type pipelineState struct {
	spec     JobSpec
	reducers []pipelineReducer
	values   map[int]any
}

// PipelineValue is the result of an action evaluated before the final RDD.
// Value is available inside transformations when that action has completed.
type PipelineValue[T any] struct {
	state *pipelineState
	index int
}

func (v PipelineValue[T]) Value() T {
	if v.state != nil {
		if raw, ok := v.state.values[v.index]; ok {
			return raw.(T)
		}
	}
	panic(executionError{err: fmt.Errorf("pipeline reduction %d has no broadcast value", v.index)})
}

func pipelineBroadcastName(index int) string {
	return fmt.Sprintf("_gospark_pipeline_%d", index)
}

// ReduceBroadcast declares a reduction dependency. In a pipeline, workers
// return one partial per partition and the driver broadcasts the combined
// result before executing the final RDD. The factory never runs this action.
func ReduceBroadcast[T any](rdd *RDD[T], fn func(T, T) T) PipelineValue[T] {
	state := rdd.ctx.pipeline
	if state == nil {
		panic("ReduceBroadcast requires a registered pipeline")
	}
	index := len(state.reducers)
	partial := MapPartitions(rdd, func(iter Iterator[T]) Iterator[T] {
		value, ok := ReduceIterator(iter, fn)
		if !ok {
			return EmptyIterator[T]()
		}
		return SliceIterator([]T{value})
	})
	state.reducers = append(state.reducers, pipelineReducer{
		partial: partial,
		merge: func(rows []any) (any, error) {
			if len(rows) == 0 {
				return nil, fmt.Errorf("cannot reduce an empty RDD")
			}
			acc, ok := rows[0].(T)
			if !ok {
				return nil, fmt.Errorf("pipeline reduction %d returned %T", index, rows[0])
			}
			for _, row := range rows[1:] {
				value, ok := row.(T)
				if !ok {
					return nil, fmt.Errorf("pipeline reduction %d returned %T", index, row)
				}
				acc = fn(acc, value)
			}
			return acc, nil
		},
	})
	if state.spec.PipelinePhase == "final" || state.spec.PipelinePhase == "reduce" {
		if value, err := ReadBroadcast[T](state.spec, pipelineBroadcastName(index)); err == nil {
			state.values[index] = value
		}
	}
	return PipelineValue[T]{state: state, index: index}
}

// RegisterPipeline registers one RDD graph for all phases of a distributed
// pipeline. It must run in both the driver and worker binaries.
func RegisterPipeline[T any](name string, build func(*Context, JobSpec) (*RDD[T], error)) {
	RegisterJob(name, func(ctx *Context, spec JobSpec) (RDDAny, error) {
		if spec.PipelinePhase != "final" && spec.PipelinePhase != "reduce" {
			return nil, fmt.Errorf("pipeline %q must be submitted with RunPipeline", name)
		}
		ctx.pipeline = &pipelineState{spec: spec, values: map[int]any{}}
		output, err := build(ctx, spec)
		if err != nil {
			return nil, err
		}
		if output == nil {
			return nil, fmt.Errorf("pipeline %q returned a nil RDD", name)
		}
		if spec.PipelinePhase == "reduce" {
			if spec.PipelineNode < 0 || spec.PipelineNode >= len(ctx.pipeline.reducers) {
				return nil, fmt.Errorf("pipeline reduction %d does not exist", spec.PipelineNode)
			}
			return ctx.pipeline.reducers[spec.PipelineNode].partial, nil
		}
		return output, nil
	})
}

// RunPipeline evaluates reductions, broadcasts their values, then collects
// the final RDD. The worker executable must import the same registered graph.
func RunPipeline[T any](spec JobSpec, workers []TaskRunner) ([]T, error) {
	return RunPipelineContext[T](context.Background(), spec, workers, ScheduleOpts{})
}

func RunPipelineContext[T any](ctx context.Context, spec JobSpec, workers []TaskRunner, opts ScheduleOpts) ([]T, error) {
	if spec.PipelinePhase != "" || spec.Action != "" || spec.TaskName == "" {
		return nil, fmt.Errorf("pipeline requires a task name and no phase or action")
	}
	if spec.NumPartitions <= 0 {
		spec.NumPartitions = 2
	}
	for _, broadcast := range spec.Broadcasts {
		if len(broadcast.Name) >= len("_gospark_pipeline_") && broadcast.Name[:len("_gospark_pipeline_")] == "_gospark_pipeline_" {
			return nil, fmt.Errorf("pipeline broadcast names beginning with _gospark_pipeline_ are reserved")
		}
	}
	spec.PipelinePhase = "final"
	spec.Action = ActionCollect
	plan, err := PlanJob(spec)
	if err != nil {
		return nil, err
	}
	spec.InputSplits = plan.InputSplits
	build, _ := GetJob(spec.TaskName)
	driver := NewContext(&Config{AppName: spec.TaskName, Master: MasterLocal, NumPartitions: spec.NumPartitions})
	defer driver.Stop()
	driver.installInputSplits(spec.InputSplits)
	if _, err := build(driver, spec); err != nil {
		return nil, err
	}
	for index, reduction := range driver.pipeline.reducers {
		phase := spec
		phase.PipelinePhase = "reduce"
		phase.PipelineNode = index
		phase.Broadcasts = append([]Broadcast(nil), spec.Broadcasts...)
		rows, err := ScheduleContext(ctx, phase, workers, opts)
		if err != nil {
			return nil, err
		}
		value, err := reduction.merge(rows)
		if err != nil {
			return nil, err
		}
		driver.pipeline.values[index] = value
		if err := AddBroadcast(&spec, pipelineBroadcastName(index), value); err != nil {
			return nil, err
		}
	}
	rows, err := ScheduleContext(ctx, spec, workers, opts)
	if err != nil {
		return nil, err
	}
	values := make([]T, len(rows))
	for i, row := range rows {
		value, ok := row.(T)
		if !ok {
			return nil, fmt.Errorf("pipeline result %d has type %T", i, row)
		}
		values[i] = value
	}
	return values, nil
}

// RunPipelineLocal executes the same graph in one process without RPCs.
func RunPipelineLocal[T any](spec JobSpec) ([]T, error) {
	if spec.PipelinePhase != "" || spec.Action != "" || spec.TaskName == "" {
		return nil, fmt.Errorf("pipeline requires a task name and no phase or action")
	}
	if spec.NumPartitions <= 0 {
		spec.NumPartitions = 2
	}
	spec.PipelinePhase = "final"
	spec.Action = ActionCollect
	build, ok := GetJob(spec.TaskName)
	if !ok {
		return nil, fmt.Errorf("pipeline %q not registered", spec.TaskName)
	}
	ctx := NewContext(&Config{AppName: spec.TaskName, Master: MasterLocal, NumPartitions: spec.NumPartitions})
	defer ctx.Stop()
	root, err := build(ctx, spec)
	if err != nil {
		return nil, err
	}
	for index, reduction := range ctx.pipeline.reducers {
		rows := collectAny(reduction.partial)
		value, err := reduction.merge(rows)
		if err != nil {
			return nil, err
		}
		ctx.pipeline.values[index] = value
	}
	rows := collectAny(root)
	values := make([]T, len(rows))
	for i, row := range rows {
		value, ok := row.(T)
		if !ok {
			return nil, fmt.Errorf("pipeline result %d has type %T", i, row)
		}
		values[i] = value
	}
	return values, nil
}

func collectAny(rdd RDDAny) []any {
	computeShuffleStages(rdd)
	var rows []any
	for _, part := range rdd.Partitions() {
		iter := rdd.ComputeAny(part)
		for value, ok := iter(); ok; value, ok = iter() {
			rows = append(rows, value)
		}
	}
	return rows
}
