package spark

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func init() {
	RegisterPipeline("pipeline-normalized", func(ctx *Context, spec JobSpec) (*RDD[float64], error) {
		rdd := Parallelize(ctx, []float64{2, 3, 5}, spec.NumPartitions)
		total := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		return Map(rdd, func(x float64) float64 { return x / total.Value() }), nil
	})
	RegisterPipeline("pipeline-two-reductions", func(ctx *Context, spec JobSpec) (*RDD[float64], error) {
		rdd := Parallelize(ctx, []float64{2, 3, 5}, spec.NumPartitions)
		total := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		largest := ReduceBroadcast(rdd, math.Max)
		return Map(rdd, func(x float64) float64 { return x/total.Value() + largest.Value() }), nil
	})
	RegisterPipeline("pipeline-empty", func(ctx *Context, spec JobSpec) (*RDD[float64], error) {
		rdd := Parallelize(ctx, []float64{}, spec.NumPartitions)
		total := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		return Map(rdd, func(x float64) float64 { return x / total.Value() }), nil
	})
	RegisterPipeline("pipeline-dependent-reductions", func(ctx *Context, spec JobSpec) (*RDD[float64], error) {
		rdd := Parallelize(ctx, []float64{2, 3, 5}, spec.NumPartitions)
		total := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		fractions := Map(rdd, func(x float64) float64 { return x / total.Value() })
		fractionSum := ReduceBroadcast(fractions, func(a, b float64) float64 { return a + b })
		return Map(fractions, func(x float64) float64 { return x + fractionSum.Value() }), nil
	})
	RegisterPipeline("pipeline-text-input", func(ctx *Context, spec JobSpec) (*RDD[float64], error) {
		text := TextFile(ctx, spec.Params["path"], spec.NumPartitions)
		rdd := Map(text, func(line string) float64 {
			n, _ := strconv.ParseFloat(line, 64)
			return n
		})
		total := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		return Map(rdd, func(x float64) float64 { return x / total.Value() }), nil
	})
}

func assertPipelineValues(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, value := range got {
		if math.Abs(value-want[i]) > 1e-12 {
			t.Fatalf("item %d: got %g want %g", i, value, want[i])
		}
	}
}

func TestPipelineLocalReducesBeforeMap(t *testing.T) {
	values, err := RunPipelineLocal[float64](JobSpec{TaskName: "pipeline-normalized", NumPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{0.2, 0.3, 0.5})
	values, err = RunPipelineLocal[float64](JobSpec{TaskName: "pipeline-two-reductions", NumPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{5.2, 5.3, 5.5})
}

func TestPipelineTwoWorkerProcesses(t *testing.T) {
	values, err := RunPipeline[float64](JobSpec{TaskName: "pipeline-normalized", NumPartitions: 2},
		[]TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{0.2, 0.3, 0.5})
}

func TestPipelineSaveAcrossWorkerProcesses(t *testing.T) {
	out := filepath.Join(t.TempDir(), "normalized")
	manifest, err := RunPipelineSave(JobSpec{TaskName: "pipeline-normalized", NumPartitions: 2},
		[]TaskRunner{startTestWorker(t), startTestWorker(t)}, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 2 {
		t.Fatalf("expected two committed partitions: %+v", manifest)
	}
	var values []float64
	for _, part := range manifest.Partitions {
		data, err := os.ReadFile(part.Key)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Fields(string(data)) {
			value, err := strconv.ParseFloat(line, 64)
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, value)
		}
	}
	assertPipelineValues(t, values, []float64{0.2, 0.3, 0.5})
	if _, err := ReadCommittedOutput(out); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineDependentReductions(t *testing.T) {
	values, err := RunPipeline[float64](JobSpec{TaskName: "pipeline-dependent-reductions", NumPartitions: 2},
		[]TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{1.2, 1.3, 1.5})
}

func TestPipelineReusesDriverInputSplits(t *testing.T) {
	dir := t.TempDir()
	for name, data := range map[string]string{"a.txt": "2\n", "b.txt": "3\n5\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	values, err := RunPipeline[float64](JobSpec{TaskName: "pipeline-text-input", NumPartitions: 2,
		Params: map[string]string{"path": dir}}, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{0.2, 0.3, 0.5})
}

type failPipelinePartial struct {
	inner TaskRunner
	once  sync.Once
}

func (r *failPipelinePartial) Exec(task Task) (ExecResult, error) {
	result, err := r.inner.Exec(task)
	if err == nil && task.Job.PipelinePhase == "reduce" && task.PartitionID == 0 {
		failed := false
		r.once.Do(func() { failed = true })
		if failed {
			return ExecResult{}, fmt.Errorf("lost reduction response")
		}
	}
	return result, err
}

func TestPipelineReductionRetry(t *testing.T) {
	runner := &failPipelinePartial{inner: localRunner{storeDir: t.TempDir()}}
	values, err := RunPipeline[float64](JobSpec{TaskName: "pipeline-two-reductions", NumPartitions: 2}, []TaskRunner{runner})
	if err != nil {
		t.Fatal(err)
	}
	assertPipelineValues(t, values, []float64{5.2, 5.3, 5.5})
}

func TestPipelineRejectsEmptyReductionAndPrematureValue(t *testing.T) {
	_, err := RunPipelineLocal[float64](JobSpec{TaskName: "pipeline-empty", NumPartitions: 2})
	if err == nil || !strings.Contains(err.Error(), "empty RDD") {
		t.Fatalf("expected empty reduction error, got %v", err)
	}
	_, err = RunPipelineAny(JobSpec{TaskName: "pipeline-normalized", PipelinePhase: "final", NumPartitions: 2}, []TaskRunner{localRunner{storeDir: t.TempDir()}})
	if err == nil || !strings.Contains(err.Error(), "phase") {
		t.Fatalf("expected pipeline phase error, got %v", err)
	}
	ctx := NewContext(&Config{AppName: "unresolved-pipeline", Master: MasterLocal, NumPartitions: 1})
	defer ctx.Stop()
	ctx.pipeline = &pipelineState{values: map[int]any{}}
	rdd := Parallelize(ctx, []float64{1}, 1)
	value := ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
	func() {
		defer func() {
			failure, ok := recover().(executionError)
			if !ok || !strings.Contains(failure.err.Error(), "no broadcast value") {
				t.Errorf("expected unresolved broadcast error, got %v", failure)
			}
		}()
		_ = value.Value()
	}()
}
