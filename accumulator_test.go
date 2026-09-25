package spark

import (
	"fmt"
	"os"
	"testing"
)

func init() {
	RegisterJob("accumulator-check", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		count, err := NewInt64Accumulator(ctx, "items")
		if err != nil {
			return nil, err
		}
		weight, err := NewFloat64Accumulator(ctx, "weight")
		if err != nil {
			return nil, err
		}
		input := Parallelize(ctx, []int{1, 2, 3, 4, 5, 6}, 2)
		base := Map(input, func(v int) int {
			count.Add(int64(v))
			weight.Add(float64(v) / 2)
			return v
		})
		if spec.Params["shuffle"] == "true" {
			return Repartition(base, 2), nil
		}
		return base, nil
	})
}

func accumulatorFixture(t *testing.T) (*Context, JobSpec, *Int64Accumulator, *Float64Accumulator) {
	t.Helper()
	ctx := NewContext(nil)
	t.Cleanup(ctx.Stop)
	count, err := NewInt64Accumulator(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	weight, err := NewFloat64Accumulator(ctx, "weight")
	if err != nil {
		t.Fatal(err)
	}
	spec := JobSpec{TaskName: "accumulator-check", Action: ActionCollect, NumPartitions: 2}
	if err := BindAccumulators(&spec, ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, spec, count, weight
}

func checkAccumulatorTotals(t *testing.T, count *Int64Accumulator, weight *Float64Accumulator) {
	t.Helper()
	i, err := count.Value()
	if err != nil || i != 21 {
		t.Fatalf("items = %d, %v", i, err)
	}
	f, err := weight.Value()
	if err != nil || f != 10.5 {
		t.Fatalf("weight = %v, %v", f, err)
	}
}

func TestLocalCollectAccumulators(t *testing.T) {
	ctx, spec, count, weight := accumulatorFixture(t)
	factory, _ := GetJob(spec.TaskName)
	rdd, err := factory(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	got := Collect(rdd.(*RDD[int]))
	if fmt.Sprint(got) != "[1 2 3 4 5 6]" {
		t.Fatal(got)
	}
	checkAccumulatorTotals(t, count, weight)
}

func TestDistributedAccumulators(t *testing.T) {
	_, spec, count, weight := accumulatorFixture(t)
	got, err := Schedule(spec, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[1 2 3 4 5 6]" {
		t.Fatal(got)
	}
	checkAccumulatorTotals(t, count, weight)
}

type lostAccumulatorResponse struct{ TaskRunner }

func (r lostAccumulatorResponse) Exec(task Task) (ExecResult, error) {
	res, err := r.TaskRunner.Exec(task)
	if err == nil && task.PartitionID == 0 && task.Attempt == 0 {
		return ExecResult{}, fmt.Errorf("lost response")
	}
	return res, err
}

func TestRetriedAccumulatorPartition(t *testing.T) {
	_, spec, count, weight := accumulatorFixture(t)
	_, err := Schedule(spec, []TaskRunner{lostAccumulatorResponse{localRunner{storeDir: t.TempDir()}}})
	if err != nil {
		t.Fatal(err)
	}
	checkAccumulatorTotals(t, count, weight)
}

func TestRepairedAccumulatorMap(t *testing.T) {
	_, spec, count, weight := accumulatorFixture(t)
	spec.Params = map[string]string{"shuffle": "true"}
	removed := false
	_, err := ScheduleWith(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{OnTaskComplete: func(_ Task, result ExecResult) error {
		if !removed && result.Manifest != nil {
			removed = true
			return os.RemoveAll(result.Manifest.Location)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	checkAccumulatorTotals(t, count, weight)
}

func TestTaskCannotReadAccumulatorTotal(t *testing.T) {
	ctx := NewContext(nil)
	defer ctx.Stop()
	ctx.prepareAccumulators(JobSpec{Accumulators: []AccumulatorDef{{Name: "items", Kind: "int64"}, {Name: "weight", Kind: "float64"}}})
	count, _ := NewInt64Accumulator(ctx, "items")
	weight, _ := NewFloat64Accumulator(ctx, "weight")
	count.Add(3)
	weight.Add(1.5)
	if _, err := count.Value(); err == nil {
		t.Fatal("task read int64 total")
	}
	if _, err := weight.Value(); err == nil {
		t.Fatal("task read float64 total")
	}
	if _, err := NewInt64Accumulator(ctx, "unknown"); err == nil {
		t.Fatal("undeclared accumulator accepted")
	}
}
