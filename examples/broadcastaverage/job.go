package broadcastaverage

import (
	"fmt"
	"math"

	spark "goSpark"
)

const (
	totalJob    = "broadcast-average-total"
	averageJob  = "broadcast-average-map"
	totalAction = "broadcast-average-sum"
)

var numbers = []float64{2, 3, 5}

type Result struct {
	Total    float64
	Averages []float64
}

func init() {
	spark.RegisterTreeReduce[float64](totalAction, func(a, b float64) float64 { return a + b })
	spark.RegisterJob(totalJob, func(ctx *spark.Context, spec spark.JobSpec) (spark.RDDAny, error) {
		return spark.Parallelize(ctx, numbers, spec.NumPartitions), nil
	})
	spark.RegisterJob(averageJob, func(ctx *spark.Context, spec spark.JobSpec) (spark.RDDAny, error) {
		totalBc, err := spark.ReadBroadcastVar[float64](ctx, spec, "total")
		if err != nil {
			return nil, err
		}
		if totalBc.Value() == 0 {
			return nil, fmt.Errorf("cannot normalize by a zero total")
		}
		rdd := spark.Parallelize(ctx, numbers, spec.NumPartitions)
		return spark.Map(rdd, func(x float64) float64 { return x / totalBc.Value() }), nil
	})
}

// Local performs the reduce, broadcast, and map within one Go process.
func Local() (Result, error) {
	ctx := spark.NewContext(&spark.Config{AppName: "broadcast-average", Master: "local[*]", NumPartitions: 2})
	defer ctx.Stop()
	rdd := spark.Parallelize(ctx, numbers, 2)
	total, ok := spark.Reduce(rdd, func(a, b float64) float64 { return a + b })
	if !ok || total == 0 {
		return Result{}, fmt.Errorf("cannot normalize an empty or zero-sum RDD")
	}
	totalBc := spark.NewBroadcast(ctx, total)
	averages := spark.Map(rdd, func(x float64) float64 { return x / totalBc.Value() })
	return Result{Total: total, Averages: spark.Collect(averages)}, nil
}

// Distributed reduces once across workers, then broadcasts the scalar in a
// second job. Both registered factories must be compiled into every worker.
func Distributed(workers []spark.TaskRunner, partitions int) (Result, error) {
	partial, err := spark.Schedule(spark.JobSpec{
		TaskName: totalJob, Action: totalAction, NumPartitions: partitions,
	}, workers)
	if err != nil {
		return Result{}, err
	}
	if len(partial) != 1 {
		return Result{}, fmt.Errorf("expected one total, got %d", len(partial))
	}
	total, ok := partial[0].(float64)
	if !ok || total == 0 {
		return Result{}, fmt.Errorf("invalid or zero total: %v", partial[0])
	}
	spec := spark.JobSpec{TaskName: averageJob, Action: spark.ActionCollect, NumPartitions: partitions}
	if err := spark.AddBroadcast(&spec, "total", total); err != nil {
		return Result{}, err
	}
	rows, err := spark.Schedule(spec, workers)
	if err != nil {
		return Result{}, err
	}
	averages := make([]float64, len(rows))
	for i, row := range rows {
		x, ok := row.(float64)
		if !ok {
			return Result{}, fmt.Errorf("unexpected normalized record %T", row)
		}
		averages[i] = x
	}
	return Result{Total: total, Averages: averages}, nil
}

// Check checks the exact fixture, including the number and order of records.
func Check(result Result) error {
	want := []float64{0.2, 0.3, 0.5}
	if result.Total != 10 || len(result.Averages) != len(want) {
		return fmt.Errorf("expected total=10 and 3 shares, got total=%g shares=%v", result.Total, result.Averages)
	}
	for i := range want {
		if math.Abs(result.Averages[i]-want[i]) > 1e-12 {
			return fmt.Errorf("share %d: got %g want %g", i, result.Averages[i], want[i])
		}
	}
	return nil
}
