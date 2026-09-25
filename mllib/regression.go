package mllib

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	spark "goSpark"
)

const linearRegressionJob = "mllib-linear-regression"

func init() {
	spark.RegisterJob(linearRegressionJob, func(ctx *spark.Context, spec spark.JobSpec) (spark.RDDAny, error) {
		fitIntercept := spec.Params["intercept"] != "false"
		lines := spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions)
		return spark.MapPartitions(lines, func(it spark.Iterator[string]) spark.Iterator[regressionStats] {
			var batch []LabeledPoint
			for {
				line, ok := it()
				if !ok {
					break
				}
				if strings.TrimSpace(line) == "" {
					continue
				}
				point, err := ParseLabeledPoint(line)
				if err != nil {
					return spark.SliceIterator([]regressionStats{{Err: err.Error()}})
				}
				batch = append(batch, point)
			}
			stats, err := summarize(batch, fitIntercept)
			if err != nil {
				return spark.SliceIterator([]regressionStats{{Err: err.Error()}})
			}
			if stats.Count == 0 {
				return spark.EmptyIterator[regressionStats]()
			}
			return spark.SliceIterator([]regressionStats{stats})
		}), nil
	})
}

// LabeledPoint is one supervised training row.
type LabeledPoint struct {
	Label    float64
	Features []float64
}

// LinearRegressionModel is an ordinary least-squares model.
type LinearRegressionModel struct {
	Weights   []float64
	Intercept float64
}

// Predict returns the fitted value for one feature vector.
func (m LinearRegressionModel) Predict(features []float64) (float64, error) {
	if len(features) != len(m.Weights) {
		return 0, fmt.Errorf("feature length %d does not match model length %d", len(features), len(m.Weights))
	}
	return dot(m.Weights, features) + m.Intercept, nil
}

// Config controls ordinary least-squares training.
type Config struct {
	FitIntercept bool
	MaxFeatures  int
}

func (c Config) withDefaults() Config {
	if c.MaxFeatures <= 0 {
		c.MaxFeatures = 32
	}
	return c
}

// DefaultConfig fits an intercept and allows up to 32 features.
func DefaultConfig() Config {
	return Config{FitIntercept: true, MaxFeatures: 32}
}

// ParseLabeledPoint reads "label f1 f2 ..." with whitespace separators.
func ParseLabeledPoint(line string) (LabeledPoint, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return LabeledPoint{}, fmt.Errorf("labeled point needs a label and one feature: %q", line)
	}
	label, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return LabeledPoint{}, fmt.Errorf("label: %w", err)
	}
	features := make([]float64, len(fields)-1)
	for i, field := range fields[1:] {
		features[i], err = strconv.ParseFloat(field, 64)
		if err != nil {
			return LabeledPoint{}, fmt.Errorf("feature %d: %w", i, err)
		}
	}
	return LabeledPoint{Label: label, Features: features}, nil
}

// Train fits ordinary least squares on an in-memory sample.
func Train(points []LabeledPoint, cfg Config) (LinearRegressionModel, error) {
	cfg = cfg.withDefaults()
	stats, err := summarize(points, cfg.FitIntercept)
	if err != nil {
		return LinearRegressionModel{}, err
	}
	if stats.Dim-interceptColumns(cfg.FitIntercept) > cfg.MaxFeatures {
		return LinearRegressionModel{}, fmt.Errorf("feature count exceeds limit %d", cfg.MaxFeatures)
	}
	return solve(stats, cfg.FitIntercept)
}

// TrainRDD fits ordinary least squares by summing per-partition normal equations.
func TrainRDD(points *spark.RDD[LabeledPoint], cfg Config) (LinearRegressionModel, error) {
	cfg = cfg.withDefaults()
	parts := spark.MapPartitions(points, func(it spark.Iterator[LabeledPoint]) spark.Iterator[regressionStats] {
		var batch []LabeledPoint
		for {
			point, ok := it()
			if !ok {
				break
			}
			batch = append(batch, point)
		}
		stats, err := summarize(batch, cfg.FitIntercept)
		if err != nil {
			return spark.SliceIterator([]regressionStats{{Err: err.Error()}})
		}
		if stats.Count == 0 {
			return spark.EmptyIterator[regressionStats]()
		}
		return spark.SliceIterator([]regressionStats{stats})
	})
	return solveAll(spark.Collect(parts), cfg)
}

// TrainFiles fits a text dataset already visible at path on every worker.
// Each line is "label f1 f2 ...". The worker image must import this package.
func TrainFiles(path string, runners []spark.TaskRunner, partitions int, cfg Config) (LinearRegressionModel, error) {
	cfg = cfg.withDefaults()
	if path == "" {
		return LinearRegressionModel{}, fmt.Errorf("training path is required")
	}
	if partitions <= 0 {
		partitions = 1
	}
	intercept := "true"
	if !cfg.FitIntercept {
		intercept = "false"
	}
	recs, err := spark.Schedule(spark.JobSpec{
		TaskName:      linearRegressionJob,
		Action:        spark.ActionCollect,
		NumPartitions: partitions,
		Params:        map[string]string{"path": path, "intercept": intercept},
	}, runners)
	if err != nil {
		return LinearRegressionModel{}, err
	}
	stats := make([]regressionStats, 0, len(recs))
	for _, rec := range recs {
		item, ok := rec.(regressionStats)
		if !ok {
			return LinearRegressionModel{}, fmt.Errorf("unexpected training record %T", rec)
		}
		stats = append(stats, item)
	}
	return solveAll(stats, cfg)
}

type regressionStats struct {
	Count int
	Dim   int
	XtX   []float64
	Xty   []float64
	Err   string
}

func summarize(points []LabeledPoint, fitIntercept bool) (regressionStats, error) {
	if len(points) == 0 {
		return regressionStats{}, nil
	}
	features := len(points[0].Features)
	if features == 0 {
		return regressionStats{}, fmt.Errorf("labeled point has no features")
	}
	dim := features + interceptColumns(fitIntercept)
	stats := regressionStats{Dim: dim, XtX: make([]float64, dim*dim), Xty: make([]float64, dim)}
	row := make([]float64, dim)
	for _, point := range points {
		if len(point.Features) != features {
			return regressionStats{}, fmt.Errorf("feature length %d does not match %d", len(point.Features), features)
		}
		copy(row, point.Features)
		if fitIntercept {
			row[features] = 1
		}
		stats.Count++
		for i := 0; i < dim; i++ {
			stats.Xty[i] += row[i] * point.Label
			for j := 0; j < dim; j++ {
				stats.XtX[i*dim+j] += row[i] * row[j]
			}
		}
	}
	return stats, nil
}

func solveAll(parts []regressionStats, cfg Config) (LinearRegressionModel, error) {
	if len(parts) == 0 {
		return LinearRegressionModel{}, fmt.Errorf("no training rows")
	}
	if parts[0].Err != "" {
		return LinearRegressionModel{}, fmt.Errorf("%s", parts[0].Err)
	}
	total := parts[0]
	total.XtX = append([]float64(nil), total.XtX...)
	total.Xty = append([]float64(nil), total.Xty...)
	for _, part := range parts[1:] {
		if part.Err != "" {
			return LinearRegressionModel{}, fmt.Errorf("%s", part.Err)
		}
		if part.Dim != total.Dim || len(part.XtX) != len(total.XtX) {
			return LinearRegressionModel{}, fmt.Errorf("inconsistent training partitions")
		}
		total.Count += part.Count
		for i := range total.XtX {
			total.XtX[i] += part.XtX[i]
		}
		for i := range total.Xty {
			total.Xty[i] += part.Xty[i]
		}
	}
	features := total.Dim - interceptColumns(cfg.FitIntercept)
	if features > cfg.MaxFeatures {
		return LinearRegressionModel{}, fmt.Errorf("feature count exceeds limit %d", cfg.MaxFeatures)
	}
	return solve(total, cfg.FitIntercept)
}

func solve(stats regressionStats, fitIntercept bool) (LinearRegressionModel, error) {
	if stats.Count == 0 {
		return LinearRegressionModel{}, fmt.Errorf("no training rows")
	}
	matrix := make([][]float64, stats.Dim)
	target := append([]float64(nil), stats.Xty...)
	for i := range matrix {
		matrix[i] = append([]float64(nil), stats.XtX[i*stats.Dim:(i+1)*stats.Dim]...)
	}
	solution, err := solveLinear(matrix, target)
	if err != nil {
		return LinearRegressionModel{}, err
	}
	features := stats.Dim
	intercept := 0.0
	if fitIntercept {
		features--
		intercept = solution[features]
	}
	return LinearRegressionModel{Weights: solution[:features], Intercept: intercept}, nil
}

func solveLinear(a [][]float64, b []float64) ([]float64, error) {
	n := len(b)
	for col := 0; col < n; col++ {
		pivot := col
		best := math.Abs(a[col][col])
		for row := col + 1; row < n; row++ {
			if value := math.Abs(a[row][col]); value > best {
				best = value
				pivot = row
			}
		}
		if best < 1e-10 {
			return nil, fmt.Errorf("singular training matrix")
		}
		a[col], a[pivot] = a[pivot], a[col]
		b[col], b[pivot] = b[pivot], b[col]
		scale := a[col][col]
		for c := col; c < n; c++ {
			a[col][c] /= scale
		}
		b[col] /= scale
		for row := 0; row < n; row++ {
			if row == col {
				continue
			}
			factor := a[row][col]
			for c := col; c < n; c++ {
				a[row][c] -= factor * a[col][c]
			}
			b[row] -= factor * b[col]
		}
	}
	return b, nil
}

func interceptColumns(fit bool) int {
	if fit {
		return 1
	}
	return 0
}

func dot(weights, features []float64) float64 {
	var sum float64
	for i := range weights {
		sum += weights[i] * features[i]
	}
	return sum
}
