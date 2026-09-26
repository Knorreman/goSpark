package mllib

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	spark "goSpark"
)

const (
	linearRegressionJob   = "mllib-linear-regression"
	logisticRegressionJob = "mllib-logistic-regression"
	lossSquared           = "squared"
	lossLogistic          = "logistic"
)

// Training follows Spark's L-BFGS linear regression: each iteration broadcasts
// the current weights, every worker streams its own rows, and only the summed
// gradient returns to the driver. Rows are not collected, and the driver does
// not form or solve XᵀX.
func init() {
	registerTrainJob(linearRegressionJob, lossSquared)
	registerTrainJob(logisticRegressionJob, lossLogistic)
}

func registerTrainJob(name, loss string) {
	spark.RegisterPipeline(name, func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[trainUpdate], error) {
		fitIntercept := spec.Params["intercept"] != "false"
		mode := spec.Params["mode"]
		if mode != "moments" && mode != "gradient" {
			return nil, fmt.Errorf("training mode must be moments or gradient")
		}
		var state trainState
		if mode == "gradient" {
			var err error
			state, err = spark.ReadBroadcast[trainState](spec, "state")
			if err != nil {
				return nil, err
			}
		}
		lines := spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions)
		return spark.MapPartitions(lines, func(it spark.Iterator[string]) spark.Iterator[trainUpdate] {
			update, err := updateFromLines(it, mode, fitIntercept, state, loss)
			if err != nil {
				return spark.SliceIterator([]trainUpdate{{Err: err.Error()}})
			}
			if update.Count == 0 {
				return spark.EmptyIterator[trainUpdate]()
			}
			return spark.SliceIterator([]trainUpdate{update})
		}), nil
	})
}

// LabeledPoint is one supervised training row.
type LabeledPoint struct {
	Label    float64
	Features []float64
}

// LinearRegressionModel is a linear model trained by batch gradient descent.
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

// Config controls distributed L-BFGS. RegParam is L2 on the weights only,
// matching Spark's intercept handling. StepSize is the initial line-search step.
type Config struct {
	FitIntercept bool
	MaxFeatures  int
	Iterations   int
	StepSize     float64
	RegParam     float64
}

func (c Config) withDefaults() Config {
	if c.MaxFeatures <= 0 {
		c.MaxFeatures = 4096
	}
	if c.Iterations <= 0 {
		c.Iterations = 100
	}
	if c.StepSize <= 0 {
		c.StepSize = 1
	}
	return c
}

// DefaultConfig fits an intercept for up to 100 L-BFGS iterations.
func DefaultConfig() Config {
	return Config{FitIntercept: true, MaxFeatures: 4096, Iterations: 100, StepSize: 1}
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

// Train fits points that are already in memory. The optimizer is the same one
// used for distributed training.
func Train(points []LabeledPoint, cfg Config) (LinearRegressionModel, error) {
	return trainLabeled(points, cfg, lossSquared)
}

// TrainRDD runs one distributed pass per iteration over an existing RDD.
// Only a gradient vector is collected from each partition.
func TrainRDD(points *spark.RDD[LabeledPoint], cfg Config) (LinearRegressionModel, error) {
	return trainRDD(points, cfg, lossSquared)
}

// TrainFiles fits a text dataset visible at path on every worker.
// Each iteration broadcasts the current weights and collects partition gradients.
// Each line is "label f1 f2 ...". The worker image must import this package.
func TrainFiles(path string, runners []spark.TaskRunner, partitions int, cfg Config) (LinearRegressionModel, error) {
	return trainFiles(path, runners, partitions, cfg, linearRegressionJob)
}

func trainFiles(path string, runners []spark.TaskRunner, partitions int, cfg Config, job string) (LinearRegressionModel, error) {
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
	return descend(cfg, -1, func(mode string, state trainState) (trainUpdate, error) {
		spec := spark.JobSpec{
			TaskName:      job,
			NumPartitions: partitions,
			Params:        map[string]string{"path": path, "intercept": intercept, "mode": mode},
		}
		if mode == "gradient" {
			if err := spark.AddBroadcast(&spec, "state", state); err != nil {
				return trainUpdate{}, err
			}
		}
		recs, err := spark.RunPipeline[trainUpdate](spec, runners)
		if err != nil {
			return trainUpdate{}, err
		}
		return mergeUpdates(recs)
	})
}

type trainState struct {
	Weights   []float64
	Intercept float64
	Mean      []float64
	Scale     []float64
	Center    bool
}

type trainUpdate struct {
	Count int
	Sum   []float64
	SumSq []float64
	Grad  []float64
	Bias  float64
	Loss  float64
	Err   string
}

type featureScale struct {
	Mean   []float64
	Scale  []float64
	Center bool
}

func descend(cfg Config, features int, pass func(mode string, state trainState) (trainUpdate, error)) (LinearRegressionModel, error) {
	moments, err := pass("moments", trainState{})
	if err != nil {
		return LinearRegressionModel{}, err
	}
	if moments.Err != "" {
		return LinearRegressionModel{}, fmt.Errorf("%s", moments.Err)
	}
	if moments.Count == 0 {
		return LinearRegressionModel{}, fmt.Errorf("no training rows")
	}
	if features < 0 {
		features = len(moments.Sum)
	}
	if features == 0 || features != len(moments.Sum) {
		return LinearRegressionModel{}, fmt.Errorf("feature length does not match training rows")
	}
	if features > cfg.MaxFeatures {
		return LinearRegressionModel{}, fmt.Errorf("feature count exceeds limit %d", cfg.MaxFeatures)
	}
	scale, err := scaler(moments, cfg.FitIntercept)
	if err != nil {
		return LinearRegressionModel{}, err
	}
	weights := make([]float64, features)
	var intercept float64
	var history []lbfgsPair
	state := trainState{Weights: weights, Intercept: intercept, Mean: scale.Mean, Scale: scale.Scale, Center: scale.Center}
	grad, loss, err := meanGradient(pass, state, moments.Count, features, cfg)
	if err != nil {
		return LinearRegressionModel{}, err
	}
	for iter := 0; iter < cfg.Iterations; iter++ {
		if norm(grad) < 1e-10 {
			break
		}
		direction := lbfgsDirection(grad, history)
		if dot(grad, direction) >= 0 {
			history = nil
			direction = scaleVec(grad, -1)
		}
		_, next, nextGrad, nextLoss, err := lineSearch(cfg, scale, weights, intercept, loss, grad, direction, pass, moments.Count, features)
		if err != nil {
			return LinearRegressionModel{}, err
		}
		previous := appendParam(weights, intercept, cfg.FitIntercept)
		s := make([]float64, len(grad))
		y := make([]float64, len(grad))
		for i := range grad {
			s[i] = next[i] - previous[i]
			y[i] = nextGrad[i] - grad[i]
		}
		if dot(y, s) > 1e-12 {
			history = append(history, lbfgsPair{s: s, y: y, rho: 1 / dot(y, s)})
			if len(history) > 10 {
				history = history[1:]
			}
		}
		copy(weights, next[:features])
		if cfg.FitIntercept {
			intercept = next[features]
		}
		grad, loss = nextGrad, nextLoss
	}
	return unscale(weights, intercept, scale), nil
}

func scaler(moments trainUpdate, fitIntercept bool) (featureScale, error) {
	n := float64(moments.Count)
	scale := featureScale{Mean: make([]float64, len(moments.Sum)), Scale: make([]float64, len(moments.Sum)), Center: fitIntercept}
	for i := range moments.Sum {
		mean := moments.Sum[i] / n
		scale.Mean[i] = mean
		second := moments.SumSq[i] / n
		variance := second - mean*mean
		if !fitIntercept {
			variance = second
		}
		if variance < 1e-18 {
			return featureScale{}, fmt.Errorf("singular training matrix")
		}
		scale.Scale[i] = 1 / math.Sqrt(variance)
	}
	return scale, nil
}

func unscale(weights []float64, intercept float64, scale featureScale) LinearRegressionModel {
	raw := make([]float64, len(weights))
	for i := range weights {
		raw[i] = weights[i] * scale.Scale[i]
		if scale.Center {
			intercept -= weights[i] * scale.Mean[i] * scale.Scale[i]
		}
	}
	return LinearRegressionModel{Weights: raw, Intercept: intercept}
}

func updateFromLines(it spark.Iterator[string], mode string, fitIntercept bool, state trainState, loss string) (trainUpdate, error) {
	return updateFrom(mode, fitIntercept, state, func(fn func(LabeledPoint) error) error {
		for {
			line, ok := it()
			if !ok {
				return nil
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			point, err := ParseLabeledPoint(line)
			if err != nil {
				return err
			}
			if err := fn(point); err != nil {
				return err
			}
		}
	}, loss)
}

func updateFromPoints(points []LabeledPoint, mode string, fitIntercept bool, state trainState, loss string) (trainUpdate, error) {
	return updateFrom(mode, fitIntercept, state, func(fn func(LabeledPoint) error) error {
		for _, point := range points {
			if err := fn(point); err != nil {
				return err
			}
		}
		return nil
	}, loss)
}

func updateFromPointsIter(it spark.Iterator[LabeledPoint], mode string, fitIntercept bool, state trainState, loss string) (trainUpdate, error) {
	return updateFrom(mode, fitIntercept, state, func(fn func(LabeledPoint) error) error {
		for {
			point, ok := it()
			if !ok {
				return nil
			}
			if err := fn(point); err != nil {
				return err
			}
		}
	}, loss)
}

func updateFrom(mode string, fitIntercept bool, state trainState, scan func(func(LabeledPoint) error) error, loss string) (trainUpdate, error) {
	var update trainUpdate
	var features int
	scaled := []float64{}
	err := scan(func(point LabeledPoint) error {
		if features == 0 {
			features = len(point.Features)
			if features == 0 {
				return fmt.Errorf("labeled point has no features")
			}
			if mode == "moments" {
				update.Sum = make([]float64, features)
				update.SumSq = make([]float64, features)
			} else {
				if len(state.Weights) != features {
					return fmt.Errorf("broadcast weights do not match features")
				}
				update.Grad = make([]float64, features)
				scaled = make([]float64, features)
			}
		} else if len(point.Features) != features {
			return fmt.Errorf("feature length %d does not match %d", len(point.Features), features)
		}
		if loss == lossLogistic && point.Label != 0 && point.Label != 1 {
			return fmt.Errorf("logistic label must be 0 or 1, got %v", point.Label)
		}
		update.Count++
		if mode == "moments" {
			for i, value := range point.Features {
				update.Sum[i] += value
				update.SumSq[i] += value * value
			}
			return nil
		}
		for i, value := range point.Features {
			if state.Center {
				scaled[i] = (value - state.Mean[i]) * state.Scale[i]
			} else {
				scaled[i] = value * state.Scale[i]
			}
		}
		margin := state.Intercept + dot(state.Weights, scaled)
		diff, pointLoss, err := lossGradient(loss, margin, point.Label)
		if err != nil {
			return err
		}
		for i := range update.Grad {
			update.Grad[i] += diff * scaled[i]
		}
		if fitIntercept {
			update.Bias += diff
		}
		update.Loss += pointLoss
		return nil
	})
	return update, err
}

func mergeUpdates(parts []trainUpdate) (trainUpdate, error) {
	if len(parts) == 0 {
		return trainUpdate{}, nil
	}
	total := parts[0]
	total.Sum = append([]float64(nil), total.Sum...)
	total.SumSq = append([]float64(nil), total.SumSq...)
	total.Grad = append([]float64(nil), total.Grad...)
	for _, part := range parts[1:] {
		if part.Err != "" {
			return trainUpdate{}, fmt.Errorf("%s", part.Err)
		}
		if total.Err != "" {
			return trainUpdate{}, fmt.Errorf("%s", total.Err)
		}
		if (len(part.Sum) > 0 && len(part.Sum) != len(total.Sum)) || (len(part.Grad) > 0 && len(part.Grad) != len(total.Grad)) {
			return trainUpdate{}, fmt.Errorf("inconsistent training partitions")
		}
		total.Count += part.Count
		for i := range part.Sum {
			total.Sum[i] += part.Sum[i]
			total.SumSq[i] += part.SumSq[i]
		}
		for i := range part.Grad {
			total.Grad[i] += part.Grad[i]
		}
		total.Bias += part.Bias
		total.Loss += part.Loss
	}
	if total.Err != "" {
		return trainUpdate{}, fmt.Errorf("%s", total.Err)
	}
	return total, nil
}

type lbfgsPair struct {
	s, y []float64
	rho  float64
}

func meanGradient(pass func(string, trainState) (trainUpdate, error), state trainState, count, features int, cfg Config) ([]float64, float64, error) {
	grad, err := pass("gradient", state)
	if err != nil {
		return nil, 0, err
	}
	if grad.Err != "" {
		return nil, 0, fmt.Errorf("%s", grad.Err)
	}
	if grad.Count != count || len(grad.Grad) != features {
		return nil, 0, fmt.Errorf("incomplete gradient")
	}
	invN := 1 / float64(grad.Count)
	g := make([]float64, features)
	for i := range g {
		g[i] = grad.Grad[i]*invN + cfg.RegParam*state.Weights[i]
	}
	if cfg.FitIntercept {
		g = append(g, grad.Bias*invN)
	}
	reg := 0.0
	for _, weight := range state.Weights {
		reg += weight * weight
	}
	return g, grad.Loss*invN + 0.5*cfg.RegParam*reg, nil
}

func lineSearch(cfg Config, scale featureScale, weights []float64, intercept, loss float64, grad, direction []float64, pass func(string, trainState) (trainUpdate, error), count, features int) (float64, []float64, []float64, float64, error) {
	slope := dot(grad, direction)
	step := cfg.StepSize
	current := appendParam(weights, intercept, cfg.FitIntercept)
	for tries := 0; tries < 30; tries++ {
		trial := make([]float64, len(current))
		for i := range trial {
			trial[i] = current[i] + step*direction[i]
		}
		state := stateFrom(trial, scale, cfg.FitIntercept)
		nextGrad, nextLoss, err := meanGradient(pass, state, count, features, cfg)
		if err != nil {
			return 0, nil, nil, 0, err
		}
		if nextLoss <= loss+1e-4*step*slope {
			return step, trial, nextGrad, nextLoss, nil
		}
		step *= 0.5
	}
	return 0, nil, nil, 0, fmt.Errorf("line search failed")
}

func lbfgsDirection(grad []float64, history []lbfgsPair) []float64 {
	q := append([]float64(nil), grad...)
	alpha := make([]float64, len(history))
	for i := len(history) - 1; i >= 0; i-- {
		alpha[i] = history[i].rho * dot(history[i].s, q)
		q = axpy(-alpha[i], history[i].y, q)
	}
	gamma := 1.0
	if n := len(history); n > 0 {
		last := history[n-1]
		denom := dot(last.y, last.y)
		if denom > 1e-18 {
			gamma = dot(last.s, last.y) / denom
		}
	}
	r := scaleVec(q, gamma)
	for i := range history {
		beta := history[i].rho * dot(history[i].y, r)
		r = axpy(alpha[i]-beta, history[i].s, r)
	}
	return scaleVec(r, -1)
}

func stateFrom(param []float64, scale featureScale, fitIntercept bool) trainState {
	features := len(param)
	intercept := 0.0
	if fitIntercept {
		features--
		intercept = param[features]
	}
	return trainState{Weights: param[:features], Intercept: intercept, Mean: scale.Mean, Scale: scale.Scale, Center: scale.Center}
}

func appendParam(weights []float64, intercept float64, fitIntercept bool) []float64 {
	out := append([]float64(nil), weights...)
	if fitIntercept {
		out = append(out, intercept)
	}
	return out
}

func axpy(alpha float64, x, y []float64) []float64 {
	out := make([]float64, len(y))
	for i := range y {
		out[i] = y[i] + alpha*x[i]
	}
	return out
}

func scaleVec(v []float64, alpha float64) []float64 {
	out := make([]float64, len(v))
	for i := range v {
		out[i] = alpha * v[i]
	}
	return out
}

func norm(v []float64) float64 {
	return math.Sqrt(dot(v, v))
}

func lossGradient(kind string, margin, label float64) (float64, float64, error) {
	switch kind {
	case lossSquared:
		diff := margin - label
		return diff, 0.5 * diff * diff, nil
	case lossLogistic:
		if label != 0 && label != 1 {
			return 0, 0, fmt.Errorf("logistic label must be 0 or 1, got %v", label)
		}
		return sigmoid(margin) - label, logisticLoss(margin, label), nil
	default:
		return 0, 0, fmt.Errorf("unknown loss %q", kind)
	}
}

func sigmoid(x float64) float64 {
	if x >= 0 {
		z := math.Exp(-x)
		return 1 / (1 + z)
	}
	z := math.Exp(x)
	return z / (1 + z)
}

func logisticLoss(margin, label float64) float64 {
	if margin >= 0 {
		return math.Log1p(math.Exp(-margin)) + (1-label)*margin
	}
	return math.Log1p(math.Exp(margin)) - label*margin
}

func dot(weights, features []float64) float64 {
	var sum float64
	for i := range weights {
		sum += weights[i] * features[i]
	}
	return sum
}
