package mllib

import spark "goSpark"

// LogisticRegressionModel is a binary classifier. Labels are 0 or 1.
type LogisticRegressionModel struct {
	Weights   []float64
	Intercept float64
}

// PredictProbability returns the probability that the label is 1.
func (m LogisticRegressionModel) PredictProbability(features []float64) (float64, error) {
	raw, err := (LinearRegressionModel{Weights: m.Weights, Intercept: m.Intercept}).Predict(features)
	if err != nil {
		return 0, err
	}
	return sigmoid(raw), nil
}

// Predict returns 1 when the probability is at least 0.5, otherwise 0.
func (m LogisticRegressionModel) Predict(features []float64) (float64, error) {
	probability, err := m.PredictProbability(features)
	if err != nil {
		return 0, err
	}
	if probability >= 0.5 {
		return 1, nil
	}
	return 0, nil
}

// TrainLogistic fits binary logistic regression with the same distributed L-BFGS
// loop as linear regression. Only gradients are collected.
func TrainLogistic(points []LabeledPoint, cfg Config) (LogisticRegressionModel, error) {
	model, err := trainLabeled(points, cfg, lossLogistic)
	return logisticModel(model), err
}

// TrainLogisticRDD fits an RDD of labeled points without collecting the rows.
func TrainLogisticRDD(points *spark.RDD[LabeledPoint], cfg Config) (LogisticRegressionModel, error) {
	model, err := trainRDD(points, cfg, lossLogistic)
	return logisticModel(model), err
}

// TrainLogisticFiles fits text rows visible on every worker.
// Each line is "label f1 f2 ...", and label must be 0 or 1.
func TrainLogisticFiles(path string, runners []spark.TaskRunner, partitions int, cfg Config) (LogisticRegressionModel, error) {
	model, err := trainFiles(path, runners, partitions, cfg, logisticRegressionJob)
	return logisticModel(model), err
}

func trainLabeled(points []LabeledPoint, cfg Config, loss string) (LinearRegressionModel, error) {
	cfg = cfg.withDefaults()
	if len(points) == 0 {
		return LinearRegressionModel{}, errNoRows()
	}
	return descend(cfg, len(points[0].Features), func(mode string, state trainState) (trainUpdate, error) {
		return updateFromPoints(points, mode, cfg.FitIntercept, state, loss)
	})
}

func trainRDD(points *spark.RDD[LabeledPoint], cfg Config, loss string) (LinearRegressionModel, error) {
	cfg = cfg.withDefaults()
	return descend(cfg, -1, func(mode string, state trainState) (trainUpdate, error) {
		parts := spark.MapPartitions(points, func(it spark.Iterator[LabeledPoint]) spark.Iterator[trainUpdate] {
			update, err := updateFromPointsIter(it, mode, cfg.FitIntercept, state, loss)
			if err != nil {
				return spark.SliceIterator([]trainUpdate{{Err: err.Error()}})
			}
			if update.Count == 0 {
				return spark.EmptyIterator[trainUpdate]()
			}
			return spark.SliceIterator([]trainUpdate{update})
		})
		return mergeUpdates(spark.Collect(parts))
	})
}

func logisticModel(model LinearRegressionModel) LogisticRegressionModel {
	return LogisticRegressionModel{Weights: model.Weights, Intercept: model.Intercept}
}

func errNoRows() error {
	return errString("no training rows")
}

type errString string

func (e errString) Error() string { return string(e) }
