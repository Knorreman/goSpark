package mllib

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	spark "goSpark"
)

func TestLogisticRegressionSeparatesClasses(t *testing.T) {
	var points []LabeledPoint
	for x := 0; x < 6; x++ {
		for z := 0; z < 6; z++ {
			label := 0.0
			if x+z >= 6 {
				label = 1
			}
			points = append(points, LabeledPoint{Label: label, Features: []float64{float64(x), float64(z)}})
		}
	}
	model, err := TrainLogistic(points, Config{FitIntercept: true, Iterations: 30, RegParam: 0.01})
	if err != nil {
		t.Fatal(err)
	}
	low, err := model.Predict([]float64{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	high, err := model.Predict([]float64{5, 5})
	if err != nil {
		t.Fatal(err)
	}
	if low != 0 || high != 1 {
		t.Fatalf("classes = %g %g", low, high)
	}
	p0, err := model.PredictProbability([]float64{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	p1, err := model.PredictProbability([]float64{5, 5})
	if err != nil {
		t.Fatal(err)
	}
	if !(p0 < 0.5 && p0 < p1 && p1 > 0.5) {
		t.Fatalf("probabilities = %g %g", p0, p1)
	}
}

func TestLogisticRegressionDistributedFiles(t *testing.T) {
	dir := t.TempDir()
	var class0, class1 []string
	for x := 0; x < 6; x++ {
		class0 = append(class0, fmt.Sprintf("0 %d", x))
		class1 = append(class1, fmt.Sprintf("1 %d", x+8))
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte(strings.Join(class0, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte(strings.Join(class1, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	model, err := TrainLogisticFiles(dir, []spark.TaskRunner{startWorker(t), startWorker(t)}, 2, Config{FitIntercept: true, Iterations: 20, RegParam: 0.01})
	if err != nil {
		t.Fatal(err)
	}
	low, err := model.Predict([]float64{0})
	if err != nil {
		t.Fatal(err)
	}
	high, err := model.Predict([]float64{12})
	if err != nil {
		t.Fatal(err)
	}
	if low != 0 || high != 1 {
		t.Fatalf("distributed classes = %g %g", low, high)
	}
}

func TestLogisticRegressionRejectsOtherLabels(t *testing.T) {
	_, err := TrainLogistic([]LabeledPoint{{Label: 2, Features: []float64{1}}}, Config{FitIntercept: true})
	if err == nil || !strings.Contains(err.Error(), "0 or 1") {
		t.Fatalf("got %v", err)
	}
}
