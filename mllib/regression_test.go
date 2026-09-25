package mllib

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	spark "goSpark"
)

func TestMain(m *testing.M) {
	if os.Getenv("GOSPARK_WORKER_SERVE") == "1" {
		store := os.Getenv("GOSPARK_STORE")
		srv, addr, err := spark.ServeWorker(store, "127.0.0.1:0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "worker serve: %v\n", err)
			os.Exit(1)
		}
		_ = srv
		fmt.Printf("LISTEN=%s\n", addr)
		os.Stdout.Sync()
		select {}
	}
	os.Exit(m.Run())
}

func TestLinearRegressionRecoversKnownLine(t *testing.T) {
	var points []LabeledPoint
	for x := 0; x < 6; x++ {
		for z := 0; z < 4; z++ {
			points = append(points, LabeledPoint{Label: 1 + 2*float64(x) - 3*float64(z), Features: []float64{float64(x), float64(z)}})
		}
	}
	model, err := Train(points, Config{FitIntercept: true, Iterations: 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNear(t, model.Intercept, 1)
	assertNear(t, model.Weights[0], 2)
	assertNear(t, model.Weights[1], -3)
	got, err := model.Predict([]float64{4, 1})
	if err != nil {
		t.Fatal(err)
	}
	assertNear(t, got, 1+8-3)
}

func TestLinearRegressionDistributedFiles(t *testing.T) {
	dir := t.TempDir()
	var lines []string
	for x := 0; x < 8; x++ {
		lines = append(lines, fmt.Sprintf("%g %g", 1+2*float64(x), float64(x)))
	}
	for i, chunk := range [][]string{lines[:3], lines[3:6], lines[6:]} {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("part-%d.txt", i)), []byte(strings.Join(chunk, "\n")+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	model, err := TrainFiles(dir, []spark.TaskRunner{startWorker(t), startWorker(t)}, 2, Config{FitIntercept: true, Iterations: 5})
	if err != nil {
		t.Fatal(err)
	}
	assertNear(t, model.Intercept, 1)
	assertNear(t, model.Weights[0], 2)
}

func TestLinearRegressionCorrelatedFeatures(t *testing.T) {
	var points []LabeledPoint
	for i := 0; i < 12; i++ {
		x := float64(i)
		z := 0.25*x + float64(i%3)
		points = append(points, LabeledPoint{Label: 4 + 3*x - 2*z, Features: []float64{x, z}})
	}
	model, err := Train(points, Config{FitIntercept: true, Iterations: 40})
	if err != nil {
		t.Fatal(err)
	}
	assertNear(t, model.Intercept, 4)
	assertNear(t, model.Weights[0], 3)
	assertNear(t, model.Weights[1], -2)
}

func TestLinearRegressionRejectsBadInput(t *testing.T) {
	if _, err := ParseLabeledPoint("1"); err == nil {
		t.Fatal("accepted a point without features")
	}
	if _, err := Train(nil, Config{}); err == nil {
		t.Fatal("trained on no rows")
	}
	_, err := Train([]LabeledPoint{{Label: 1, Features: []float64{1}}, {Label: 2, Features: []float64{1, 2}}}, Config{})
	if err == nil {
		t.Fatal("accepted inconsistent feature lengths")
	}
	_, err = Train([]LabeledPoint{{Label: 1, Features: []float64{0}}, {Label: 2, Features: []float64{0}}}, Config{FitIntercept: true})
	if err == nil || !strings.Contains(err.Error(), "singular") {
		t.Fatalf("expected singular matrix, got %v", err)
	}
}

func assertNear(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Fatalf("got %g want %g", got, want)
	}
}

func startWorker(t *testing.T) *spark.WorkerClient {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "GOSPARK_WORKER_SERVE=1", "GOSPARK_STORE="+t.TempDir())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	done := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		if sc.Scan() {
			done <- sc.Text()
			return
		}
		done <- ""
	}()
	var line string
	select {
	case line = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for worker")
	}
	if !strings.HasPrefix(line, "LISTEN=") {
		t.Fatalf("expected LISTEN=, got %q", line)
	}
	return &spark.WorkerClient{BaseURL: "http://" + strings.TrimPrefix(line, "LISTEN=")}
}
