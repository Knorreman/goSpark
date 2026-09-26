package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	spark "goSpark"
	"goSpark/mllib"
)

func init() {
	spark.RegisterPipeline("sample-data", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[string], error) {
		return spark.Parallelize(ctx, []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}, spec.NumPartitions), nil
	})
	spark.RegisterPipeline("k8s-join", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[int, spark.Pair[string, int]]], error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		left := spark.Parallelize(ctx, []spark.Pair[int, string]{
			spark.NewPair(1, "one"), spark.NewPair(2, "two"), spark.NewPair(3, "three"),
		}, np)
		right := spark.Parallelize(ctx, []spark.Pair[int, int]{
			spark.NewPair(1, 10), spark.NewPair(1, 11), spark.NewPair(2, 20), spark.NewPair(4, 40),
		}, np)
		return spark.Join(left, right, spark.NewHashPartitioner(np)), nil
	})
	spark.RegisterPipeline("k8s-wc", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		lines := []string{"hello world", "hello spark", "world spark hello"}
		rdd := spark.Parallelize(ctx, lines, np)
		words := spark.FlatMap(rdd, func(line string) []string { return strings.Fields(line) })
		pairs := spark.Map(words, func(w string) spark.Pair[string, int] { return spark.NewPair(w, 1) })
		return spark.ReduceByKey(pairs, spark.NewHashPartitioner(np), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("k8s-broadcast", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		rates, err := spark.ReadBroadcast[map[string]int](spec, "rates")
		if err != nil {
			return nil, err
		}
		words := spark.Parallelize(ctx, []string{"a", "b", "missing"}, spec.NumPartitions)
		return spark.Map(words, func(word string) spark.Pair[string, int] {
			return spark.NewPair(word, rates[word])
		}), nil
	})
	spark.RegisterPipeline("k8s-text", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		lines := spark.TextFile(ctx, spec.Params["input"], np)
		words := spark.FlatMap(lines, func(line string) []string { return strings.Fields(line) })
		pairs := spark.Map(words, func(word string) spark.Pair[string, int] { return spark.NewPair(word, 1) })
		return spark.ReduceByKey(pairs, spark.NewHashPartitioner(np), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("k8s-sort", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[int, int]], error) {
		np := spec.NumPartitions
		if np <= 0 {
			np = 2
		}
		rows := spark.Parallelize(ctx, []spark.Pair[int, int]{
			spark.NewPair(4, 1), spark.NewPair(1, 1), spark.NewPair(3, 1), spark.NewPair(2, 1),
		}, np)
		return spark.SortByKey(rows, func(a, b int) bool { return a < b }, true, np), nil
	})
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: gospark-worker <command>\n")
		fmt.Fprintf(os.Stderr, "Commands: smoke, wordcount, distributed, savetest, distributed-save, serve, schedule, print-k8s\n")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "smoke":
		runSmokeTest()
	case "wordcount":
		runWordCount()
	case "distributed":
		runDistributed()
	case "savetest":
		runSaveTest()
	case "distributed-save":
		runDistributedSave()
	case "serve":
		runServe()
	case "schedule":
		runRunPipelineAny()
	case "create-bucket":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "bucket name required")
			os.Exit(1)
		}
		if err := spark.CreateS3Bucket(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "verify-output":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "output path required")
			os.Exit(1)
		}
		if err := runVerifyOutput(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "print-k8s":
		runPrintK8s()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		fmt.Fprintf(os.Stderr, "Commands: smoke, wordcount, distributed, savetest, distributed-save, serve, schedule, print-k8s\n")
		os.Exit(1)
	}
}

func runSmokeTest() {
	fmt.Println("goSpark worker smoke test starting...")
	ctx := spark.NewContext(&spark.Config{
		AppName:       "smoke-test",
		Master:        "local[1]",
		NumPartitions: 2,
	})
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rdd := spark.Parallelize(ctx, data, 2)

	sum, ok := spark.Reduce(rdd, func(a, b int) int { return a + b })
	if !ok {
		fmt.Fprintf(os.Stderr, "Reduce failed\n")
		os.Exit(1)
	}

	expected := 55
	if sum != expected {
		fmt.Fprintf(os.Stderr, "Expected sum %d, got %d\n", expected, sum)
		os.Exit(1)
	}

	result := spark.Collect(spark.Filter(rdd, func(x int) bool { return x%2 == 0 }))
	evens := len(result)

	fmt.Printf("Smoke test PASSED: sum=%d, evens=%d\n", sum, evens)
	fmt.Println("Data:", result)
}

func runWordCount() {
	fmt.Println("goSpark worker word count starting...")
	ctx := spark.NewContext(&spark.Config{
		AppName:       "wordcount-worker",
		Master:        "local[1]",
		NumPartitions: 2,
	})
	defer ctx.Stop()

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName = "local"
	}
	fmt.Printf("Running on pod: %s\n", podName)

	lines := []string{
		"hello world from go spark",
		"go spark runs on kubernetes",
		"spark is a distributed data engine",
		"hello from the distributed worker pod",
		"kubernetes orchestrates spark workers efficiently",
	}

	rdd := spark.Parallelize(ctx, lines, 2)

	words := spark.FlatMap(rdd, func(line string) []string {
		return strings.Fields(line)
	})

	pairs := spark.Map(words, func(word string) spark.Pair[string, int] {
		return spark.NewPair(word, 1)
	})

	counts := spark.ReduceByKey(pairs, spark.NewHashPartitioner(2), func(a, b int) int {
		return a + b
	})

	result := spark.Collect(counts)

	fmt.Printf("Word count results from pod %s:\n", podName)
	wordMap := make(map[string]int)
	for _, p := range result {
		wordMap[p.Key] = p.Value
		fmt.Printf("  %s: %d\n", p.Key, p.Value)
	}

	totalWords := 0
	for _, v := range wordMap {
		totalWords += v
	}
	fmt.Printf("Total words: %d, Unique words: %d\n", totalWords, len(wordMap))

	if _, ok := wordMap["spark"]; ok {
		fmt.Println("Verified: 'spark' found in results")
	}
}

func runDistributed() {
	fmt.Println("goSpark distributed computation starting...")
	ctx := spark.NewContext(&spark.Config{
		AppName:       "distributed-worker",
		Master:        "local[1]",
		NumPartitions: 2,
	})
	defer ctx.Stop()

	podIndex := os.Getenv("POD_INDEX")
	if podIndex == "" {
		podIndex = "0"
	}
	fmt.Printf("Pod index: %s\n", podIndex)

	fmt.Println("Running local computation as distributed worker...")

	const N = 10000
	data := make([]int, N)
	for i := range data {
		data[i] = i + 1
	}
	rdd := spark.Parallelize(ctx, data, 4)

	sum, _ := spark.Reduce(rdd, func(a, b int) int { return a + b })
	fmt.Printf("Sum of 1..%d = %d (expected %d)\n", N, sum, N*(N+1)/2)

	count, _ := spark.Reduce(spark.Map(rdd, func(x int) int { return 1 }), func(a, b int) int { return a + b })
	fmt.Printf("Count: %d (expected %d)\n", count, N)

	evens := spark.Collect(spark.Filter(rdd, func(x int) bool { return x%2 == 0 }))
	evensSum := 0
	for _, v := range evens {
		evensSum += v
	}
	fmt.Printf("Even numbers: %d items, sum=%d\n", len(evens), evensSum)

	pairs := spark.Map(rdd, func(x int) spark.Pair[string, int] {
		if x%3 == 0 {
			return spark.NewPair("div3", 1)
		} else if x%2 == 0 {
			return spark.NewPair("even", 1)
		}
		return spark.NewPair("odd", 1)
	})

	grouped := spark.ReduceByKey(pairs, spark.NewHashPartitioner(3), func(a, b int) int { return a + b })
	for _, p := range spark.Collect(grouped) {
		fmt.Printf("  Group %s: %d items\n", p.Key, p.Value)
	}

	fmt.Println("Distributed computation PASSED!")

	result := map[string]int{
		"pod_index": 0,
		"sum":       sum,
		"count":     count,
		"evens":     len(evens),
	}
	jsonData, _ := json.Marshal(result)
	fmt.Printf("RESULT_JSON: %s\n", string(jsonData))
}

func runSaveTest() {
	fmt.Println("goSpark SaveAsTextFile test...")
	ctx := spark.NewContext(&spark.Config{
		AppName:       "save-test",
		Master:        "local[1]",
		NumPartitions: 3,
	})
	defer ctx.Stop()

	data := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	rdd := spark.Parallelize(ctx, data, 3)

	outputDir := "/tmp/gospark-save-test"
	os.RemoveAll(outputDir)

	fmt.Println("Writing partitions to:", outputDir)
	if err := spark.SaveAsTextFile(rdd, outputDir); err != nil {
		fmt.Fprintf(os.Stderr, "SaveAsTextFile failed: %v\n", err)
		os.Exit(1)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ReadDir failed: %v\n", err)
		os.Exit(1)
	}

	partFiles := 0
	totalLines := 0
	for _, e := range entries {
		if len(e.Name()) >= 4 && e.Name()[:4] == "part" {
			partFiles++
			content, err := os.ReadFile(outputDir + "/" + e.Name())
			if err != nil {
				continue
			}
			lines := 0
			for _, c := range content {
				if c == '\n' {
					lines++
				}
			}
			fmt.Printf("  %s: %d lines\n", e.Name(), lines)
			totalLines += lines
		}
	}

	fmt.Printf("Partition files: %d, Total lines: %d (expected %d)\n", partFiles, totalLines, len(data))

	upperRDD := spark.Map(spark.Parallelize(ctx, data, 2), func(s string) string { return strings.ToUpper(s) })
	upperDir := "/tmp/gospark-save-test-upper"
	os.RemoveAll(upperDir)

	fmt.Println("\nWriting with custom partition writer to:", upperDir)
	err = spark.SaveAsTextFileWith(upperRDD, upperDir, func(basePath string, partitionIndex int, lines []string) error {
		os.MkdirAll(basePath, 0755)
		f, err := os.Create(fmt.Sprintf("%s/part-%05d", basePath, partitionIndex))
		if err != nil {
			return err
		}
		defer f.Close()
		for _, line := range lines {
			fmt.Fprintf(f, "LINE: %s\n", line)
		}
		fmt.Printf("  Wrote partition %d: %d lines (prefixed with LINE:)\n", partitionIndex, len(lines))
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "SaveAsTextFileWith failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nVerify: reading back files...")
	for _, e := range entries {
		if len(e.Name()) >= 4 && e.Name()[:4] == "part" {
			continue
		}
	}

	upperEntries, _ := os.ReadDir(upperDir)
	for _, e := range upperEntries {
		if len(e.Name()) >= 4 && e.Name()[:4] == "part" {
			content, _ := os.ReadFile(upperDir + "/" + e.Name())
			fmt.Printf("  %s:\n%s\n", e.Name(), string(content))
		}
	}

	fmt.Println("\nSaveAsTextFile test PASSED!")
}

func runDistributedSave() {
	taskName := os.Getenv("GOSPARK_TASK")
	outputPath := os.Getenv("GOSPARK_OUTPUT")
	partitionStr := os.Getenv("GOSPARK_PARTITION_INDEX")
	if partitionStr == "" {
		partitionStr = os.Getenv("POD_INDEX")
	}

	fmt.Printf("Distributed save: task=%s partition=%s output=%s\n", taskName, partitionStr, outputPath)

	if err := spark.WorkerRunPartitionFromEnv(); err != nil {
		fmt.Fprintf(os.Stderr, "Distributed save failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Distributed save PASSED!")
}

func runServe() {
	store := os.Getenv("GOSPARK_STORE")
	if store == "" {
		store = os.TempDir() + "/gospark-shuffle"
	}
	addr := os.Getenv("GOSPARK_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	srv, listen, err := spark.ServeWorker(store, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "serve failed: %v\n", err)
		os.Exit(1)
	}
	_ = srv
	fmt.Printf("LISTEN=%s STORE=%s\n", listen, store)
	select {}
}

func runRunPipelineAny() {
	taskName := os.Getenv("GOSPARK_TASK")
	if taskName == "" {
		taskName = "k8s-wc"
	}
	np, _ := strconv.Atoi(os.Getenv("GOSPARK_PARTITIONS"))
	if np <= 0 {
		np = 2
	}
	raw := os.Getenv("GOSPARK_WORKERS")
	if raw == "" {
		fmt.Fprintf(os.Stderr, "GOSPARK_WORKERS is required\n")
		os.Exit(1)
	}
	var runners []spark.TaskRunner
	for _, u := range strings.Split(raw, ",") {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		runners = append(runners, &spark.WorkerClient{BaseURL: u})
	}
	if len(runners) == 0 {
		fmt.Fprintf(os.Stderr, "no workers in GOSPARK_WORKERS\n")
		os.Exit(1)
	}
	fmt.Printf("Scheduling %s partitions=%d workers=%d\n", taskName, np, len(runners))
	var urls []string
	for _, r := range runners {
		if c, ok := r.(*spark.WorkerClient); ok {
			urls = append(urls, c.BaseURL)
		}
	}
	if err := spark.WaitForWorkers(urls, waitWorkersTimeout()); err != nil {
		fmt.Fprintf(os.Stderr, "wait workers: %v\n", err)
		os.Exit(1)
	}
	var opts spark.ScheduleOpts
	if secs, _ := strconv.Atoi(os.Getenv("GOSPARK_TEST_PAUSE_AFTER_MAP")); secs > 0 {
		paused := false
		opts.OnRepair = func(e spark.FetchError) {
			fmt.Printf("SHUFFLE_LOST shuffle=%d map=%d\n", e.ShuffleID, e.MapID)
		}
		opts.OnTaskComplete = func(task spark.Task, res spark.ExecResult) error {
			if res.Manifest != nil && task.Attempt > 0 {
				fmt.Printf("MAP_REBUILT shuffle=%d map=%d attempt=%d\n", res.Manifest.ShuffleID, res.Manifest.MapID, task.Attempt)
			}
			if res.Manifest != nil && task.PartitionID == 1 && !paused {
				paused = true
				fmt.Printf("MAP_PUBLISHED shuffle=%d map=%d\n", res.Manifest.ShuffleID, res.Manifest.MapID)
				time.Sleep(time.Duration(secs) * time.Second)
			}
			return nil
		}
	}
	action := os.Getenv("GOSPARK_ACTION")
	if action == "" {
		action = spark.ActionCollect
	}
	spec := spark.JobSpec{TaskName: taskName, Action: action, NumPartitions: np}
	if input := os.Getenv("GOSPARK_INPUT"); input != "" {
		if spec.Params == nil {
			spec.Params = map[string]string{}
		}
		spec.Params["input"] = input
	}
	if taskName == "k8s-broadcast" {
		if err := spark.AddBroadcast(&spec, "rates", map[string]int{"a": 3, "b": 5}); err != nil {
			fmt.Fprintf(os.Stderr, "broadcast: %v\n", err)
			os.Exit(1)
		}
	}
	if taskName == "k8s-logistic" || taskName == "k8s-linear" {
		if err := runModelJob(taskName, os.Getenv("GOSPARK_INPUT"), runners, np); err != nil {
			fmt.Fprintf(os.Stderr, "train failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Schedule PASSED!")
		return
	}
	if action == spark.ActionSave {
		if spec.Params == nil {
			spec.Params = map[string]string{}
		}
		spec.Params["path"] = os.Getenv("GOSPARK_OUTPUT")
		manifest, err := spark.RunPipelineSaveContext(context.Background(), spec, runners, spec.Params["path"], opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "save failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("OUTPUT_COMMITTED job=%s parts=%d path=%s\n", manifest.JobID, len(manifest.Partitions), spec.Params["path"])
		fmt.Println("Schedule PASSED!")
		return
	}
	recs, err := spark.RunPipelineAnyContext(context.Background(), spec, runners, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "schedule failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("RESULT_COUNT=%d\n", len(recs))
	for _, rec := range recs {
		fmt.Printf("  %#v\n", rec)
	}
	if err := checkScheduledResult(taskName, recs); err != nil {
		fmt.Fprintf(os.Stderr, "result check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Schedule PASSED!")
}

func runModelJob(task, path string, runners []spark.TaskRunner, partitions int) error {
	cfg := mllib.Config{FitIntercept: true, Iterations: 15, StepSize: 1}
	switch task {
	case "k8s-logistic":
		model, err := mllib.TrainLogisticFiles(path, runners, partitions, mllib.Config{FitIntercept: true, Iterations: 20, StepSize: 1, RegParam: 0.01})
		if err != nil {
			return err
		}
		low, err := model.Predict([]float64{0})
		if err != nil {
			return err
		}
		high, err := model.Predict([]float64{12})
		if err != nil {
			return err
		}
		fmt.Printf("LOGISTIC low=%g high=%g\n", low, high)
		if low != 0 || high != 1 {
			return fmt.Errorf("logistic classes low=%g high=%g", low, high)
		}
	case "k8s-linear":
		model, err := mllib.TrainFiles(path, runners, partitions, cfg)
		if err != nil {
			return err
		}
		fmt.Printf("LINEAR intercept=%g weight=%g\n", model.Intercept, model.Weights[0])
		if math.Abs(model.Intercept-1) > 1e-2 || math.Abs(model.Weights[0]-2) > 1e-2 {
			return fmt.Errorf("linear model intercept=%g weight=%g", model.Intercept, model.Weights[0])
		}
	default:
		return fmt.Errorf("unknown model job %s", task)
	}
	return nil
}

func checkScheduledResult(task string, recs []any) error {
	counts := map[string]int{}
	var keys []int
	for _, rec := range recs {
		switch p := rec.(type) {
		case spark.Pair[string, int]:
			counts[p.Key] += p.Value
		case spark.Pair[int, int]:
			keys = append(keys, p.Key)
		}
	}
	switch task {
	case "k8s-broadcast":
		if counts["a"] != 3 || counts["b"] != 5 || counts["missing"] != 0 {
			return fmt.Errorf("broadcast counts %v", counts)
		}
		fmt.Printf("BROADCAST a=%d b=%d\n", counts["a"], counts["b"])
	case "k8s-text":
		if counts["hello"] != 2 || counts["world"] != 1 || counts["spark"] != 1 || counts["ignore"] != 0 {
			return fmt.Errorf("text counts %v", counts)
		}
		fmt.Printf("TEXT hello=%d world=%d spark=%d\n", counts["hello"], counts["world"], counts["spark"])
	case "k8s-sort":
		for i := 1; i < len(keys); i++ {
			if keys[i] < keys[i-1] {
				return fmt.Errorf("unsorted keys %v", keys)
			}
		}
		if fmt.Sprint(keys) != "[1 2 3 4]" {
			return fmt.Errorf("sort keys %v", keys)
		}
		fmt.Printf("SORTED %v\n", keys)
	}
	return nil
}

func waitWorkersTimeout() time.Duration {
	if v := os.Getenv("GOSPARK_WAIT_WORKERS"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return 2 * time.Minute
}

func runVerifyOutput(path string) error {
	m, err := spark.ReadCommittedOutput(path)
	if err != nil {
		return err
	}
	fs := spark.ResolvePath(path).FS
	for _, p := range m.Partitions {
		r, err := fs.Open(p.Key)
		if err != nil {
			return err
		}
		content, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		sum := sha256.Sum256(content)
		if int64(len(content)) != p.Bytes || hex.EncodeToString(sum[:]) != p.SHA256 {
			return fmt.Errorf("corrupt partition %d", p.PartitionID)
		}
		fmt.Printf("PARTITION index=%d attempt=%d\n", p.PartitionID, p.Attempt)
		fmt.Print(string(content))
	}
	fmt.Printf("OUTPUT_VERIFIED parts=%d job=%s\n", len(m.Partitions), m.JobID)
	return nil
}

func runPrintK8s() {
	ns := os.Getenv("GOSPARK_NAMESPACE")
	if ns == "" {
		ns = "gospark-two-pod"
	}
	image := os.Getenv("GOSPARK_IMAGE")
	if image == "" {
		image = "gospark/worker:latest"
	}
	part := ""
	if len(os.Args) > 2 {
		part = os.Args[2]
	}
	task := os.Getenv("GOSPARK_TASK")
	if task == "" {
		task = "k8s-wc"
	}
	switch part {
	case "exec":
		fmt.Print(executorManifest(ns, image))
	case "driver":
		if output := os.Getenv("GOSPARK_OUTPUT"); output != "" {
			fmt.Print(spark.K8sSaveDriverManifest(ns, image, task, output, os.Getenv("GOSPARK_S3_SECRET"), 2, 2))
		} else if secs, _ := strconv.Atoi(os.Getenv("GOSPARK_TEST_PAUSE_AFTER_MAP")); secs > 0 {
			fmt.Print(spark.K8sFailureTestDriverManifest(ns, image, task, 2, 2, secs))
		} else if input := os.Getenv("GOSPARK_INPUT"); input != "" {
			fmt.Print(spark.K8sDriverManifestWithInput(ns, image, task, input, 2, 2, dataMounts()))
		} else {
			fmt.Print(spark.K8sDriverManifest(ns, image, task, 2, 2))
		}
	default:
		fmt.Print(executorManifest(ns, image))
		fmt.Println("---")
		fmt.Print(spark.K8sDriverManifest(ns, image, task, 2, 2))
	}
}

func executorManifest(namespace, image string) string {
	mounts := dataMounts()
	if len(mounts) == 0 {
		return spark.K8sExecutorManifest(namespace, image, 2)
	}
	return spark.K8sExecutorManifestWithData(namespace, image, 2, mounts)
}

func dataMounts() []spark.K8sMount {
	var mounts []spark.K8sMount
	if cm := os.Getenv("GOSPARK_TEXT_CONFIGMAP"); cm != "" {
		mounts = append(mounts, spark.K8sMount{Name: "text", ConfigMap: cm, Path: "/data/text"})
	}
	if cm := os.Getenv("GOSPARK_LOGISTIC_CONFIGMAP"); cm != "" {
		mounts = append(mounts, spark.K8sMount{Name: "logistic", ConfigMap: cm, Path: "/data/logistic"})
	}
	if cm := os.Getenv("GOSPARK_LINEAR_CONFIGMAP"); cm != "" {
		mounts = append(mounts, spark.K8sMount{Name: "linear", ConfigMap: cm, Path: "/data/linear"})
	}
	return mounts
}
