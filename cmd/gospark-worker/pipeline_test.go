package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	spark "goSpark"
)

func TestBundledPipelines(t *testing.T) {
	if !spark.IsPipeline("k8s-wc") || !spark.IsPipeline("k8s-join") || !spark.IsPipeline("k8s-broadcast") || !spark.IsPipeline("k8s-text") || !spark.IsPipeline("k8s-sort") {
		t.Fatal("bundled job was not registered as a pipeline")
	}
	var workers []spark.TaskRunner
	for range 2 {
		srv, addr, err := spark.ServeWorker(t.TempDir(), "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = srv.Close() })
		workers = append(workers, &spark.WorkerClient{BaseURL: "http://" + addr})
	}
	for _, task := range []string{"k8s-wc", "k8s-join"} {
		records, err := spark.RunPipelineAny(spark.JobSpec{TaskName: task, NumPartitions: 2}, workers)
		if err != nil || len(records) != 3 {
			t.Fatalf("%s returned %v, err=%v", task, records, err)
		}
	}
	spec := spark.JobSpec{TaskName: "k8s-broadcast", NumPartitions: 2}
	if err := spark.AddBroadcast(&spec, "rates", map[string]int{"a": 3, "b": 5}); err != nil {
		t.Fatal(err)
	}
	broadcast, err := spark.RunPipeline[spark.Pair[string, int]](spec, workers)
	if err != nil {
		t.Fatal(err)
	}
	if len(broadcast) != 3 || broadcast[0].Value != 3 || broadcast[1].Value != 5 {
		t.Fatalf("broadcast result %v", broadcast)
	}
	sorted, err := spark.RunPipeline[spark.Pair[int, int]](spark.JobSpec{TaskName: "k8s-sort", NumPartitions: 2}, workers)
	if err != nil {
		t.Fatal(err)
	}
	var keys []int
	for _, p := range sorted {
		keys = append(keys, p.Key)
	}
	if !reflect.DeepEqual(keys, []int{1, 2, 3, 4}) {
		t.Fatalf("sorted keys %v", keys)
	}
	dir := t.TempDir()
	for filename, body := range map[string]string{"one.txt": "hello world\n", "two.txt": "hello spark\n"} {
		if err := os.WriteFile(filepath.Join(dir, filename), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	text, err := spark.RunPipeline[spark.Pair[string, int]](spark.JobSpec{TaskName: "k8s-text", NumPartitions: 2,
		Params: map[string]string{"input": dir}}, workers)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, p := range text {
		counts[p.Key] = p.Value
	}
	if !reflect.DeepEqual(counts, map[string]int{"hello": 2, "world": 1, "spark": 1}) {
		t.Fatalf("text counts %v", counts)
	}
}
