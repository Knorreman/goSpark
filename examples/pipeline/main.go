package main

import (
	"fmt"
	"os"
	"strings"

	spark "goSpark"
)

const jobName = "normalized-pipeline"

func init() {
	spark.RegisterPipeline(jobName, func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[float64], error) {
		rdd := spark.Parallelize(ctx, []float64{2, 3, 5}, spec.NumPartitions)
		totalBc := spark.ReduceBroadcast(rdd, func(a, b float64) float64 { return a + b })
		return spark.Map(rdd, func(x float64) float64 { return x / totalBc.Value() }), nil
	})
}

func main() {
	spec := spark.JobSpec{TaskName: jobName, NumPartitions: 2}
	var values []float64
	var err error
	switch {
	case len(os.Args) == 1 || os.Args[1] == "local":
		values, err = spark.RunPipelineLocal[float64](spec)
	case os.Args[1] == "serve":
		store := os.Getenv("GOSPARK_STORE")
		if store == "" {
			store = os.TempDir() + "/gospark-pipeline"
		}
		addr := os.Getenv("GOSPARK_LISTEN")
		if addr == "" {
			addr = "127.0.0.1:0"
		}
		_, listen, serveErr := spark.ServeWorker(store, addr)
		if serveErr != nil {
			fmt.Fprintln(os.Stderr, serveErr)
			os.Exit(1)
		}
		fmt.Println("LISTEN=" + listen)
		select {}
	case os.Args[1] == "run":
		var workers []spark.TaskRunner
		for _, address := range strings.Split(os.Getenv("GOSPARK_WORKERS"), ",") {
			if address != "" {
				workers = append(workers, &spark.WorkerClient{BaseURL: strings.TrimSpace(address)})
			}
		}
		values, err = spark.RunPipeline[float64](spec, workers)
	default:
		err = fmt.Errorf("usage: pipeline [local|serve|run]")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("normalized=%v\n", values)
}
