package main

import (
	"fmt"
	"os"
	"strings"

	spark "goSpark"
)

func init() {
	spark.RegisterPipeline("example-wordcount", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		lines := spark.Parallelize(ctx, []string{
			"hello world",
			"hello spark",
			"world is great",
			"spark is awesome",
		}, spec.NumPartitions)
		words := spark.FlatMap(lines, func(line string) []string {
			return strings.Split(line, " ")
		})
		pairs := spark.Map(words, func(word string) spark.Pair[string, int] {
			return spark.NewPair(word, 1)
		})
		return spark.ReduceByKey(pairs, spark.NewHashPartitioner(spec.NumPartitions), func(a, b int) int {
			return a + b
		}), nil
	})
	spark.RegisterPipeline("example-words", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[string], error) {
		lines := spark.Parallelize(ctx, []string{
			"hello world",
			"hello spark",
			"world is great",
			"spark is awesome",
		}, spec.NumPartitions)
		return spark.FlatMap(lines, func(line string) []string {
			return strings.Split(line, " ")
		}), nil
	})
}

func main() {
	spec := spark.JobSpec{TaskName: "example-wordcount", NumPartitions: 2}
	result, err := spark.RunPipelineLocal[spark.Pair[string, int]](spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Word counts:")
	for _, p := range result {
		fmt.Printf("  %s: %d\n", p.Key, p.Value)
	}
	words, err := spark.RunPipelineLocal[string](spark.JobSpec{TaskName: "example-words", NumPartitions: 2})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("\nTotal words: %d\n", len(words))
	if len(words) > 0 {
		fmt.Printf("First word: %s\n", words[0])
	}
}
