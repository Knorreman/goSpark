package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"

	spark "goSpark"
)

func init() {
	edges := []spark.Pair[string, string]{
		spark.NewPair("A", "B"), spark.NewPair("A", "C"), spark.NewPair("B", "C"),
		spark.NewPair("C", "A"), spark.NewPair("D", "C"),
	}
	spark.RegisterPipeline("example-rank-degree", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, string]], error) {
		links := spark.Parallelize(ctx, edges, 2)
		return spark.ReduceByKey(links, spark.NewHashPartitioner(4), func(a, b string) string { return a }), nil
	})
	spark.RegisterPipeline("example-rank-pages", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[string], error) {
		links := spark.Parallelize(ctx, edges, 2)
		return spark.Distinct(spark.Union(spark.Keys(links), spark.Values(links)), 4), nil
	})
	spark.RegisterPipeline("example-rank-links", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, []string]], error) {
		return spark.GroupByKey(spark.Parallelize(ctx, edges, 2), spark.NewHashPartitioner(4)), nil
	})
	spark.RegisterPipeline("example-rank-pi", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		const numSamples = 100000
		rng := rand.New(rand.NewSource(42))
		samples := make([]spark.Pair[int, bool], numSamples)
		for i := 0; i < numSamples; i++ {
			x := rng.Float64() - 0.5
			y := rng.Float64() - 0.5
			samples[i] = spark.NewPair(i, x*x+y*y <= 0.25)
		}
		return spark.ReduceByKey(spark.Map(spark.Parallelize(ctx, samples, 4), func(p spark.Pair[int, bool]) spark.Pair[string, int] {
			if p.Value {
				return spark.NewPair("inside", 1)
			}
			return spark.NewPair("outside", 1)
		}), spark.NewHashPartitioner(2), func(a, b int) int { return a + b }), nil
	})
}

func run[T any](name string) []T {
	rows, err := spark.RunPipelineLocal[T](spark.JobSpec{TaskName: name, NumPartitions: 2})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return rows
}

func main() {
	fmt.Println("=== PageRank Algorithm ===")
	fmt.Println()
	outDegreeMap := map[string]int{}
	for _, p := range run[spark.Pair[string, string]]("example-rank-degree") {
		outDegreeMap[p.Key]++
	}
	allPagesList := run[string]("example-rank-pages")
	n := len(allPagesList)
	fmt.Printf("Graph: %d pages, %d edges\n", n, 5)
	for _, page := range allPagesList {
		fmt.Printf("  %s -> out-degree: %d\n", page, outDegreeMap[page])
	}
	fmt.Println()
	rank := 1.0 / float64(n)
	ranks := map[string]float64{}
	for _, page := range allPagesList {
		ranks[page] = rank
	}
	adjacency := map[string][]string{}
	for _, p := range run[spark.Pair[string, []string]]("example-rank-links") {
		adjacency[p.Key] = p.Value
	}
	fmt.Println("Running PageRank iterations...")
	dampingFactor := 0.85
	for i := 0; i < 10; i++ {
		contributions := map[string]float64{}
		for page, neighbors := range adjacency {
			if len(neighbors) == 0 {
				continue
			}
			contrib := ranks[page] / float64(len(neighbors))
			for _, neighbor := range neighbors {
				contributions[neighbor] += contrib
			}
		}
		for _, page := range allPagesList {
			ranks[page] = (1-dampingFactor)/float64(n) + dampingFactor*contributions[page]
		}
		if (i+1)%5 == 0 {
			fmt.Printf("  After iteration %d:\n", i+1)
			for _, page := range allPagesList {
				fmt.Printf("    %s: %.6f\n", page, ranks[page])
			}
		}
	}
	fmt.Println()
	fmt.Println("Final PageRank scores:")
	var totalRank float64
	for _, page := range allPagesList {
		fmt.Printf("  %s: %.6f\n", page, ranks[page])
		totalRank += ranks[page]
	}
	fmt.Printf("\nTotal rank (should be ~1.0): %.6f\n", totalRank)
	fmt.Println()
	fmt.Println("=== Pi Estimation (Monte Carlo) ===")
	fmt.Println()
	var inside, total int64
	for _, p := range run[spark.Pair[string, int]]("example-rank-pi") {
		if p.Key == "inside" {
			inside = int64(p.Value)
		}
		total += int64(p.Value)
	}
	piEstimate := 4.0 * float64(inside) / float64(total)
	fmt.Printf("Samples: %d\n", total)
	fmt.Printf("Inside circle: %d\n", inside)
	fmt.Printf("Pi estimate: %.6f\n", piEstimate)
	fmt.Printf("Actual Pi:   %.6f\n", math.Pi)
	fmt.Printf("Error:       %.4f%%\n", math.Abs(piEstimate-math.Pi)/math.Pi*100)
}
