package main

import (
	"fmt"
	"math"
	"math/rand"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "PageRank",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	fmt.Println("=== PageRank Algorithm ===")
	fmt.Println()

	edges := []spark.Pair[string, string]{
		spark.NewPair("A", "B"),
		spark.NewPair("A", "C"),
		spark.NewPair("B", "C"),
		spark.NewPair("C", "A"),
		spark.NewPair("D", "C"),
	}

	links := spark.Parallelize(ctx, edges, 2)

	outDegree := spark.ReduceByKey(links, spark.NewHashPartitioner(4), func(a, b string) string { return a })

	outDegreeMap := make(map[string]int)
	for _, p := range spark.Collect(outDegree) {
		outDegreeMap[p.Key]++
	}

	allPages := spark.Distinct(spark.Union(
		spark.Keys(links),
		spark.Values(links),
	), 4)
	allPagesList := spark.Collect(allPages)
	n := len(allPagesList)

	fmt.Printf("Graph: %d pages, %d edges\n", n, len(edges))
	for _, page := range allPagesList {
		fmt.Printf("  %s -> out-degree: %d\n", page, outDegreeMap[page])
	}
	fmt.Println()

	rank := 1.0 / float64(n)
	ranks := make(map[string]float64)
	for _, page := range allPagesList {
		ranks[page] = rank
	}

	dampingFactor := 0.85
	numIterations := 10

	linkPairs := spark.GroupByKey(links, spark.NewHashPartitioner(4))

	adjacency := make(map[string][]string)
	for _, p := range spark.Collect(linkPairs) {
		adjacency[p.Key] = p.Value
	}

	fmt.Println("Running PageRank iterations...")
	for i := 0; i < numIterations; i++ {
		contributions := make(map[string]float64)
		for page, neighbors := range adjacency {
			numNeighbors := len(neighbors)
			if numNeighbors > 0 {
				contrib := ranks[page] / float64(numNeighbors)
				for _, neighbor := range neighbors {
					contributions[neighbor] += contrib
				}
			}
		}

		for _, page := range allPagesList {
			contrib := contributions[page]
			ranks[page] = (1-dampingFactor)/float64(n) + dampingFactor*contrib
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

	const numSamples = 100000
	rng := rand.New(rand.NewSource(42))
	samples := make([]spark.Pair[int, bool], numSamples)
	for i := 0; i < numSamples; i++ {
		x := rng.Float64() - 0.5
		y := rng.Float64() - 0.5
		inside := (x*x + y*y) <= 0.25
		samples[i] = spark.NewPair(i, inside)
	}

	samplesRDD := spark.Parallelize(ctx, samples, 4)

	insideCount := spark.ReduceByKey(
		spark.Map(samplesRDD, func(p spark.Pair[int, bool]) spark.Pair[string, int] {
			if p.Value {
				return spark.NewPair("inside", 1)
			}
			return spark.NewPair("outside", 1)
		}),
		spark.NewHashPartitioner(2),
		func(a, b int) int { return a + b },
	)

	counts := spark.Collect(insideCount)
	var inside, total int64
	for _, p := range counts {
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

	_ = outDegree
}
