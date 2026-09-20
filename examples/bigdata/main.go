package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "BigDataOperations",
		Master:        "local[*]",
		NumPartitions: 8,
	})
	defer ctx.Stop()

	fmt.Println("=== Big Data Operations Demo ===")
	fmt.Println()

	const N = 100000
	data := make([]int, N)
	for i := 0; i < N; i++ {
		data[i] = i
	}
	rdd := spark.Parallelize(ctx, data, 8)

	evens := spark.Filter(rdd, func(x int) bool { return x%2 == 0 })
	evenCountInts := spark.Map(evens, func(x int) int { return 1 })
	evenSum, _ := spark.Reduce(evenCountInts, func(a, b int) int { return a + b })
	fmt.Printf("Even numbers in [0,%d]: %d\n", N-1, evenSum)

	squares := spark.Map(rdd, func(x int) int { return x * x })
	sumSquares, _ := spark.Reduce(squares, func(a, b int) int { return a + b })
	fmt.Printf("Sum of squares [0,%d]: %d\n", N-1, sumSquares)
	expectedSum := int64(N-1) * int64(N) * int64(2*N-1) / 6
	fmt.Printf("  Expected (formula): %d\n", expectedSum)

	cubes := spark.Map(rdd, func(x int) int { return x * x * x })
	cubeSum, _ := spark.Reduce(cubes, func(a, b int) int { return a + b })
	fmt.Printf("Sum of cubes [0,%d]: %d\n", N-1, cubeSum)

	minVal, _ := spark.Min(rdd, func(a, b int) bool { return a < b })
	maxVal, _ := spark.Max(rdd, func(a, b int) bool { return a < b })
	fmt.Printf("Range: [%d, %d]\n", minVal, maxVal)

	first10 := spark.Take(rdd, 10)
	fmt.Printf("First 10: %v\n", first10)

	numBuckets := 20
	histData := spark.Map(rdd, func(x int) spark.Pair[int, int] {
		bucket := x / (N / numBuckets)
		if bucket >= numBuckets {
			bucket = numBuckets - 1
		}
		return spark.NewPair(bucket, 1)
	})

	histogram := spark.ReduceByKey(histData, spark.NewHashPartitioner(8), func(a, b int) int { return a + b })
	bucketSize := N / numBuckets
	fmt.Println()
	fmt.Println("Histogram (20 buckets):")
	for _, p := range spark.Collect(histogram) {
		low := p.Key * bucketSize
		high := low + bucketSize - 1
		bar := strings.Repeat("#", p.Value/500)
		fmt.Printf("  [%5d-%5d]: %s (%d)\n", low, high, bar, p.Value)
	}

	fmt.Println()
	fmt.Println("--- String Analysis ---")
	words := []string{
		"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
		"the", "dog", "barked", "at", "the", "fox", "and", "the", "fox", "ran",
		"quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "again",
		"the", "lazy", "dog", "did", "not", "move", "the", "fox", "was", "too",
		"quick", "for", "the", "dog", "to", "catch", "but", "the", "dog", "tried",
	}
	wordRDD := spark.Parallelize(ctx, words, 4)

	wordCounts := spark.ReduceByKey(
		spark.Map(wordRDD, func(w string) spark.Pair[string, int] { return spark.NewPair(w, 1) }),
		spark.NewHashPartitioner(4),
		func(a, b int) int { return a + b },
	)

	fmt.Println("Word frequency (alphabetical):")
	for _, p := range spark.Collect(wordCounts) {
		bar := strings.Repeat("#", p.Value)
		fmt.Printf("  %-6s: %s (%d)\n", p.Key, bar, p.Value)
	}

	uniqueCount := spark.Count(spark.Distinct(wordRDD, 4))
	fmt.Printf("\nUnique words: %d\n", uniqueCount)

	fmt.Println()
	fmt.Println("--- Cartesian Product ---")
	small1 := spark.Parallelize(ctx, []string{"A", "B"}, 1)
	small2 := spark.Parallelize(ctx, []int{1, 2, 3}, 1)
	product := spark.Cartesian(small1, small2)
	fmt.Println("Cartesian product:")
	for _, p := range spark.Collect(product) {
		fmt.Printf("  (%s, %d)\n", p.Key, p.Value)
	}

	fmt.Println()
	fmt.Println("--- Zip ---")
	letters := spark.Parallelize(ctx, []string{"alpha", "beta", "gamma"}, 1)
	nums := spark.Parallelize(ctx, []int{1, 2, 3}, 1)
	zipped := spark.Zip(letters, nums)
	fmt.Println("Zip:")
	for _, p := range spark.Collect(zipped) {
		fmt.Printf("  %s -> %d\n", p.Key, p.Value)
	}

	fmt.Println()
	fmt.Println("--- Glom (partition contents) ---")
	data4 := make([]int, 12)
	for i := range data4 {
		data4[i] = i * 10
	}
	glommed := spark.Glom(spark.Parallelize(ctx, data4, 3))
	for idx, partition := range spark.Collect(glommed) {
		fmt.Printf("  Partition %d: %v\n", idx, partition)
	}

	fmt.Println()
	fmt.Println("--- Pi Estimate (100k samples) ---")
	sampleData := make([]spark.Pair[int, float64], 100000)
	rng := rand.New(rand.NewSource(42))
	for i := range sampleData {
		x := rng.Float64() - 0.5
		y := rng.Float64() - 0.5
		inside := 0.0
		if x*x+y*y <= 0.25 {
			inside = 1.0
		}
		sampleData[i] = spark.NewPair(i, inside)
	}

	insideSamples := spark.ReduceByKey(
		spark.Map(spark.Parallelize(ctx, sampleData, 4),
			func(p spark.Pair[int, float64]) spark.Pair[string, float64] {
				if p.Value > 0 {
					return spark.NewPair("inside", 1.0)
				}
				return spark.NewPair("outside", 1.0)
			}),
		spark.NewHashPartitioner(2),
		func(a, b float64) float64 { return a + b },
	)

	totalInside := 0.0
	totalAll := 0.0
	for _, p := range spark.Collect(insideSamples) {
		if p.Key == "inside" {
			totalInside = p.Value
		}
		totalAll += p.Value
	}
	piEstimate := 4.0 * totalInside / totalAll
	fmt.Printf("  Pi estimate: %.6f (actual: %.6f, error: %.4f%%)\n",
		piEstimate, math.Pi, math.Abs(piEstimate-math.Pi)/math.Pi*100)
}
