package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"

	spark "goSpark"
)

func numbers() []int {
	data := make([]int, 100000)
	for i := range data {
		data[i] = i
	}
	return data
}

func words() []string {
	return []string{
		"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
		"the", "dog", "barked", "at", "the", "fox", "and", "the", "fox", "ran",
		"quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog", "again",
		"the", "lazy", "dog", "did", "not", "move", "the", "fox", "was", "too",
		"quick", "for", "the", "dog", "to", "catch", "but", "the", "dog", "tried",
	}
}

func reduced[T any](ctx *spark.Context, rdd *spark.RDD[T], fn func(T, T) T) *spark.RDD[T] {
	total := spark.ReduceBroadcast(rdd, fn)
	return spark.Map(spark.Parallelize(ctx, []T{*new(T)}, 1), func(T) T { return total.Value() })
}

func init() {
	spark.RegisterPipeline("example-big-even", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		evens := spark.Map(spark.Filter(spark.Parallelize(ctx, numbers(), 8), func(x int) bool { return x%2 == 0 }), func(int) int { return 1 })
		return reduced(ctx, evens, func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-big-squares", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return reduced(ctx, spark.Map(spark.Parallelize(ctx, numbers(), 8), func(x int) int { return x * x }), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-big-cubes", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return reduced(ctx, spark.Map(spark.Parallelize(ctx, numbers(), 8), func(x int) int { return x * x * x }), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-big-range", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return spark.Parallelize(ctx, numbers(), 8), nil
	})
	spark.RegisterPipeline("example-big-hist", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[int, int]], error) {
		const n, buckets = 100000, 20
		return spark.ReduceByKey(spark.Map(spark.Parallelize(ctx, numbers(), 8), func(x int) spark.Pair[int, int] {
			bucket := x / (n / buckets)
			if bucket >= buckets {
				bucket = buckets - 1
			}
			return spark.NewPair(bucket, 1)
		}), spark.NewHashPartitioner(8), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-big-words", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.ReduceByKey(spark.Map(spark.Parallelize(ctx, words(), 4), func(w string) spark.Pair[string, int] {
			return spark.NewPair(w, 1)
		}), spark.NewHashPartitioner(4), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-big-unique", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[string], error) {
		return spark.Distinct(spark.Parallelize(ctx, words(), 4), 4), nil
	})
	spark.RegisterPipeline("example-big-cartesian", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.Cartesian(spark.Parallelize(ctx, []string{"A", "B"}, 1), spark.Parallelize(ctx, []int{1, 2, 3}, 1)), nil
	})
	spark.RegisterPipeline("example-big-zip", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.Zip(spark.Parallelize(ctx, []string{"alpha", "beta", "gamma"}, 1), spark.Parallelize(ctx, []int{1, 2, 3}, 1)), nil
	})
	spark.RegisterPipeline("example-big-glom", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[[]int], error) {
		data := make([]int, 12)
		for i := range data {
			data[i] = i * 10
		}
		return spark.Glom(spark.Parallelize(ctx, data, 3)), nil
	})
	spark.RegisterPipeline("example-big-pi", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		sampleData := make([]spark.Pair[int, float64], 100000)
		rng := rand.New(rand.NewSource(42))
		for i := range sampleData {
			x := rng.Float64() - 0.5
			y := rng.Float64() - 0.5
			inside := 0.0
			if x*x+y*y <= 0.25 {
				inside = 1
			}
			sampleData[i] = spark.NewPair(i, inside)
		}
		return spark.ReduceByKey(spark.Map(spark.Parallelize(ctx, sampleData, 4), func(p spark.Pair[int, float64]) spark.Pair[string, float64] {
			if p.Value > 0 {
				return spark.NewPair("inside", 1.0)
			}
			return spark.NewPair("outside", 1.0)
		}), spark.NewHashPartitioner(2), func(a, b float64) float64 { return a + b }), nil
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
	const n = 100000
	fmt.Println("=== Big Data Operations Demo ===")
	fmt.Println()
	fmt.Printf("Even numbers in [0,%d]: %d\n", n-1, run[int]("example-big-even")[0])
	fmt.Printf("Sum of squares [0,%d]: %d\n", n-1, run[int]("example-big-squares")[0])
	fmt.Printf("  Expected (formula): %d\n", int64(n-1)*int64(n)*int64(2*n-1)/6)
	fmt.Printf("Sum of cubes [0,%d]: %d\n", n-1, run[int]("example-big-cubes")[0])
	values := run[int]("example-big-range")
	minVal, maxVal := values[0], values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	fmt.Printf("Range: [%d, %d]\n", minVal, maxVal)
	fmt.Printf("First 10: %v\n", values[:10])
	fmt.Println()
	fmt.Println("Histogram (20 buckets):")
	for _, p := range run[spark.Pair[int, int]]("example-big-hist") {
		low := p.Key * (n / 20)
		fmt.Printf("  [%5d-%5d]: %s (%d)\n", low, low+n/20-1, strings.Repeat("#", p.Value/500), p.Value)
	}
	fmt.Println()
	fmt.Println("--- String Analysis ---")
	fmt.Println("Word frequency (alphabetical):")
	for _, p := range run[spark.Pair[string, int]]("example-big-words") {
		fmt.Printf("  %-6s: %s (%d)\n", p.Key, strings.Repeat("#", p.Value), p.Value)
	}
	fmt.Printf("\nUnique words: %d\n", len(run[string]("example-big-unique")))
	fmt.Println()
	fmt.Println("--- Cartesian Product ---")
	fmt.Println("Cartesian product:")
	for _, p := range run[spark.Pair[string, int]]("example-big-cartesian") {
		fmt.Printf("  (%s, %d)\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Zip ---")
	fmt.Println("Zip:")
	for _, p := range run[spark.Pair[string, int]]("example-big-zip") {
		fmt.Printf("  %s -> %d\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Glom (partition contents) ---")
	for idx, partition := range run[[]int]("example-big-glom") {
		fmt.Printf("  Partition %d: %v\n", idx, partition)
	}
	fmt.Println()
	fmt.Println("--- Pi Estimate (100k samples) ---")
	var inside, total float64
	for _, p := range run[spark.Pair[string, float64]]("example-big-pi") {
		if p.Key == "inside" {
			inside = p.Value
		}
		total += p.Value
	}
	piEstimate := 4 * inside / total
	fmt.Printf("  Pi estimate: %.6f (actual: %.6f, error: %.4f%%)\n", piEstimate, math.Pi, math.Abs(piEstimate-math.Pi)/math.Pi*100)
}
