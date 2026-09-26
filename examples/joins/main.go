package main

import (
	"fmt"
	"os"

	spark "goSpark"
)

func init() {
	users := []spark.Pair[int, string]{
		spark.NewPair(1, "Alice"), spark.NewPair(2, "Bob"), spark.NewPair(3, "Carol"),
		spark.NewPair(4, "Dave"), spark.NewPair(5, "Eve"),
	}
	orders := []spark.Pair[int, float64]{
		spark.NewPair(1, 120.50), spark.NewPair(2, 85.00), spark.NewPair(1, 230.00),
		spark.NewPair(3, 45.99), spark.NewPair(2, 199.99), spark.NewPair(5, 500.00),
		spark.NewPair(4, 75.00), spark.NewPair(3, 150.00), spark.NewPair(5, 320.50),
		spark.NewPair(1, 89.99),
	}
	categories := []spark.Pair[int, string]{
		spark.NewPair(1, "Electronics"), spark.NewPair(2, "Books"), spark.NewPair(3, "Clothing"),
		spark.NewPair(4, "Home"), spark.NewPair(5, "Sports"),
	}
	partitioner := spark.NewHashPartitioner(3)
	spark.RegisterPipeline("example-join", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[int, spark.Pair[string, float64]]], error) {
		return spark.Join(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, orders, 2), partitioner), nil
	})
	spark.RegisterPipeline("example-left-join", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[int, spark.Pair[string, *float64]]], error) {
		return spark.LeftOuterJoin(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, orders, 2), partitioner), nil
	})
	spark.RegisterPipeline("example-cogroup", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[int, spark.Pair[[]string, []string]]], error) {
		return spark.Cogroup(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, categories, 2), partitioner), nil
	})
	spark.RegisterPipeline("example-spend", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[int, float64]], error) {
		return spark.ReduceByKey(spark.Parallelize(ctx, orders, 2), partitioner, func(a, b float64) float64 { return a + b }), nil
	})
	spark.RegisterPipeline("example-intersection", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return spark.Intersection(spark.Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2), spark.Parallelize(ctx, []int{3, 4, 5, 6, 7}, 2)), nil
	})
	spark.RegisterPipeline("example-subtract", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return spark.Subtract(spark.Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2), spark.Parallelize(ctx, []int{3, 4, 5, 6, 7}, 2)), nil
	})
	spark.RegisterPipeline("example-union", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		return spark.Union(spark.Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2), spark.Parallelize(ctx, []int{3, 4, 5, 6, 7}, 2)), nil
	})
	spark.RegisterPipeline("example-distinct", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		union := spark.Union(spark.Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2), spark.Parallelize(ctx, []int{3, 4, 5, 6, 7}, 2))
		return spark.Distinct(union, 3), nil
	})
	spark.RegisterPipeline("example-cartesian", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.Cartesian(spark.Parallelize(ctx, []string{"A", "B"}, 1), spark.Parallelize(ctx, []int{1, 2, 3}, 1)), nil
	})
	spark.RegisterPipeline("example-zip", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.Zip(spark.Parallelize(ctx, []string{"alpha", "beta", "gamma"}, 1), spark.Parallelize(ctx, []int{1, 2, 3}, 1)), nil
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
	fmt.Println("=== Data Enrichment with Joins ===")
	fmt.Println()
	fmt.Println("--- Inner Join: Users x Orders ---")
	for _, p := range run[spark.Pair[int, spark.Pair[string, float64]]]("example-join") {
		fmt.Printf("  User %s (id=%d): $%.2f\n", p.Value.Key, p.Key, p.Value.Value)
	}
	fmt.Println()
	fmt.Println("--- Left Outer Join: Users x Orders ---")
	for _, p := range run[spark.Pair[int, spark.Pair[string, *float64]]]("example-left-join") {
		if p.Value.Value != nil {
			fmt.Printf("  User %s (id=%d): $%.2f\n", p.Value.Key, p.Key, *p.Value.Value)
		} else {
			fmt.Printf("  User %s (id=%d): no orders\n", p.Value.Key, p.Key)
		}
	}
	fmt.Println()
	fmt.Println("--- Cogroup: Users + Categories by ID ---")
	for _, p := range run[spark.Pair[int, spark.Pair[[]string, []string]]]("example-cogroup") {
		fmt.Printf("  ID %d: users=%v, categories=%v\n", p.Key, p.Value.Key, p.Value.Value)
	}
	fmt.Println()
	fmt.Println("--- Total spend per user ---")
	for _, p := range run[spark.Pair[int, float64]]("example-spend") {
		fmt.Printf("  User id=%d: $%.2f\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Set Operations ---")
	fmt.Printf("  Intersection: %v\n", run[int]("example-intersection"))
	fmt.Printf("  Subtract(1,2): %v\n", run[int]("example-subtract"))
	fmt.Printf("  Union (with duplicates): %d elements\n", len(run[int]("example-union")))
	fmt.Printf("  Distinct union: %v\n", run[int]("example-distinct"))
	fmt.Println()
	fmt.Println("--- Cartesian Product ---")
	for _, p := range run[spark.Pair[string, int]]("example-cartesian") {
		fmt.Printf("  (%s, %d)\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Zip ---")
	for _, p := range run[spark.Pair[string, int]]("example-zip") {
		fmt.Printf("  %s -> %d\n", p.Key, p.Value)
	}
}
