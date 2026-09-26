package main

import (
	"fmt"
	"os"
	"strings"

	spark "goSpark"
)

func transactions() []string {
	return []string{
		"alice,laptop,1200,2024-01", "bob,phone,800,2024-01", "alice,keyboard,150,2024-01",
		"carol,laptop,1200,2024-02", "bob,monitor,400,2024-02", "alice,mouse,50,2024-02",
		"carol,phone,800,2024-03", "bob,laptop,1200,2024-03", "alice,monitor,400,2024-03",
		"dave,phone,800,2024-01", "dave,keyboard,150,2024-02", "carol,mouse,50,2024-03",
		"eve,laptop,1200,2024-01", "eve,phone,800,2024-02", "eve,monitor,400,2024-03",
	}
}

func init() {
	spark.RegisterPipeline("example-sales-customer", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, spark.Pair[string, int]]], error) {
		parsed := spark.Map(spark.Parallelize(ctx, transactions(), 3), func(line string) spark.Pair[string, spark.Pair[string, int]] {
			parts := strings.Split(line, ",")
			amount := 0
			fmt.Sscanf(parts[2], "%d", &amount)
			return spark.NewPair(parts[0], spark.NewPair(parts[1]+"@"+parts[3], amount))
		})
		return spark.ReduceByKey(parsed, spark.NewHashPartitioner(4), func(a, b spark.Pair[string, int]) spark.Pair[string, int] {
			return spark.NewPair(a.Key, a.Value+b.Value)
		}), nil
	})
	spark.RegisterPipeline("example-sales-month", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.ReduceByKey(spark.FlatMap(spark.Parallelize(ctx, transactions(), 3), func(line string) []spark.Pair[string, int] {
			parts := strings.Split(line, ",")
			amount := 0
			fmt.Sscanf(parts[2], "%d", &amount)
			return []spark.Pair[string, int]{spark.NewPair(parts[3], amount)}
		}), spark.NewHashPartitioner(3), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-sales-product", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.ReduceByKey(spark.FlatMap(spark.Parallelize(ctx, transactions(), 3), func(line string) []spark.Pair[string, int] {
			parts := strings.Split(line, ",")
			amount := 0
			fmt.Sscanf(parts[2], "%d", &amount)
			return []spark.Pair[string, int]{spark.NewPair(parts[1], amount)}
		}), spark.NewHashPartitioner(4), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-sales-all", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[string], error) {
		rows := append(transactions(), transactions()[0], transactions()[3])
		return spark.Parallelize(ctx, rows, 3), nil
	})
	spark.RegisterPipeline("example-sales-unique", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[string], error) {
		rows := append(transactions(), transactions()[0], transactions()[3])
		return spark.Distinct(spark.Parallelize(ctx, rows, 3), 4), nil
	})
	spark.RegisterPipeline("example-sales-avg", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		productData := spark.FlatMap(spark.Parallelize(ctx, transactions(), 3), func(line string) []spark.Pair[string, int] {
			parts := strings.Split(line, ",")
			amount := 0
			fmt.Sscanf(parts[2], "%d", &amount)
			return []spark.Pair[string, int]{spark.NewPair(parts[1], amount)}
		})
		return spark.MapValues(spark.ReduceByKey(spark.Map(productData, func(p spark.Pair[string, int]) spark.Pair[string, spark.Pair[int, int]] {
			return spark.NewPair(p.Key, spark.NewPair(p.Value, 1))
		}), spark.NewHashPartitioner(4), func(a, b spark.Pair[int, int]) spark.Pair[int, int] {
			return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
		}), func(p spark.Pair[int, int]) float64 {
			if p.Value == 0 {
				return 0
			}
			return float64(p.Key) / float64(p.Value)
		}), nil
	})
	spark.RegisterPipeline("example-sales-total", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[int], error) {
		monthly := spark.FlatMap(spark.Parallelize(ctx, transactions(), 3), func(line string) []spark.Pair[string, int] {
			parts := strings.Split(line, ",")
			amount := 0
			fmt.Sscanf(parts[2], "%d", &amount)
			return []spark.Pair[string, int]{spark.NewPair(parts[3], amount)}
		})
		total := spark.ReduceBroadcast(monthly, func(a, b spark.Pair[string, int]) spark.Pair[string, int] {
			return spark.NewPair("total", a.Value+b.Value)
		})
		return spark.Map(spark.Parallelize(ctx, []int{0}, 1), func(int) int { return total.Value().Value }), nil
	})
}

func run[T any](name string) []T {
	rows, err := spark.RunPipelineLocal[T](spark.JobSpec{TaskName: name, NumPartitions: 3})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return rows
}

func main() {
	fmt.Println("=== Sales Analytics Pipeline ===")
	fmt.Println()
	fmt.Println("--- Total spend per customer ---")
	for _, p := range run[spark.Pair[string, spark.Pair[string, int]]]("example-sales-customer") {
		fmt.Printf("  %s: %s=$%d\n", p.Key, p.Value.Key, p.Value.Value)
	}
	fmt.Println()
	fmt.Println("--- Monthly revenue ---")
	for _, p := range run[spark.Pair[string, int]]("example-sales-month") {
		fmt.Printf("  %s: $%d\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Revenue by product ---")
	for _, p := range run[spark.Pair[string, int]]("example-sales-product") {
		fmt.Printf("  %s: $%d\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Duplicate removal ---")
	fmt.Printf("  Total records: %d\n", len(run[string]("example-sales-all")))
	fmt.Printf("  Unique records: %d\n", len(run[string]("example-sales-unique")))
	fmt.Println()
	fmt.Println("--- Average price by product ---")
	for _, p := range run[spark.Pair[string, float64]]("example-sales-avg") {
		fmt.Printf("  %s: $%.2f\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Printf("=== Grand total revenue: $%d ===\n", run[int]("example-sales-total")[0])
}
