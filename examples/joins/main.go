package main

import (
	"fmt"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "DataEnrichmentJoin",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	fmt.Println("=== Data Enrichment with Joins ===")
	fmt.Println()

	users := []spark.Pair[int, string]{
		spark.NewPair(1, "Alice"),
		spark.NewPair(2, "Bob"),
		spark.NewPair(3, "Carol"),
		spark.NewPair(4, "Dave"),
		spark.NewPair(5, "Eve"),
	}

	orders := []spark.Pair[int, float64]{
		spark.NewPair(1, 120.50),
		spark.NewPair(2, 85.00),
		spark.NewPair(1, 230.00),
		spark.NewPair(3, 45.99),
		spark.NewPair(2, 199.99),
		spark.NewPair(5, 500.00),
		spark.NewPair(4, 75.00),
		spark.NewPair(3, 150.00),
		spark.NewPair(5, 320.50),
		spark.NewPair(1, 89.99),
	}

	categories := []spark.Pair[int, string]{
		spark.NewPair(1, "Electronics"),
		spark.NewPair(2, "Books"),
		spark.NewPair(3, "Clothing"),
		spark.NewPair(4, "Home"),
		spark.NewPair(5, "Sports"),
	}

	partitioner := spark.NewHashPartitioner(3)

	fmt.Println("--- Inner Join: Users x Orders ---")
	joined := spark.Join(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, orders, 2), partitioner)
	for _, p := range spark.Collect(joined) {
		fmt.Printf("  User %s (id=%d): $%.2f\n", p.Value.Key, p.Key, p.Value.Value)
	}

	fmt.Println()
	fmt.Println("--- Left Outer Join: Users x Orders ---")
	leftJoined := spark.LeftOuterJoin(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, orders, 2), partitioner)
	for _, p := range spark.Collect(leftJoined) {
		if p.Value.Value != nil {
			fmt.Printf("  User %s (id=%d): $%.2f\n", p.Value.Key, p.Key, *p.Value.Value)
		} else {
			fmt.Printf("  User %s (id=%d): no orders\n", p.Value.Key, p.Key)
		}
	}

	fmt.Println()
	fmt.Println("--- Cogroup: Users + Categories by ID ---")
	cogrouped := spark.Cogroup(spark.Parallelize(ctx, users, 2), spark.Parallelize(ctx, categories, 2), partitioner)
	for _, p := range spark.Collect(cogrouped) {
		var userNames []string
		for _, u := range p.Value.Key {
			userNames = append(userNames, u)
		}
		var catNames []string
		for _, c := range p.Value.Value {
			catNames = append(catNames, c)
		}
		fmt.Printf("  ID %d: users=%v, categories=%v\n", p.Key, userNames, catNames)
	}

	fmt.Println()
	fmt.Println("--- Total spend per user ---")
	totalByUser := spark.ReduceByKey(spark.Parallelize(ctx, orders, 2), partitioner, func(a, b float64) float64 { return a + b })
	for _, p := range spark.Collect(totalByUser) {
		fmt.Printf("  User id=%d: $%.2f\n", p.Key, p.Value)
	}

	fmt.Println()
	fmt.Println("--- Set Operations ---")
	rdd1 := spark.Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2)
	rdd2 := spark.Parallelize(ctx, []int{3, 4, 5, 6, 7}, 2)

	intersection := spark.Intersection(rdd1, rdd2)
	fmt.Printf("  Intersection: %v\n", spark.Collect(intersection))

	subtracted := spark.Subtract(rdd1, rdd2)
	fmt.Printf("  Subtract(1,2): %v\n", spark.Collect(subtracted))

	union := spark.Union(rdd1, rdd2)
	fmt.Printf("  Union (with duplicates): %d elements\n", spark.Count(union))

	distinct := spark.Distinct(union, 3)
	fmt.Printf("  Distinct union: %v\n", spark.Collect(distinct))

	fmt.Println()
	fmt.Println("--- Cartesian Product ---")
	small1 := spark.Parallelize(ctx, []string{"A", "B"}, 1)
	small2 := spark.Parallelize(ctx, []int{1, 2, 3}, 1)
	product := spark.Cartesian(small1, small2)
	for _, p := range spark.Collect(product) {
		fmt.Printf("  (%s, %d)\n", p.Key, p.Value)
	}

	fmt.Println()
	fmt.Println("--- Zip ---")
	letters := spark.Parallelize(ctx, []string{"alpha", "beta", "gamma"}, 1)
	nums := spark.Parallelize(ctx, []int{1, 2, 3}, 1)
	zipped := spark.Zip(letters, nums)
	for _, p := range spark.Collect(zipped) {
		fmt.Printf("  %s -> %d\n", p.Key, p.Value)
	}
}
