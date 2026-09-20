package main

import (
	"fmt"
	"strings"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "SalesAnalytics",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	fmt.Println("=== Sales Analytics Pipeline ===")
	fmt.Println()

	transactions := []string{
		"alice,laptop,1200,2024-01",
		"bob,phone,800,2024-01",
		"alice,keyboard,150,2024-01",
		"carol,laptop,1200,2024-02",
		"bob,monitor,400,2024-02",
		"alice,mouse,50,2024-02",
		"carol,phone,800,2024-03",
		"bob,laptop,1200,2024-03",
		"alice,monitor,400,2024-03",
		"dave,phone,800,2024-01",
		"dave,keyboard,150,2024-02",
		"carol,mouse,50,2024-03",
		"eve,laptop,1200,2024-01",
		"eve,phone,800,2024-02",
		"eve,monitor,400,2024-03",
	}

	rawRDD := spark.Parallelize(ctx, transactions, 3)

	parsedRDD := spark.Map(rawRDD, func(line string) spark.Pair[string, spark.Pair[string, int]] {
		parts := strings.Split(line, ",")
		customer := parts[0]
		product := parts[1]
		amount := 0
		fmt.Sscanf(parts[2], "%d", &amount)
		month := parts[3]
		return spark.NewPair(customer, spark.NewPair(product+"@"+month, amount))
	})

	totalByCustomer := spark.ReduceByKey(parsedRDD, spark.NewHashPartitioner(4), func(a, b spark.Pair[string, int]) spark.Pair[string, int] {
		return spark.NewPair(a.Key, a.Value+b.Value)
	})

	fmt.Println("--- Total spend per customer ---")
	for _, p := range spark.Collect(totalByCustomer) {
		fmt.Printf("  %s: %s=$%d\n", p.Key, p.Value.Key, p.Value.Value)
	}

	monthlyData := spark.FlatMap(spark.Parallelize(ctx, transactions, 3), func(line string) []spark.Pair[string, int] {
		parts := strings.Split(line, ",")
		amount := 0
		fmt.Sscanf(parts[2], "%d", &amount)
		month := parts[3]
		return []spark.Pair[string, int]{spark.NewPair(month, amount)}
	})

	monthlyRevenue := spark.ReduceByKey(monthlyData, spark.NewHashPartitioner(3), func(a, b int) int {
		return a + b
	})

	fmt.Println()
	fmt.Println("--- Monthly revenue ---")
	for _, p := range spark.Collect(monthlyRevenue) {
		fmt.Printf("  %s: $%d\n", p.Key, p.Value)
	}

	productData := spark.FlatMap(spark.Parallelize(ctx, transactions, 3), func(line string) []spark.Pair[string, int] {
		parts := strings.Split(line, ",")
		product := parts[1]
		amount := 0
		fmt.Sscanf(parts[2], "%d", &amount)
		return []spark.Pair[string, int]{spark.NewPair(product, amount)}
	})

	productRevenue := spark.ReduceByKey(productData, spark.NewHashPartitioner(4), func(a, b int) int {
		return a + b
	})

	fmt.Println()
	fmt.Println("--- Revenue by product ---")
	for _, p := range spark.Collect(productRevenue) {
		fmt.Printf("  %s: $%d\n", p.Key, p.Value)
	}

	duplicateTransactions := append(transactions, transactions[0], transactions[3])
	allRDD := spark.Parallelize(ctx, duplicateTransactions, 3)
	uniqueRDD := spark.Distinct(allRDD, 4)

	fmt.Println()
	fmt.Println("--- Duplicate removal ---")
	fmt.Printf("  Total records: %d\n", spark.Count(allRDD))
	fmt.Printf("  Unique records: %d\n", spark.Count(uniqueRDD))

	avgByProduct := spark.MapValues(
		spark.ReduceByKey(
			spark.Map(productData, func(p spark.Pair[string, int]) spark.Pair[string, spark.Pair[int, int]] {
				return spark.NewPair(p.Key, spark.NewPair(p.Value, 1))
			}),
			spark.NewHashPartitioner(4),
			func(a, b spark.Pair[int, int]) spark.Pair[int, int] {
				return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
			},
		),
		func(p spark.Pair[int, int]) float64 {
			if p.Value == 0 {
				return 0
			}
			return float64(p.Key) / float64(p.Value)
		},
	)

	fmt.Println()
	fmt.Println("--- Average price by product ---")
	for _, p := range spark.Collect(avgByProduct) {
		fmt.Printf("  %s: $%.2f\n", p.Key, p.Value)
	}

	grandTotal, ok := spark.Reduce(monthlyData, func(a, b spark.Pair[string, int]) spark.Pair[string, int] {
		return spark.NewPair("total", a.Value+b.Value)
	})
	if ok {
		fmt.Println()
		fmt.Printf("=== Grand total revenue: $%d ===\n", grandTotal.Value)
	}
}
