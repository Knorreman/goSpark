package main

import (
	"fmt"
	"strings"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "WordCount",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	lines := []string{
		"hello world",
		"hello spark",
		"world is great",
		"spark is awesome",
	}

	rdd := spark.Parallelize(ctx, lines, 2)

	words := spark.FlatMap(rdd, func(line string) []string {
		return strings.Split(line, " ")
	})

	pairs := spark.Map(words, func(word string) spark.Pair[string, int] {
		return spark.NewPair(word, 1)
	})

	counts := spark.ReduceByKey(pairs, spark.NewHashPartitioner(2), func(a, b int) int {
		return a + b
	})

	result := spark.Collect(counts)

	fmt.Println("Word counts:")
	for _, p := range result {
		fmt.Printf("  %s: %d\n", p.Key, p.Value)
	}

	count := spark.Count(words)
	fmt.Printf("\nTotal words: %d\n", count)

	first, ok := spark.First(words)
	if ok {
		fmt.Printf("First word: %s\n", first)
	}
}
