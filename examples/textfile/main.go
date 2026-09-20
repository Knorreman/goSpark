package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "TextFileProcessing",
		Master:        "local[*]",
		NumPartitions: 2,
	})
	defer ctx.Stop()

	fmt.Println("=== Text File Processing Demo ===")
	fmt.Println()

	tmpDir, err := os.MkdirTemp("", "gospark-textfile-*")
	if err != nil {
		fmt.Printf("Error creating temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	content := strings.Join([]string{
		"Go is a statically typed compiled language designed at Google",
		"Go is syntactically similar to C but with memory safety",
		"Go has garbage collection and structural typing",
		"Go was designed by Robert Griesemer Rob Pike and Ken Thompson",
		"Go is commonly used for cloud services and distributed systems",
		"Go supports concurrent programming with goroutines and channels",
		"Go has a large standard library and strong tooling ecosystem",
		"Go is open source and has a vibrant community",
	}, "\n")

	filename := filepath.Join(tmpDir, "sample.txt")
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		return
	}
	fmt.Printf("Created temp file: %s\n\n", filename)

	lines := spark.TextFile(ctx, filename, 2)
	lineCount := spark.Count(lines)
	fmt.Printf("Total lines: %d\n", lineCount)

	lines = spark.TextFile(ctx, filename, 2)

	words := spark.FlatMap(lines, func(line string) []string {
		return strings.Fields(line)
	})

	wordCount := spark.Count(words)
	fmt.Printf("Total words: %d\n", wordCount)

	lines2 := spark.TextFile(ctx, filename, 2)
	pairWords := spark.FlatMap(lines2, func(line string) []spark.Pair[string, int] {
		fields := strings.Fields(line)
		result := make([]spark.Pair[string, int], len(fields))
		for i, w := range fields {
			result[i] = spark.NewPair(strings.ToLower(strings.Trim(w, ".,;:!")), 1)
		}
		return result
	})

	freqs := spark.ReduceByKey(pairWords, spark.NewHashPartitioner(4), func(a, b int) int { return a + b })
	fmt.Println()
	fmt.Println("Word frequencies:")
	for _, p := range spark.Collect(freqs) {
		fmt.Printf("  %-12s: %d\n", p.Key, p.Value)
	}

	longWords := spark.Filter(words, func(w string) bool { return len(w) >= 8 })
	longCount, _ := spark.Reduce(spark.Map(longWords, func(w string) int { return 1 }), func(a, b int) int { return a + b })
	fmt.Printf("\nWords with 8+ characters: %d\n", longCount)

	outputDir := filepath.Join(tmpDir, "output")
	if err := spark.SaveAsTextFile(spark.Map(
		spark.TextFile(ctx, filename, 2),
		func(line string) string { return strings.ToUpper(line) },
	), outputDir); err != nil {
		fmt.Printf("Error saving: %v\n", err)
	} else {
		fmt.Println()
		fmt.Println("Saved uppercase lines to:", outputDir)
		entries, _ := os.ReadDir(outputDir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "part-") {
				data, err := os.ReadFile(filepath.Join(outputDir, e.Name()))
				if err == nil {
					fmt.Printf("  %s:\n%s\n", e.Name(), string(data))
				}
			}
		}
	}
}
