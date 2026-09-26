package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	spark "goSpark"
)

func init() {
	spark.RegisterPipeline("example-text-lines", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[string], error) {
		return spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions), nil
	})
	spark.RegisterPipeline("example-text-words", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[string], error) {
		return spark.FlatMap(spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions), func(line string) []string {
			return strings.Fields(line)
		}), nil
	})
	spark.RegisterPipeline("example-text-freqs", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		words := spark.FlatMap(spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions), func(line string) []spark.Pair[string, int] {
			fields := strings.Fields(line)
			result := make([]spark.Pair[string, int], len(fields))
			for i, w := range fields {
				result[i] = spark.NewPair(strings.ToLower(strings.Trim(w, ".,;:!")), 1)
			}
			return result
		})
		return spark.ReduceByKey(words, spark.NewHashPartitioner(4), func(a, b int) int { return a + b }), nil
	})
	spark.RegisterPipeline("example-text-long", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[int], error) {
		words := spark.FlatMap(spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions), func(line string) []string {
			return strings.Fields(line)
		})
		longWords := spark.Filter(words, func(w string) bool { return len(w) >= 8 })
		total := spark.ReduceBroadcast(spark.Map(longWords, func(string) int { return 1 }), func(a, b int) int { return a + b })
		return spark.Map(spark.Parallelize(ctx, []int{0}, 1), func(int) int { return total.Value() }), nil
	})
	spark.RegisterPipeline("example-text-upper", func(ctx *spark.Context, spec spark.JobSpec) (*spark.RDD[[]string], error) {
		lines := spark.Map(spark.TextFile(ctx, spec.Params["path"], spec.NumPartitions), func(line string) string {
			return strings.ToUpper(line)
		})
		return spark.Glom(lines), nil
	})
}

func run[T any](name, path string, partitions int) []T {
	rows, err := spark.RunPipelineLocal[T](spark.JobSpec{
		TaskName: name, NumPartitions: partitions, Params: map[string]string{"path": path},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return rows
}

func main() {
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
	fmt.Printf("Total lines: %d\n", len(run[string]("example-text-lines", filename, 2)))
	fmt.Printf("Total words: %d\n", len(run[string]("example-text-words", filename, 2)))
	fmt.Println()
	fmt.Println("Word frequencies:")
	for _, p := range run[spark.Pair[string, int]]("example-text-freqs", filename, 2) {
		fmt.Printf("  %-12s: %d\n", p.Key, p.Value)
	}
	fmt.Printf("\nWords with 8+ characters: %d\n", run[int]("example-text-long", filename, 2)[0])
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error saving: %v\n", err)
		return
	}
	fmt.Println()
	fmt.Println("Saved uppercase lines to:", outputDir)
	for i, part := range run[[]string]("example-text-upper", filename, 2) {
		name := fmt.Sprintf("part-%05d", i)
		body := strings.Join(part, "\n")
		if body != "" {
			body += "\n"
		}
		if err := os.WriteFile(filepath.Join(outputDir, name), []byte(body), 0644); err != nil {
			fmt.Printf("Error saving: %v\n", err)
			return
		}
		fmt.Printf("  %s:\n%s\n", name, body)
	}
}
