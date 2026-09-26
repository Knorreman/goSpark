package main

import (
	"fmt"
	"os"
	"strings"

	spark "goSpark"
)

type Student struct {
	Name string
	Math int
	Sci  int
	Art  int
}

func students() []Student {
	return []Student{
		{"Alice", 92, 88, 95}, {"Bob", 65, 72, 58}, {"Carol", 78, 95, 80},
		{"Dave", 45, 55, 70}, {"Eve", 98, 92, 88}, {"Frank", 72, 68, 75},
		{"Grace", 88, 90, 92}, {"Hank", 55, 60, 62}, {"Ivy", 95, 98, 97},
		{"Jack", 40, 50, 45},
	}
}

func grade(score int) string {
	switch {
	case score >= 90:
		return "A"
	case score >= 80:
		return "B"
	case score >= 70:
		return "C"
	case score >= 60:
		return "D"
	default:
		return "F"
	}
}

func init() {
	spark.RegisterPipeline("example-grades", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[Student], error) {
		return spark.Parallelize(ctx, students(), 3), nil
	})
	spark.RegisterPipeline("example-honor", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[Student], error) {
		return spark.Filter(spark.Parallelize(ctx, students(), 3), func(s Student) bool {
			return float64(s.Math+s.Sci+s.Art)/3.0 >= 85
		}), nil
	})
	spark.RegisterPipeline("example-help", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[Student], error) {
		return spark.Filter(spark.Parallelize(ctx, students(), 3), func(s Student) bool {
			return s.Math < 60 || s.Sci < 60 || s.Art < 60
		}), nil
	})
	spark.RegisterPipeline("example-subject-avg", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		return spark.MapValues(spark.ReduceByKey(spark.FlatMap(spark.Parallelize(ctx, students(), 3), func(s Student) []spark.Pair[string, spark.Pair[float64, int]] {
			return []spark.Pair[string, spark.Pair[float64, int]]{
				spark.NewPair("Math", spark.NewPair(float64(s.Math), 1)),
				spark.NewPair("Sci", spark.NewPair(float64(s.Sci), 1)),
				spark.NewPair("Art", spark.NewPair(float64(s.Art), 1)),
			}
		}), spark.NewHashPartitioner(3), func(a, b spark.Pair[float64, int]) spark.Pair[float64, int] {
			return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
		}), func(p spark.Pair[float64, int]) float64 {
			if p.Value == 0 {
				return 0
			}
			return p.Key / float64(p.Value)
		}), nil
	})
	spark.RegisterPipeline("example-student-avg", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		return spark.Map(spark.Parallelize(ctx, students(), 3), func(s Student) spark.Pair[string, float64] {
			return spark.NewPair(s.Name, float64(s.Math+s.Sci+s.Art)/3.0)
		}), nil
	})
	spark.RegisterPipeline("example-grade-dist", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, int]], error) {
		return spark.ReduceByKey(spark.FlatMap(spark.Parallelize(ctx, students(), 3), func(s Student) []spark.Pair[string, int] {
			return []spark.Pair[string, int]{
				spark.NewPair(grade(s.Math)+"_Math", 1),
				spark.NewPair(grade(s.Sci)+"_Sci", 1),
				spark.NewPair(grade(s.Art)+"_Art", 1),
			}
		}), spark.NewHashPartitioner(4), func(a, b int) int { return a + b }), nil
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
	fmt.Println("=== Student Grade Analytics ===")
	fmt.Println()
	fmt.Println("--- All students ---")
	for _, s := range run[Student]("example-grades") {
		fmt.Printf("  %-6s Math=%3d  Sci=%3d  Art=%3d\n", s.Name, s.Math, s.Sci, s.Art)
	}
	fmt.Println()
	fmt.Println("--- Honor Roll (avg >= 85) ---")
	honor := run[Student]("example-honor")
	for _, s := range honor {
		fmt.Printf("  %-6s avg=%.1f (Math=%d, Sci=%d, Art=%d)\n", s.Name, float64(s.Math+s.Sci+s.Art)/3.0, s.Math, s.Sci, s.Art)
	}
	fmt.Println()
	fmt.Println("--- Needs Help (any subject < 60) ---")
	help := run[Student]("example-help")
	for _, s := range help {
		var subjects []string
		if s.Math < 60 {
			subjects = append(subjects, "Math")
		}
		if s.Sci < 60 {
			subjects = append(subjects, "Sci")
		}
		if s.Art < 60 {
			subjects = append(subjects, "Art")
		}
		fmt.Printf("  %-6s failing: %s\n", s.Name, strings.Join(subjects, ", "))
	}
	fmt.Println()
	fmt.Println("--- Average by subject ---")
	for _, p := range run[spark.Pair[string, float64]]("example-subject-avg") {
		fmt.Printf("  %s: %.1f\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Valedictorian & Bottom Student ---")
	var top, bottom spark.Pair[string, float64]
	for i, p := range run[spark.Pair[string, float64]]("example-student-avg") {
		if i == 0 || p.Value > top.Value {
			top = p
		}
		if i == 0 || p.Value < bottom.Value {
			bottom = p
		}
	}
	fmt.Printf("  Top:    %s (avg=%.1f)\n", top.Key, top.Value)
	fmt.Printf("  Bottom: %s (avg=%.1f)\n", bottom.Key, bottom.Value)
	fmt.Println()
	fmt.Printf("--- Statistics ---\n")
	fmt.Printf("  Total students: %d\n", len(run[Student]("example-grades")))
	fmt.Printf("  Honor roll: %d students\n", len(honor))
	fmt.Printf("  Needs help: %d students\n", len(help))
	fmt.Println()
	fmt.Println("--- Grade Distribution ---")
	for _, p := range run[spark.Pair[string, int]]("example-grade-dist") {
		fmt.Printf("  %s: %d\n", p.Key, p.Value)
	}
}
