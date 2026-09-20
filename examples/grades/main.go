package main

import (
	"fmt"
	"strings"

	spark "goSpark"
)

type Student struct {
	Name string
	Math int
	Sci  int
	Art  int
}

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "StudentGrades",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	fmt.Println("=== Student Grade Analytics ===")
	fmt.Println()

	students := []Student{
		{"Alice", 92, 88, 95},
		{"Bob", 65, 72, 58},
		{"Carol", 78, 95, 80},
		{"Dave", 45, 55, 70},
		{"Eve", 98, 92, 88},
		{"Frank", 72, 68, 75},
		{"Grace", 88, 90, 92},
		{"Hank", 55, 60, 62},
		{"Ivy", 95, 98, 97},
		{"Jack", 40, 50, 45},
	}

	rdd := spark.Parallelize(ctx, students, 3)

	fmt.Println("--- All students ---")
	for _, s := range spark.Collect(rdd) {
		fmt.Printf("  %-6s Math=%3d  Sci=%3d  Art=%3d\n", s.Name, s.Math, s.Sci, s.Art)
	}

	honorRoll := spark.Filter(rdd, func(s Student) bool {
		avg := float64(s.Math+s.Sci+s.Art) / 3.0
		return avg >= 85
	})

	fmt.Println()
	fmt.Println("--- Honor Roll (avg >= 85) ---")
	for _, s := range spark.Collect(honorRoll) {
		avg := float64(s.Math+s.Sci+s.Art) / 3.0
		fmt.Printf("  %-6s avg=%.1f (Math=%d, Sci=%d, Art=%d)\n", s.Name, avg, s.Math, s.Sci, s.Art)
	}

	needsHelp := spark.Filter(rdd, func(s Student) bool {
		return s.Math < 60 || s.Sci < 60 || s.Art < 60
	})

	fmt.Println()
	fmt.Println("--- Needs Help (any subject < 60) ---")
	for _, s := range spark.Collect(needsHelp) {
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

	subjectAvgs := spark.MapValues(
		spark.ReduceByKey(
			spark.FlatMap(rdd, func(s Student) []spark.Pair[string, spark.Pair[float64, int]] {
				return []spark.Pair[string, spark.Pair[float64, int]]{
					spark.NewPair("Math", spark.NewPair(float64(s.Math), 1)),
					spark.NewPair("Sci", spark.NewPair(float64(s.Sci), 1)),
					spark.NewPair("Art", spark.NewPair(float64(s.Art), 1)),
				}
			}),
			spark.NewHashPartitioner(3),
			func(a, b spark.Pair[float64, int]) spark.Pair[float64, int] {
				return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
			},
		),
		func(p spark.Pair[float64, int]) float64 {
			if p.Value == 0 {
				return 0
			}
			return p.Key / float64(p.Value)
		},
	)

	fmt.Println()
	fmt.Println("--- Average by subject ---")
	for _, p := range spark.Collect(subjectAvgs) {
		fmt.Printf("  %s: %.1f\n", p.Key, p.Value)
	}

	studentAvgs := spark.Map(rdd, func(s Student) spark.Pair[string, float64] {
		avg := float64(s.Math+s.Sci+s.Art) / 3.0
		return spark.NewPair(s.Name, avg)
	})

	fmt.Println()
	fmt.Println("--- Valedictorian & Bottom Student ---")
	valedictorian, ok := spark.Max(studentAvgs, func(a, b spark.Pair[string, float64]) bool { return a.Value < b.Value })
	if ok {
		fmt.Printf("  Top:    %s (avg=%.1f)\n", valedictorian.Key, valedictorian.Value)
	}
	bottom, ok := spark.Min(studentAvgs, func(a, b spark.Pair[string, float64]) bool { return a.Value < b.Value })
	if ok {
		fmt.Printf("  Bottom: %s (avg=%.1f)\n", bottom.Key, bottom.Value)
	}

	classCount := spark.Count(rdd)
	fmt.Println()
	fmt.Printf("--- Statistics ---\n")
	fmt.Printf("  Total students: %d\n", classCount)
	fmt.Printf("  Honor roll: %d students\n", spark.Count(honorRoll))
	fmt.Printf("  Needs help: %d students\n", spark.Count(needsHelp))

	gradeDistribution := spark.ReduceByKey(
		spark.FlatMap(rdd, func(s Student) []spark.Pair[string, int] {
			grade := func(score int) string {
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
			return []spark.Pair[string, int]{
				spark.NewPair(grade(s.Math)+"_Math", 1),
				spark.NewPair(grade(s.Sci)+"_Sci", 1),
				spark.NewPair(grade(s.Art)+"_Art", 1),
			}
		}),
		spark.NewHashPartitioner(4),
		func(a, b int) int { return a + b },
	)

	fmt.Println()
	fmt.Println("--- Grade Distribution ---")
	for _, p := range spark.Collect(gradeDistribution) {
		fmt.Printf("  %s: %d\n", p.Key, p.Value)
	}
}
