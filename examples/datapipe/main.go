package main

import (
	"fmt"
	"math"
	"os"
	"strings"

	spark "goSpark"
)

type Reading struct {
	StationID string
	Temp      float64
	Humidity  float64
	Timestamp string
}

func readings() []string {
	return []string{
		"S01,23.5,65.0,2024-01-15T08:00", "S02,18.2,78.3,2024-01-15T08:00", "S03,25.1,55.2,2024-01-15T08:00",
		"S01,24.0,63.5,2024-01-15T12:00", "S02,19.8,76.1,2024-01-15T12:00", "S03,26.3,52.8,2024-01-15T12:00",
		"S01,21.2,68.9,2024-01-15T18:00", "S02,17.5,80.2,2024-01-15T18:00", "S03,23.8,58.6,2024-01-15T18:00",
		"S01,25.8,60.1,2024-01-16T08:00", "S02,20.5,73.4,2024-01-16T08:00", "S03,27.2,49.5,2024-01-16T08:00",
		"S01,999.0,65.0,2024-01-16T12:00", "S02,19.0,77.0,2024-01-16T12:00", "S03,26.5,51.3,2024-01-16T12:00",
	}
}

func parsed(ctx *spark.Context) *spark.RDD[spark.Pair[string, Reading]] {
	return spark.Map(spark.Parallelize(ctx, readings(), 3), func(line string) spark.Pair[string, Reading] {
		parts := strings.Split(line, ",")
		temp, hum := 0.0, 0.0
		fmt.Sscanf(parts[1], "%f", &temp)
		fmt.Sscanf(parts[2], "%f", &hum)
		return spark.NewPair(parts[0], Reading{StationID: parts[0], Temp: temp, Humidity: hum, Timestamp: parts[3]})
	})
}

func cleaned(ctx *spark.Context) *spark.RDD[spark.Pair[string, Reading]] {
	return spark.Filter(parsed(ctx), func(p spark.Pair[string, Reading]) bool {
		return p.Value.Temp < 100 && p.Value.Temp > -50
	})
}

func avgBy(ctx *spark.Context, value func(Reading) float64) *spark.RDD[spark.Pair[string, float64]] {
	return spark.MapValues(spark.ReduceByKey(spark.Map(cleaned(ctx), func(p spark.Pair[string, Reading]) spark.Pair[string, spark.Pair[float64, int]] {
		return spark.NewPair(p.Key, spark.NewPair(value(p.Value), 1))
	}), spark.NewHashPartitioner(3), func(a, b spark.Pair[float64, int]) spark.Pair[float64, int] {
		return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
	}), func(p spark.Pair[float64, int]) float64 {
		if p.Value == 0 {
			return 0
		}
		return p.Key / float64(p.Value)
	})
}

func init() {
	spark.RegisterPipeline("example-sensor-raw", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, Reading]], error) {
		return parsed(ctx), nil
	})
	spark.RegisterPipeline("example-sensor-clean", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, Reading]], error) {
		return cleaned(ctx), nil
	})
	spark.RegisterPipeline("example-sensor-temp", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		return avgBy(ctx, func(r Reading) float64 { return r.Temp }), nil
	})
	spark.RegisterPipeline("example-sensor-hum", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		return avgBy(ctx, func(r Reading) float64 { return r.Humidity }), nil
	})
	spark.RegisterPipeline("example-sensor-extreme", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[float64, string]], error) {
		return spark.Map(cleaned(ctx), func(p spark.Pair[string, Reading]) spark.Pair[float64, string] {
			return spark.NewPair(p.Value.Temp, p.Value.StationID+"@"+p.Value.Timestamp)
		}), nil
	})
	spark.RegisterPipeline("example-sensor-stations", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[string], error) {
		return spark.Distinct(spark.Keys(cleaned(ctx)), 3), nil
	})
	spark.RegisterPipeline("example-sensor-daily", func(ctx *spark.Context, _ spark.JobSpec) (*spark.RDD[spark.Pair[string, float64]], error) {
		return spark.MapValues(spark.ReduceByKey(spark.Map(cleaned(ctx), func(p spark.Pair[string, Reading]) spark.Pair[string, spark.Pair[float64, int]] {
			return spark.NewPair(strings.Split(p.Value.Timestamp, "T")[0], spark.NewPair(p.Value.Temp, 1))
		}), spark.NewHashPartitioner(3), func(a, b spark.Pair[float64, int]) spark.Pair[float64, int] {
			return spark.NewPair(a.Key+b.Key, a.Value+b.Value)
		}), func(p spark.Pair[float64, int]) float64 {
			if p.Value == 0 {
				return 0
			}
			return p.Key / float64(p.Value)
		}), nil
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
	fmt.Println("=== Sensor Data Processing Pipeline ===")
	fmt.Println()
	fmt.Println("--- Raw readings (first 5) ---")
	raw := run[spark.Pair[string, Reading]]("example-sensor-raw")
	for _, p := range raw[:5] {
		fmt.Printf("  %s: temp=%.1f humidity=%.1f @ %s\n", p.Key, p.Value.Temp, p.Value.Humidity, p.Value.Timestamp)
	}
	clean := run[spark.Pair[string, Reading]]("example-sensor-clean")
	fmt.Println()
	fmt.Println("--- Cleaned readings ---")
	fmt.Printf("  Before: %d, After: %d (removed outliers)\n", len(raw), len(clean))
	fmt.Println()
	fmt.Println("--- Average temperature per station ---")
	for _, p := range run[spark.Pair[string, float64]]("example-sensor-temp") {
		fmt.Printf("  Station %s: %.2f°C\n", p.Key, p.Value)
	}
	fmt.Println()
	fmt.Println("--- Average humidity per station ---")
	for _, p := range run[spark.Pair[string, float64]]("example-sensor-hum") {
		fmt.Printf("  Station %s: %.1f%%\n", p.Key, p.Value)
	}
	extremes := run[spark.Pair[float64, string]]("example-sensor-extreme")
	minTemp, maxTemp := extremes[0], extremes[0]
	for _, p := range extremes[1:] {
		if p.Key < minTemp.Key {
			minTemp = p
		}
		if p.Key > maxTemp.Key {
			maxTemp = p
		}
	}
	fmt.Println()
	fmt.Println("--- Extreme readings ---")
	parts := strings.Split(minTemp.Value, "@")
	fmt.Printf("  Coldest: %s at %s (%.1f°C)\n", parts[0], parts[1], minTemp.Key)
	parts = strings.Split(maxTemp.Value, "@")
	fmt.Printf("  Hottest: %s at %s (%.1f°C)\n", parts[0], parts[1], maxTemp.Key)
	fmt.Println()
	fmt.Printf("--- Distinct stations ---\n")
	for _, station := range run[string]("example-sensor-stations") {
		fmt.Printf("  %s\n", station)
	}
	fmt.Println()
	fmt.Println("--- Daily average temperature ---")
	for _, p := range run[spark.Pair[string, float64]]("example-sensor-daily") {
		fmt.Printf("  %s: %.2f°C\n", p.Key, p.Value)
	}
	var tempSum float64
	for _, r := range clean {
		tempSum += r.Value.Temp
	}
	globalAvg := tempSum / float64(len(clean))
	var variance float64
	for _, r := range clean {
		variance += math.Pow(r.Value.Temp-globalAvg, 2)
	}
	fmt.Println()
	fmt.Println("--- Global Statistics ---")
	fmt.Printf("  Mean:   %.2f°C\n", globalAvg)
	fmt.Printf("  StdDev: %.2f°C\n", math.Sqrt(variance/float64(len(clean))))
	fmt.Printf("  Min:    %.1f°C\n", minTemp.Key)
	fmt.Printf("  Max:    %.1f°C\n", maxTemp.Key)
}
