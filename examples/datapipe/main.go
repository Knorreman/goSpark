package main

import (
	"fmt"
	"math"
	"strings"

	spark "goSpark"
)

type Reading struct {
	StationID string
	Temp      float64
	Humidity  float64
	Timestamp string
}

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "SensorDataPipeline",
		Master:        "local[*]",
		NumPartitions: 4,
	})
	defer ctx.Stop()

	fmt.Println("=== Sensor Data Processing Pipeline ===")
	fmt.Println()

	readings := []string{
		"S01,23.5,65.0,2024-01-15T08:00",
		"S02,18.2,78.3,2024-01-15T08:00",
		"S03,25.1,55.2,2024-01-15T08:00",
		"S01,24.0,63.5,2024-01-15T12:00",
		"S02,19.8,76.1,2024-01-15T12:00",
		"S03,26.3,52.8,2024-01-15T12:00",
		"S01,21.2,68.9,2024-01-15T18:00",
		"S02,17.5,80.2,2024-01-15T18:00",
		"S03,23.8,58.6,2024-01-15T18:00",
		"S01,25.8,60.1,2024-01-16T08:00",
		"S02,20.5,73.4,2024-01-16T08:00",
		"S03,27.2,49.5,2024-01-16T08:00",
		"S01,999.0,65.0,2024-01-16T12:00",
		"S02,19.0,77.0,2024-01-16T12:00",
		"S03,26.5,51.3,2024-01-16T12:00",
	}

	raw := spark.Parallelize(ctx, readings, 3)

	parsed := spark.Map(raw, func(line string) spark.Pair[string, Reading] {
		parts := strings.Split(line, ",")
		temp := 0.0
		hum := 0.0
		fmt.Sscanf(parts[1], "%f", &temp)
		fmt.Sscanf(parts[2], "%f", &hum)
		return spark.NewPair(parts[0], Reading{
			StationID: parts[0],
			Temp:      temp,
			Humidity:  hum,
			Timestamp: parts[3],
		})
	})

	fmt.Println("--- Raw readings (first 5) ---")
	for _, p := range spark.Take(parsed, 5) {
		fmt.Printf("  %s: temp=%.1f humidity=%.1f @ %s\n", p.Key, p.Value.Temp, p.Value.Humidity, p.Value.Timestamp)
	}

	cleaned := spark.Filter(parsed, func(p spark.Pair[string, Reading]) bool {
		return p.Value.Temp < 100 && p.Value.Temp > -50
	})

	fmt.Println()
	fmt.Println("--- Cleaned readings ---")
	fmt.Printf("  Before: %d, After: %d (removed outliers)\n", spark.Count(parsed), spark.Count(cleaned))

	stationAvgs := spark.MapValues(
		spark.ReduceByKey(
			spark.Map(cleaned, func(p spark.Pair[string, Reading]) spark.Pair[string, spark.Pair[float64, int]] {
				return spark.NewPair(p.Key, spark.NewPair(p.Value.Temp, 1))
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
	fmt.Println("--- Average temperature per station ---")
	for _, p := range spark.Collect(stationAvgs) {
		fmt.Printf("  Station %s: %.2f°C\n", p.Key, p.Value)
	}

	stationHumAvgs := spark.MapValues(
		spark.ReduceByKey(
			spark.Map(cleaned, func(p spark.Pair[string, Reading]) spark.Pair[string, spark.Pair[float64, int]] {
				return spark.NewPair(p.Key, spark.NewPair(p.Value.Humidity, 1))
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
	fmt.Println("--- Average humidity per station ---")
	for _, p := range spark.Collect(stationHumAvgs) {
		fmt.Printf("  Station %s: %.1f%%\n", p.Key, p.Value)
	}

	minTemp, _ := spark.Min(
		spark.Map(cleaned, func(p spark.Pair[string, Reading]) spark.Pair[float64, string] {
			return spark.NewPair(p.Value.Temp, p.Value.StationID+"@"+p.Value.Timestamp)
		}),
		func(a, b spark.Pair[float64, string]) bool { return a.Key < b.Key },
	)
	maxTemp, _ := spark.Max(
		spark.Map(cleaned, func(p spark.Pair[string, Reading]) spark.Pair[float64, string] {
			return spark.NewPair(p.Value.Temp, p.Value.StationID+"@"+p.Value.Timestamp)
		}),
		func(a, b spark.Pair[float64, string]) bool { return a.Key < b.Key },
	)

	fmt.Println()
	fmt.Println("--- Extreme readings ---")
	parts := strings.Split(minTemp.Value, "@")
	fmt.Printf("  Coldest: %s at %s (%.1f°C)\n", parts[0], parts[1], minTemp.Key)
	parts = strings.Split(maxTemp.Value, "@")
	fmt.Printf("  Hottest: %s at %s (%.1f°C)\n", parts[0], parts[1], maxTemp.Key)

	fmt.Println()
	fmt.Printf("--- Distinct stations ---\n")
	for _, station := range spark.Collect(spark.Distinct(spark.Keys(cleaned), 3)) {
		fmt.Printf("  %s\n", station)
	}

	dailyAvgs := spark.MapValues(
		spark.ReduceByKey(
			spark.Map(cleaned, func(p spark.Pair[string, Reading]) spark.Pair[string, spark.Pair[float64, int]] {
				day := strings.Split(p.Value.Timestamp, "T")[0]
				return spark.NewPair(day, spark.NewPair(p.Value.Temp, 1))
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
	fmt.Println("--- Daily average temperature ---")
	for _, p := range spark.Collect(dailyAvgs) {
		fmt.Printf("  %s: %.2f°C\n", p.Key, p.Value)
	}

	allTemps := spark.Collect(spark.Values(cleaned))
	var tempSum float64
	for _, r := range allTemps {
		tempSum += r.Temp
	}
	globalAvg := tempSum / float64(len(allTemps))
	var variance float64
	for _, t := range allTemps {
		variance += math.Pow(t.Temp-globalAvg, 2)
	}
	stdDev := math.Sqrt(variance / float64(len(allTemps)))

	fmt.Println()
	fmt.Println("--- Global Statistics ---")
	fmt.Printf("  Mean:   %.2f°C\n", globalAvg)
	fmt.Printf("  StdDev: %.2f°C\n", stdDev)
	fmt.Printf("  Min:    %.1f°C\n", minTemp.Key)
	fmt.Printf("  Max:    %.1f°C\n", maxTemp.Key)
}
