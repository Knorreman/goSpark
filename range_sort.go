package spark

import (
	"fmt"
	"sort"
)

const sortSamplesPerMap = 64
const maxSortSampleBytes = 4 << 10
const sortGlobalSamples = 4096

// sampleSortPartition makes a bounded deterministic reservoir from one input
// partition. It scans without retaining the input data; subsequent map tasks
// replay their own partition with the selected range boundaries.
func sampleSortPartition(ctx *Context, dep *ShuffleDep, part Partition) ([]any, error) {
	if dep == nil || dep.sortLess == nil {
		return nil, fmt.Errorf("not a range sort shuffle")
	}
	var samples []any
	var seen uint64
	seed := uint64(part.Index()+1)*0x9e3779b97f4a7c15 + 1
	iter := dep.Parent().ComputeAny(part)
	for {
		ctx.checkCanceled()
		rec, ok := iter()
		if !ok {
			break
		}
		key := dep.ExtractKey(rec)
		payload, err := DefaultCodec().Encode(key)
		if err != nil {
			return nil, err
		}
		// Large keys remain in the shuffled data; omitting them only affects
		// balance, never ordering or correctness.
		if len(payload) > maxSortSampleBytes {
			continue
		}
		seen++
		if len(samples) < sortSamplesPerMap {
			samples = append(samples, key)
			continue
		}
		seed = seed*6364136223846793005 + 1442695040888963407
		if slot := seed % seen; slot < sortSamplesPerMap {
			samples[slot] = key
		}
	}
	return samples, nil
}

// selectRangeBounds retains duplicate split keys. The partitioner sends all
// equal keys to one side of each boundary, preserving global ordering.
func selectRangeBounds(samples []any, reducers int, less func(any, any) bool) []any {
	if len(samples) == 0 || reducers <= 1 {
		return nil
	}
	sort.SliceStable(samples, func(i, j int) bool { return less(samples[i], samples[j]) })
	bounds := make([]any, 0, reducers-1)
	for p := 1; p < reducers; p++ {
		bounds = append(bounds, samples[p*len(samples)/reducers])
	}
	return bounds
}

// Keep the driver's aggregate reservoir bounded even with many input maps.
func appendSortSamples(samples, incoming []any, seen *uint64) []any {
	for _, key := range incoming {
		*seen++
		if len(samples) < sortGlobalSamples {
			samples = append(samples, key)
			continue
		}
		h := *seen + 0x9e3779b97f4a7c15
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
		h ^= h >> 31
		if slot := h % *seen; slot < sortGlobalSamples {
			samples[slot] = key
		}
	}
	return samples
}
