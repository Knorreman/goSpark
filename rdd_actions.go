package spark

import (
	"os"
	"path/filepath"
	"sync"
)

func Collect[T any](rdd *RDD[T]) []T {
	computeShuffleStages(rdd)
	var result []T
	partitions := rdd.Partitions()
	byPartition := make([][]T, len(partitions))
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for i, p := range partitions {
		go func(index int, partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			byPartition[index] = CollectIterator(iter)
		}(i, p)
	}
	wg.Wait()
	for _, data := range byPartition {
		result = append(result, data...)
	}
	return result
}

func Count[T any](rdd *RDD[T]) int64 {
	computeShuffleStages(rdd)
	var count int64
	var mu sync.Mutex
	partitions := rdd.Partitions()
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			c := CountIterator(iter)
			mu.Lock()
			count += c
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return count
}

func Reduce[T any](rdd *RDD[T], fn func(T, T) T) (T, bool) {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()
	results := make([]T, len(partitions))
	valid := make([]bool, len(partitions))
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			r, ok := ReduceIterator(iter, fn)
			results[idx] = r
			valid[idx] = ok
		}(i, p)
	}
	wg.Wait()

	var first T
	found := false
	for i, v := range results {
		if valid[i] {
			if !found {
				first = v
				found = true
			} else {
				first = fn(first, v)
			}
		}
	}
	return first, found
}

func Fold[T any](rdd *RDD[T], zero T, fn func(T, T) T) T {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()
	results := make([]T, len(partitions))
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			acc := zero
			for {
				v, ok := iter()
				if !ok {
					break
				}
				acc = fn(acc, v)
			}
			results[idx] = acc
		}(i, p)
	}
	wg.Wait()
	acc := zero
	for _, r := range results {
		acc = fn(acc, r)
	}
	return acc
}

func ForEach[T any](rdd *RDD[T], fn func(T)) {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			ForeachIterator(iter, fn)
		}(p)
	}
	wg.Wait()
}

// ForEachPartition calls fn once per partition, including empty partitions.
// The callback consumes the partition iterator and its results are not returned.
func ForEachPartition[T any](rdd *RDD[T], fn func(Iterator[T])) {
	computeShuffleStages(rdd)
	var wg sync.WaitGroup
	for _, p := range rdd.Partitions() {
		wg.Add(1)
		go func(partition Partition) {
			defer wg.Done()
			fn(rdd.Compute(partition))
		}(p)
	}
	wg.Wait()
}

func Take[T any](rdd *RDD[T], n int) []T {
	computeShuffleStages(rdd)
	result := make([]T, 0, n)
	for _, p := range rdd.Partitions() {
		if len(result) >= n {
			break
		}
		iter := rdd.Compute(p)
		data := TakeIterator(iter, n-len(result))
		result = append(result, data...)
		if len(result) >= n {
			result = result[:n]
			break
		}
	}
	return result
}

func First[T any](rdd *RDD[T]) (T, bool) {
	result := Take(rdd, 1)
	if len(result) == 0 {
		var zero T
		return zero, false
	}
	return result[0], true
}

func IsEmpty[T any](rdd *RDD[T]) bool {
	_, ok := First(rdd)
	return !ok
}

func Min[T any](rdd *RDD[T], less func(T, T) bool) (T, bool) {
	return Reduce(rdd, func(a, b T) T {
		if less(a, b) {
			return a
		}
		return b
	})
}

func Max[T any](rdd *RDD[T], less func(T, T) bool) (T, bool) {
	return Reduce(rdd, func(a, b T) T {
		if less(a, b) {
			return b
		}
		return a
	})
}

func CountByValue[T comparable](rdd *RDD[T]) map[T]int64 {
	data := Collect(rdd)
	result := make(map[T]int64)
	for _, v := range data {
		result[v]++
	}
	return result
}

func computeShuffleStages(rddAny RDDAny) {
	if rddAny == nil || rddAny.IsCached() {
		return
	}
	deps := rddAny.Dependencies()
	if deps == nil {
		return
	}
	for _, dep := range deps {
		parent := dep.Parent()
		if parent == nil {
			continue
		}
		computeShuffleStages(parent)
		if dep.DepType() == DepShuffle {
			shuffleDep, ok := dep.(*ShuffleDep)
			if !ok {
				continue
			}
			partitions := parent.Partitions()
			if shuffleDep.sortLess != nil && shuffleDep.partitioner.NumPartitions() > 1 {
				var samples []any
				var seen uint64
				for _, p := range partitions {
					part, err := sampleSortPartition(parent.Ctx(), shuffleDep, p)
					must(err)
					samples = appendSortSamples(samples, part, &seen)
				}
				shuffleDep.partitioner.(*RangePartitioner).SetRangeBounds(selectRangeBounds(samples, shuffleDep.partitioner.NumPartitions(), shuffleDep.sortLess))
			}
			shuffleID := shuffleDep.shuffleID
			ctx := parent.Ctx()
			ctx.ShuffleManager().RegisterShuffle(shuffleID)
			dir, err := os.MkdirTemp(ctx.disk.dir, "local-stage-")
			must(err)
			store := NewDiskShuffleStore(dir)
			store.budget = ctx.shuffleBudget
			store.codec = cancelCodec{ctx: ctx.TaskContext(), RecordCodec: DefaultCodec()}
			for _, p := range partitions {
				manifest, err := writeStreamMap(ctx, store, shuffleDep, p, "local", p.Index(), 0)
				must(err)
				paths := map[int]string{}
				for _, b := range manifest.Buckets {
					paths[b.ReduceID] = filepath.Join(manifest.Location, bucketName(b.ReduceID))
				}
				ctx.ShuffleManager().(*fileShuffleManager).install(shuffleID, p.Index(), paths)
			}
		}
	}
}
