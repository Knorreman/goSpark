package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func Collect[T any](rdd *RDD[T]) []T {
	computeShuffleStages(rdd)
	var mu sync.Mutex
	var result []T
	partitions := rdd.Partitions()
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, p := range partitions {
		go func(partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			data := CollectIterator(iter)
			mu.Lock()
			result = append(result, data...)
			mu.Unlock()
		}(p)
	}
	wg.Wait()
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
			partitioner := shuffleDep.partitioner
			shuffleID := shuffleDep.shuffleID
			numReduceParts := partitioner.NumPartitions()
			ctx := parent.Ctx()
			ctx.ShuffleManager().RegisterShuffle(shuffleID)

			var mu sync.Mutex
			var wg sync.WaitGroup
			wg.Add(len(partitions))
			for _, p := range partitions {
				go func(partition Partition) {
					defer wg.Done()
					iter := parent.ComputeAny(partition)
					spills := make(map[int]*spillAcc, numReduceParts)
					for i := 0; i < numReduceParts; i++ {
						spills[i] = &spillAcc{}
					}
					spillDir := filepath.Join(os.TempDir(), "gospark-spill", fmt.Sprintf("%d-%d", shuffleID, partition.Index()))
					codec := DefaultCodec()
					for {
						item, ok := iter()
						if !ok {
							break
						}
						key := shuffleDep.ExtractKey(item)
						var reduceID int
						if key != nil {
							reduceID = partitioner.GetPartition(key)
						} else {
							reduceID = 0
						}
						_ = spills[reduceID].add(item, spillDir, codec)
					}
					buckets := make(map[int][]any, numReduceParts)
					for i := 0; i < numReduceParts; i++ {
						recs, err := spills[i].collect(codec)
						if err != nil {
							recs = nil
						}
						buckets[i] = combineMapOutput(recs, shuffleDep)
					}
					mu.Lock()
					ctx.ShuffleManager().WriteMapOutput(shuffleID, partition.Index(), buckets)
					mu.Unlock()
				}(p)
			}
			wg.Wait()
		}
	}
}
