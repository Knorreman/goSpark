package spark

import (
	"math"
	"math/rand"
)

func Map[T any, U any](rdd *RDD[T], fn func(T) U) *RDD[U] {
	return NewRDD[U](
		rdd.ctx,
		rdd.Partitions,
		func() []Dependency {
			return []Dependency{NewOneToOneDependency(rdd)}
		},
		func(partition Partition) Iterator[U] {
			return MapIterator(rdd.Compute(partition), fn)
		},
	)
}

func FlatMap[T any, U any](rdd *RDD[T], fn func(T) []U) *RDD[U] {
	return NewRDD[U](
		rdd.ctx,
		rdd.Partitions,
		func() []Dependency {
			return []Dependency{NewOneToOneDependency(rdd)}
		},
		func(partition Partition) Iterator[U] {
			return FlatMapIterator(rdd.Compute(partition), fn)
		},
	)
}

func Filter[T any](rdd *RDD[T], fn func(T) bool) *RDD[T] {
	return NewRDD[T](
		rdd.ctx,
		rdd.Partitions,
		func() []Dependency {
			return []Dependency{NewOneToOneDependency(rdd)}
		},
		func(partition Partition) Iterator[T] {
			return FilterIterator(rdd.Compute(partition), fn)
		},
	)
}

func MapPartitions[T any, U any](rdd *RDD[T], fn func(Iterator[T]) Iterator[U], preservesPartitioning ...bool) *RDD[U] {
	pp := false
	if len(preservesPartitioning) > 0 {
		pp = preservesPartitioning[0]
	}
	var partitioner Partitioner
	if pp && rdd.partitioner != nil {
		partitioner = rdd.partitioner
	}
	return NewRDD[U](
		rdd.ctx,
		rdd.Partitions,
		func() []Dependency {
			return []Dependency{NewOneToOneDependency(rdd)}
		},
		func(partition Partition) Iterator[U] {
			return fn(rdd.Compute(partition))
		},
		WithPartitioner[U](partitioner),
	)
}

func MapPartitionsWithIndex[T any, U any](rdd *RDD[T], fn func(int, Iterator[T]) Iterator[U], preservesPartitioning ...bool) *RDD[U] {
	pp := false
	if len(preservesPartitioning) > 0 {
		pp = preservesPartitioning[0]
	}
	var partitioner Partitioner
	if pp && rdd.partitioner != nil {
		partitioner = rdd.partitioner
	}
	return NewRDD[U](
		rdd.ctx,
		rdd.Partitions,
		func() []Dependency {
			return []Dependency{NewOneToOneDependency(rdd)}
		},
		func(partition Partition) Iterator[U] {
			return fn(partition.Index(), rdd.Compute(partition))
		},
		WithPartitioner[U](partitioner),
	)
}

func Distinct[T comparable](rdd *RDD[T], numPartitions ...int) *RDD[T] {
	np := rdd.GetNumPartitions()
	if len(numPartitions) > 0 && numPartitions[0] > 0 {
		np = numPartitions[0]
	}
	pairRdd := Map(rdd, func(t T) Pair[T, bool] {
		return NewPair(t, true)
	})
	partitioner := NewHashPartitioner(np)
	grouped := ReduceByKey(pairRdd, partitioner, func(a, b bool) bool { return a || b })
	return Map(grouped, func(p Pair[T, bool]) T { return p.Key })
}

func MapValues[K comparable, V any, U any](rdd *RDD[Pair[K, V]], fn func(V) U) *RDD[Pair[K, U]] {
	return Map(rdd, func(p Pair[K, V]) Pair[K, U] {
		return NewPair(p.Key, fn(p.Value))
	})
}

func FlatMapValues[K comparable, V any, U any](rdd *RDD[Pair[K, V]], fn func(V) []U) *RDD[Pair[K, U]] {
	return FlatMap(rdd, func(p Pair[K, V]) []Pair[K, U] {
		results := fn(p.Value)
		pairs := make([]Pair[K, U], len(results))
		for i, u := range results {
			pairs[i] = NewPair(p.Key, u)
		}
		return pairs
	})
}

func Keys[K comparable, V any](rdd *RDD[Pair[K, V]]) *RDD[K] {
	return Map(rdd, func(p Pair[K, V]) K { return p.Key })
}

func Values[K comparable, V any](rdd *RDD[Pair[K, V]]) *RDD[V] {
	return Map(rdd, func(p Pair[K, V]) V { return p.Value })
}

func Glom[T any](rdd *RDD[T]) *RDD[[]T] {
	return MapPartitions(rdd, func(iter Iterator[T]) Iterator[[]T] {
		data := CollectIterator(iter)
		return SliceIterator([][]T{data})
	})
}

func KeyBy[T any, K comparable](rdd *RDD[T], fn func(T) K) *RDD[Pair[K, T]] {
	return Map(rdd, func(t T) Pair[K, T] {
		return NewPair(fn(t), t)
	})
}

func Union[T any](rdds ...*RDD[T]) *RDD[T] {
	if len(rdds) == 0 {
		panic("Union requires at least one RDD")
	}
	if len(rdds) == 1 {
		return rdds[0]
	}

	first := rdds[0]

	type partitionInfo struct {
		parent     RDDAny
		parentPart Partition
	}

	var allParts []partitionInfo
	for _, r := range rdds {
		for _, p := range r.Partitions() {
			allParts = append(allParts, partitionInfo{parent: r, parentPart: p})
		}
	}

	return NewRDD[T](
		first.ctx,
		func() []Partition {
			partitions := make([]Partition, len(allParts))
			for i := range allParts {
				partitions[i] = NewPartition(i)
			}
			return partitions
		},
		func() []Dependency {
			deps := make([]Dependency, len(rdds))
			offset := 0
			for i, r := range rdds {
				numParts := len(r.Partitions())
				start := offset
				r := r
				deps[i] = NewNarrowDep(r, func(pid int) []int {
					return []int{pid - start}
				})
				offset += numParts
			}
			return deps
		},
		func(partition Partition) Iterator[T] {
			idx := partition.Index()
			if idx >= len(allParts) {
				return EmptyIterator[T]()
			}
			info := allParts[idx]
			parentRDD := info.parent.(*RDD[T])
			return parentRDD.Compute(info.parentPart)
		},
	)
}

func NewUnionRDD[T any](rdds ...*RDD[T]) *RDD[T] {
	return Union(rdds...)
}

func Coalesce[T any](rdd *RDD[T], numPartitions int, shuffle ...bool) *RDD[T] {
	doShuffle := false
	if len(shuffle) > 0 {
		doShuffle = shuffle[0]
	}

	if doShuffle {
		return shuffleRepartition(rdd, numPartitions)
	}

	if numPartitions >= rdd.GetNumPartitions() {
		return rdd
	}

	return newCoalescedRDD(rdd, numPartitions)
}

func shuffleRepartition[T any](rdd *RDD[T], numPartitions int) *RDD[T] {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	keyed := MapPartitionsWithIndex(rdd, func(idx int, iter Iterator[T]) Iterator[Pair[int, T]] {
		i := 0
		return func() (Pair[int, T], bool) {
			v, ok := iter()
			if !ok {
				var zero Pair[int, T]
				return zero, false
			}
			key := (idx + i) % numPartitions
			i++
			return NewPair(key, v), true
		}
	})
	sid := rdd.ctx.nextShuffleID()
	p := NewPartitionIdPassthrough(numPartitions)
	return NewRDD[T](rdd.ctx, func() []Partition { return NewPartitions(numPartitions) }, func() []Dependency {
		return []Dependency{NewShuffleDep(keyed, p, sid, false, nil, func(v any) any { return v.(Pair[int, T]).Key })}
	}, func(part Partition) Iterator[T] {
		return MapIterator(readShuffleReduceOutput[int, T](rdd.ctx, sid, part.Index(), len(keyed.Partitions())), func(v Pair[int, T]) T { return v.Value })
	})
}

func Repartition[T any](rdd *RDD[T], numPartitions int) *RDD[T] {
	return Coalesce(rdd, numPartitions, true)
}

func newCoalescedRDD[T any](parent *RDD[T], numPartitions int) *RDD[T] {
	parentParts := parent.Partitions()
	parentLen := len(parentParts)

	if numPartitions > parentLen {
		numPartitions = parentLen
	}

	partitionMapping := make([][]int, numPartitions)
	for i := 0; i < parentLen; i++ {
		targetPart := i % numPartitions
		partitionMapping[targetPart] = append(partitionMapping[targetPart], i)
	}

	return NewRDD[T](
		parent.ctx,
		func() []Partition {
			return NewPartitions(numPartitions)
		},
		func() []Dependency {
			return []Dependency{NewNarrowDep(parent, func(pid int) []int {
				if pid < len(partitionMapping) {
					return partitionMapping[pid]
				}
				return []int{pid}
			})}
		},
		func(partition Partition) Iterator[T] {
			parentPartIdxs := partitionMapping[partition.Index()]
			var iters []Iterator[T]
			for _, idx := range parentPartIdxs {
				if idx < len(parentParts) {
					iters = append(iters, parent.Compute(parentParts[idx]))
				}
			}
			return ChainIterators(iters)
		},
	)
}

func Sample[T any](rdd *RDD[T], withReplacement bool, fraction float64, seed ...int64) *RDD[T] {
	seedVal := int64(1)
	if len(seed) > 0 {
		seedVal = seed[0]
	}

	return MapPartitionsWithIndex(rdd, func(idx int, iter Iterator[T]) Iterator[T] {
		localRng := NewSimpleRNG(seedVal + int64(idx))
		return FilterIterator(iter, func(t T) bool {
			if withReplacement {
				return localRng.Float64() < fraction
			}
			return localRng.Float64() < fraction
		})
	})
}

// RandomSplit assigns every input occurrence to exactly one split. Weights must
// be finite and non-negative, with at least one positive weight.
func RandomSplit[T any](rdd *RDD[T], weights []float64, seed int64) []*RDD[T] {
	if len(weights) == 0 {
		panic("RandomSplit requires at least one weight")
	}
	total := 0.0
	for _, weight := range weights {
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			panic("RandomSplit weights must be finite and non-negative")
		}
		total += weight
	}
	if total == 0 || math.IsInf(total, 0) {
		panic("RandomSplit requires a finite, positive total weight")
	}
	// Normalizing before adding avoids cumulative overflow for large weights.
	boundaries := make([]float64, len(weights))
	var cumulative float64
	for i, weight := range weights {
		cumulative += weight / total
		boundaries[i] = cumulative
	}
	boundaries[len(boundaries)-1] = 1

	splits := make([]*RDD[T], len(weights))
	for i := range splits {
		split := i
		splits[i] = MapPartitionsWithIndex(rdd, func(idx int, iter Iterator[T]) Iterator[T] {
			rng := rand.New(rand.NewSource(seed + int64(idx)))
			return FilterIterator(iter, func(_ T) bool {
				value := rng.Float64()
				bucket := 0
				for bucket < len(boundaries)-1 && value >= boundaries[bucket] {
					bucket++
				}
				return bucket == split
			})
		})
	}
	return splits
}

func Zip[T any, U any](rdd1 *RDD[T], rdd2 *RDD[U]) *RDD[Pair[T, U]] {
	if len(rdd1.Partitions()) != len(rdd2.Partitions()) {
		panic("Can only zip RDDs with same number of partitions")
	}

	return NewRDD[Pair[T, U]](
		rdd1.ctx,
		rdd1.Partitions,
		func() []Dependency {
			return []Dependency{
				NewOneToOneDependency(rdd1),
				NewOneToOneDependency(rdd2),
			}
		},
		func(partition Partition) Iterator[Pair[T, U]] {
			iter1 := rdd1.Compute(partition)
			iter2 := rdd2.Compute(partition)
			return func() (Pair[T, U], bool) {
				v1, ok1 := iter1()
				v2, ok2 := iter2()
				if !ok1 || !ok2 {
					var zero Pair[T, U]
					return zero, false
				}
				return NewPair(v1, v2), true
			}
		},
	)
}

func Cartesian[T any, U any](rdd1 *RDD[T], rdd2 *RDD[U]) *RDD[Pair[T, U]] {
	return newCartesianRDD(rdd1, rdd2)
}

func Subtract[T comparable](rdd1 *RDD[T], rdd2 *RDD[T]) *RDD[T] {
	other := Collect(rdd2)
	otherSet := make(map[T]bool)
	for _, v := range other {
		otherSet[v] = true
	}
	return Filter(rdd1, func(t T) bool {
		return !otherSet[t]
	})
}

func Intersection[T comparable](rdd1 *RDD[T], rdd2 *RDD[T]) *RDD[T] {
	otherSet := make(map[T]bool)
	for _, v := range Collect(rdd2) {
		otherSet[v] = true
	}
	return Distinct(Filter(rdd1, func(t T) bool {
		return otherSet[t]
	}))
}

type simpleRNG struct {
	state int64
}

func NewSimpleRNG(seed int64) *simpleRNG {
	return &simpleRNG{state: seed}
}

func (r *simpleRNG) Float64() float64 {
	r.state = r.state*1103515245 + 12345
	return float64(uint64(r.state)>>16) / float64(1<<47)
}
