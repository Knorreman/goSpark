package spark

import "sort"

func ReduceByKey[K comparable, V any](rdd *RDD[Pair[K, V]], partitioner Partitioner, fn func(V, V) V) *RDD[Pair[K, V]] {
	return NewReduceByKeyRDD(rdd, partitioner, fn)
}

func GroupByKey[K comparable, V any](rdd *RDD[Pair[K, V]], partitioner Partitioner) *RDD[Pair[K, []V]] {
	return NewGroupByKeyRDD(rdd, partitioner)
}

func CountByKey[K comparable, V any](rdd *RDD[Pair[K, V]]) map[K]int64 {
	data := Collect(rdd)
	result := make(map[K]int64)
	for _, p := range data {
		result[p.Key]++
	}
	return result
}

type reduceByKeyRDD[K comparable, V any] struct {
	rdd         *RDD[Pair[K, V]]
	partitioner Partitioner
	reduceFn    func(V, V) V
	shuffleID   int
}

func NewReduceByKeyRDD[K comparable, V any](rdd *RDD[Pair[K, V]], partitioner Partitioner, fn func(V, V) V) *RDD[Pair[K, V]] {
	shuffleID := rdd.ctx.nextShuffleID()
	parent := rdd
	r := &reduceByKeyRDD[K, V]{
		rdd:         parent,
		partitioner: partitioner,
		reduceFn:    fn,
		shuffleID:   shuffleID,
	}
	var ke func(any) any = func(item any) any {
		if pair, ok := item.(Pair[K, V]); ok {
			return pair.Key
		}
		return nil
	}
	agg := &AggregatorAny{
		CreateCombiner: func(v any) any { return v },
		MergeValue: func(c, v any) any {
			return fn(c.(V), v.(V))
		},
		MergeCombiners: func(a, b any) any {
			return fn(a.(V), b.(V))
		},
	}

	return NewRDD[Pair[K, V]](
		parent.ctx,
		func() []Partition {
			return NewPartitions(partitioner.NumPartitions())
		},
		func() []Dependency {
			return []Dependency{
				NewShuffleDep(parent, partitioner, shuffleID, true, agg, ke),
			}
		},
		func(partition Partition) Iterator[Pair[K, V]] {
			return newReduceShuffleRead[K, V](parent.ctx, shuffleID, partition.Index(), len(parent.Partitions()), r.reduceFn)
		},
		WithPartitioner[Pair[K, V]](partitioner),
	)
}

func newReduceShuffleRead[K comparable, V any](ctx *Context, shuffleID int, reduceID int, numMaps int, fn func(V, V) V) Iterator[Pair[K, V]] {
	blocks := ctx.ShuffleManager().ReadReduceOutput(shuffleID, reduceID, numMaps)
	grouped := make(map[K]V)
	found := make(map[K]bool)
	for _, block := range blocks {
		for _, item := range block {
			pair, ok := item.(Pair[K, V])
			if !ok {
				continue
			}
			if found[pair.Key] {
				grouped[pair.Key] = fn(grouped[pair.Key], pair.Value)
			} else {
				grouped[pair.Key] = pair.Value
				found[pair.Key] = true
			}
		}
	}
	result := make([]Pair[K, V], 0, len(grouped))
	for k, v := range grouped {
		result = append(result, NewPair(k, v))
	}
	return SliceIterator(result)
}

type groupByKeyRDD[K comparable, V any] struct {
	rdd         *RDD[Pair[K, V]]
	partitioner Partitioner
	shuffleID   int
}

func NewGroupByKeyRDD[K comparable, V any](rdd *RDD[Pair[K, V]], partitioner Partitioner) *RDD[Pair[K, []V]] {
	shuffleID := rdd.ctx.nextShuffleID()
	parent := rdd
	var ke func(any) any = func(item any) any {
		if pair, ok := item.(Pair[K, V]); ok {
			return pair.Key
		}
		return nil
	}

	return NewRDD[Pair[K, []V]](
		parent.ctx,
		func() []Partition {
			return NewPartitions(partitioner.NumPartitions())
		},
		func() []Dependency {
			return []Dependency{
				NewShuffleDep(parent, partitioner, shuffleID, false, nil, ke),
			}
		},
		func(partition Partition) Iterator[Pair[K, []V]] {
			return groupShuffleRead[K, V](parent.ctx, shuffleID, partition.Index(), len(parent.Partitions()))
		},
		WithPartitioner[Pair[K, []V]](partitioner),
	)
}

func groupShuffleRead[K comparable, V any](ctx *Context, shuffleID int, reduceID int, numMaps int) Iterator[Pair[K, []V]] {
	blocks := ctx.ShuffleManager().ReadReduceOutput(shuffleID, reduceID, numMaps)
	grouped := make(map[K][]V)
	for _, block := range blocks {
		for _, item := range block {
			pair, ok := item.(Pair[K, V])
			if !ok {
				continue
			}
			grouped[pair.Key] = append(grouped[pair.Key], pair.Value)
		}
	}
	result := make([]Pair[K, []V], 0, len(grouped))
	for k, v := range grouped {
		result = append(result, NewPair(k, v))
	}
	return SliceIterator(result)
}

func Join[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, W]]] {
	return NewJoinRDD(rdd1, rdd2, partitioner)
}

func LeftOuterJoin[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, *W]]] {
	return NewLeftOuterJoinRDD(rdd1, rdd2, partitioner)
}

func RightOuterJoin[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[*V, W]]] {
	return NewRightOuterJoinRDD(rdd1, rdd2, partitioner)
}

func NewJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return FlatMap(cogrouped, func(p Pair[K, Pair[[]V, []W]]) []Pair[K, Pair[V, W]] {
		var result []Pair[K, Pair[V, W]]
		for _, v := range p.Value.Key {
			for _, w := range p.Value.Value {
				result = append(result, NewPair(p.Key, NewPair(v, w)))
			}
		}
		return result
	})
}

func NewLeftOuterJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, *W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return FlatMap(cogrouped, func(p Pair[K, Pair[[]V, []W]]) []Pair[K, Pair[V, *W]] {
		var result []Pair[K, Pair[V, *W]]
		vs := p.Value.Key
		ws := p.Value.Value
		for _, v := range vs {
			if len(ws) > 0 {
				for _, w := range ws {
					w := w
					result = append(result, NewPair(p.Key, NewPair(v, &w)))
				}
			} else {
				result = append(result, NewPair(p.Key, NewPair(v, (*W)(nil))))
			}
		}
		return result
	})
}

func NewRightOuterJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[*V, W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return FlatMap(cogrouped, func(p Pair[K, Pair[[]V, []W]]) []Pair[K, Pair[*V, W]] {
		var result []Pair[K, Pair[*V, W]]
		vs := p.Value.Key
		ws := p.Value.Value
		for _, w := range ws {
			if len(vs) > 0 {
				for _, v := range vs {
					v := v
					result = append(result, NewPair(p.Key, NewPair(&v, w)))
				}
			} else {
				result = append(result, NewPair(p.Key, NewPair((*V)(nil), w)))
			}
		}
		return result
	})
}

func Cogroup[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[[]V, []W]]] {
	return newCogroupRDD(rdd1, rdd2, partitioner)
}

func newCogroupRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[[]V, []W]]] {
	ctx := rdd1.ctx
	grouped1 := GroupByKey(rdd1, partitioner)
	grouped2 := GroupByKey(rdd2, partitioner)
	n := partitioner.NumPartitions()

	return NewRDD[Pair[K, Pair[[]V, []W]]](
		ctx,
		func() []Partition {
			return NewPartitions(n)
		},
		func() []Dependency {
			return []Dependency{
				NewOneToOneDependency(grouped1),
				NewOneToOneDependency(grouped2),
			}
		},
		func(partition Partition) Iterator[Pair[K, Pair[[]V, []W]]] {
			idx := partition.Index()
			g1 := CollectIterator(grouped1.Compute(NewPartition(idx)))
			g2 := CollectIterator(grouped2.Compute(NewPartition(idx)))

			combined := make(map[K]Pair[[]V, []W])
			for _, p := range g1 {
				val := combined[p.Key]
				val.Key = p.Value
				combined[p.Key] = val
			}
			for _, p := range g2 {
				val := combined[p.Key]
				val.Value = p.Value
				combined[p.Key] = val
			}

			result := make([]Pair[K, Pair[[]V, []W]], 0, len(combined))
			for k, v := range combined {
				result = append(result, NewPair(k, v))
			}
			return SliceIterator(result)
		},
		WithPartitioner[Pair[K, Pair[[]V, []W]]](partitioner),
	)
}

func SortByKey[K comparable, V any](rdd *RDD[Pair[K, V]], less func(K, K) bool, ascending bool, numPartitions ...int) *RDD[Pair[K, V]] {
	np := rdd.GetNumPartitions()
	if len(numPartitions) > 0 && numPartitions[0] > 0 {
		np = numPartitions[0]
	}
	if np == 0 {
		np = 1
	}
	// Correctness-first global sort: all map outputs are available to each
	// result task. Range sampling and external merge sorting are future work.
	sid := rdd.ctx.nextShuffleID()
	return NewRDD[Pair[K, V]](rdd.ctx,
		func() []Partition { return NewPartitions(np) },
		func() []Dependency { return []Dependency{NewShuffleDep(rdd, NewHashPartitioner(1), sid, false, nil)} },
		func(p Partition) Iterator[Pair[K, V]] {
			data := CollectIterator(readShuffleReduceOutput[K, V](rdd.ctx, sid, 0, len(rdd.Partitions())))
			sort.SliceStable(data, func(i, j int) bool {
				if ascending {
					return less(data[i].Key, data[j].Key)
				}
				return less(data[j].Key, data[i].Key)
			})
			return SliceIterator(data[p.Index()*len(data)/np : (p.Index()+1)*len(data)/np])
		})
}

func sortSlice[K comparable, V any](data []Pair[K, V], less func(i, j int) bool) {
	n := len(data)
	for i := 0; i < n-1; i++ {
		for j := i + 1; j < n; j++ {
			if less(j, i) {
				data[i], data[j] = data[j], data[i]
			}
		}
	}
}

func AggregateByKey[K comparable, V any, U any](rdd *RDD[Pair[K, V]], zeroValue U, seqOp func(U, V) U, combOp func(U, U) U, partitioner Partitioner) *RDD[Pair[K, U]] {
	mapped := Map(rdd, func(p Pair[K, V]) Pair[K, U] {
		return NewPair(p.Key, seqOp(zeroValue, p.Value))
	})
	return ReduceByKey(mapped, partitioner, combOp)
}

func FoldByKey[K comparable, V any](rdd *RDD[Pair[K, V]], zeroValue V, fn func(V, V) V, partitioner Partitioner) *RDD[Pair[K, V]] {
	return AggregateByKey(rdd, zeroValue, fn, fn, partitioner)
}

func CombineByKey[K comparable, V any, C any](
	rdd *RDD[Pair[K, V]],
	createCombiner func(V) C,
	mergeValue func(C, V) C,
	mergeCombiners func(C, C) C,
	partitioner Partitioner,
) *RDD[Pair[K, C]] {
	return newCombineByKeyRDD(rdd, createCombiner, mergeValue, mergeCombiners, partitioner)
}

func newCombineByKeyRDD[K comparable, V any, C any](
	rdd *RDD[Pair[K, V]],
	createCombiner func(V) C,
	mergeValue func(C, V) C,
	mergeCombiners func(C, C) C,
	partitioner Partitioner,
) *RDD[Pair[K, C]] {
	parent := rdd
	ctx := parent.ctx
	shuffleID := ctx.nextShuffleID()
	var ke func(any) any = func(item any) any {
		if pair, ok := item.(Pair[K, V]); ok {
			return pair.Key
		}
		return nil
	}

	return NewRDD[Pair[K, C]](
		ctx,
		func() []Partition {
			return NewPartitions(partitioner.NumPartitions())
		},
		func() []Dependency {
			return []Dependency{
				NewShuffleDep(parent, partitioner, shuffleID, false, nil, ke),
			}
		},
		func(partition Partition) Iterator[Pair[K, C]] {
			blocks := ctx.ShuffleManager().ReadReduceOutput(shuffleID, partition.Index(), len(parent.Partitions()))
			combined := make(map[K]C)
			found := make(map[K]bool)
			for _, block := range blocks {
				for _, item := range block {
					pair, ok := item.(Pair[K, V])
					if !ok {
						continue
					}
					if found[pair.Key] {
						combined[pair.Key] = mergeValue(combined[pair.Key], pair.Value)
					} else {
						combined[pair.Key] = createCombiner(pair.Value)
						found[pair.Key] = true
					}
				}
			}
			result := make([]Pair[K, C], 0, len(combined))
			for k, v := range combined {
				result = append(result, NewPair(k, v))
			}
			return SliceIterator(result)
		},
		WithPartitioner[Pair[K, C]](partitioner),
	)
}
