package spark

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
	return aggregatePairs[K, V, V](ctx, shuffleID, reduceID, numMaps, func(v V) V { return v }, fn, ctx.groupBytes())
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
	return boundedGroups[K, V](ctx, shuffleID, reduceID, numMaps)
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

// FullOuterJoin emits every matching pair and every unmatched value, with nil on the absent side.
func FullOuterJoin[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[*V, *W]]] {
	return NewFullOuterJoinRDD(rdd1, rdd2, partitioner)
}

// SubtractByKey keeps all left records whose key is absent from the right RDD.
func SubtractByKey[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, V]] {
	return NewSubtractByKeyRDD(rdd1, rdd2, partitioner)
}

func NewJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return MapPartitions(cogrouped, func(groups Iterator[Pair[K, Pair[[]V, []W]]]) Iterator[Pair[K, Pair[V, W]]] {
		return expandIterator(groups, func(p Pair[K, Pair[[]V, []W]]) Iterator[Pair[K, Pair[V, W]]] {
			i, j := 0, 0
			return func() (Pair[K, Pair[V, W]], bool) {
				if i >= len(p.Value.Key) || len(p.Value.Value) == 0 {
					return Pair[K, Pair[V, W]]{}, false
				}
				out := NewPair(p.Key, NewPair(p.Value.Key[i], p.Value.Value[j]))
				j++
				if j == len(p.Value.Value) {
					j = 0
					i++
				}
				return out, true
			}
		})
	})
}

func NewLeftOuterJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[V, *W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return MapPartitions(cogrouped, func(groups Iterator[Pair[K, Pair[[]V, []W]]]) Iterator[Pair[K, Pair[V, *W]]] {
		return expandIterator(groups, func(p Pair[K, Pair[[]V, []W]]) Iterator[Pair[K, Pair[V, *W]]] {
			i, j := 0, 0
			return func() (Pair[K, Pair[V, *W]], bool) {
				if i >= len(p.Value.Key) {
					return Pair[K, Pair[V, *W]]{}, false
				}
				v := p.Value.Key[i]
				var w *W
				if len(p.Value.Value) > 0 {
					x := p.Value.Value[j]
					w = &x
					j++
					if j == len(p.Value.Value) {
						j = 0
						i++
					}
				} else {
					i++
				}
				return NewPair(p.Key, NewPair(v, w)), true
			}
		})
	})
}

func NewRightOuterJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[*V, W]]] {
	left := NewLeftOuterJoinRDD(rdd2, rdd1, partitioner)
	return Map(left, func(p Pair[K, Pair[W, *V]]) Pair[K, Pair[*V, W]] {
		return NewPair(p.Key, NewPair(p.Value.Value, p.Value.Key))
	})
}

func NewFullOuterJoinRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, Pair[*V, *W]]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return MapPartitions(cogrouped, func(groups Iterator[Pair[K, Pair[[]V, []W]]]) Iterator[Pair[K, Pair[*V, *W]]] {
		return expandIterator(groups, func(p Pair[K, Pair[[]V, []W]]) Iterator[Pair[K, Pair[*V, *W]]] {
			i, j := 0, 0
			return func() (Pair[K, Pair[*V, *W]], bool) {
				if len(p.Value.Key) == 0 {
					if j >= len(p.Value.Value) {
						return Pair[K, Pair[*V, *W]]{}, false
					}
					w := p.Value.Value[j]
					j++
					return NewPair(p.Key, NewPair((*V)(nil), &w)), true
				}
				if i >= len(p.Value.Key) {
					return Pair[K, Pair[*V, *W]]{}, false
				}
				v := p.Value.Key[i]
				var w *W
				if len(p.Value.Value) > 0 {
					x := p.Value.Value[j]
					w = &x
					j++
					if j == len(p.Value.Value) {
						j = 0
						i++
					}
				} else {
					i++
				}
				return NewPair(p.Key, NewPair(&v, w)), true
			}
		})
	}, true)
}

func NewSubtractByKeyRDD[K comparable, V any, W any](rdd1 *RDD[Pair[K, V]], rdd2 *RDD[Pair[K, W]], partitioner Partitioner) *RDD[Pair[K, V]] {
	cogrouped := Cogroup(rdd1, rdd2, partitioner)
	return MapPartitions(cogrouped, func(groups Iterator[Pair[K, Pair[[]V, []W]]]) Iterator[Pair[K, V]] {
		return expandIterator(groups, func(p Pair[K, Pair[[]V, []W]]) Iterator[Pair[K, V]] {
			if len(p.Value.Value) != 0 {
				return EmptyIterator[Pair[K, V]]()
			}
			return MapIterator(SliceIterator(p.Value.Key), func(v V) Pair[K, V] {
				return NewPair(p.Key, v)
			})
		})
	}, true)
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
			g1, g2 := grouped1.Compute(NewPartition(idx)), grouped2.Compute(NewPartition(idx))
			a, oka := g1()
			b, okb := g2()
			return func() (Pair[K, Pair[[]V, []W]], bool) {
				if !oka && !okb {
					return Pair[K, Pair[[]V, []W]]{}, false
				}
				if oka && okb && a.Key == b.Key {
					out := NewPair(a.Key, NewPair(a.Value, b.Value))
					a, oka = g1()
					b, okb = g2()
					return out, true
				}
				if !okb || (oka && keyToken(a.Key) < keyToken(b.Key)) {
					out := NewPair(a.Key, NewPair(a.Value, []W(nil)))
					a, oka = g1()
					return out, true
				}
				out := NewPair(b.Key, NewPair([]V(nil), b.Value))
				b, okb = g2()
				return out, true
			}
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
	registerRecord(*new(K)) // sample keys travel over the worker RPC as gob interfaces
	sid := rdd.ctx.nextShuffleID()
	compare := func(a, b any) bool {
		if ascending {
			return less(a.(K), b.(K))
		}
		return less(b.(K), a.(K))
	}
	partitioner := NewRangePartitioner(np, compare)
	dep := NewShuffleDep(rdd, partitioner, sid, false, nil, func(v any) any { return v.(Pair[K, V]).Key })
	dep.sortLess = compare
	return NewRDD[Pair[K, V]](rdd.ctx,
		func() []Partition { return NewPartitions(np) },
		func() []Dependency { return []Dependency{dep} },
		func(p Partition) Iterator[Pair[K, V]] {
			stream, _ := externalSort(rdd.ctx, shuffleStream(rdd.ctx, sid, p.Index(), len(rdd.Partitions())), func(a, b any) bool {
				return compare(a.(Pair[K, V]).Key, b.(Pair[K, V]).Key)
			})
			return iteratorFromStream[Pair[K, V]](rdd.ctx, stream)
		})
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
			return aggregatePairs[K, V, C](ctx, shuffleID, partition.Index(), len(parent.Partitions()), createCombiner, mergeValue, ctx.groupBytes())
		},
		WithPartitioner[Pair[K, C]](partitioner),
	)
}
