package spark

func newCartesianRDD[T any, U any](rdd1 *RDD[T], rdd2 *RDD[U]) *RDD[Pair[T, U]] {
	r1Parts := rdd1.Partitions()
	r2Parts := rdd2.Partitions()
	n1 := len(r1Parts)
	n2 := len(r2Parts)
	numPartitions := n1 * n2
	if numPartitions == 0 {
		return newEmptyRDD[Pair[T, U]](rdd1.ctx)
	}

	return NewRDD[Pair[T, U]](
		rdd1.ctx,
		func() []Partition {
			parts := make([]Partition, numPartitions)
			idx := 0
			for i := 0; i < n1; i++ {
				for j := 0; j < n2; j++ {
					parts[idx] = &cartesianPartition{index: idx, leftIdx: i, rightIdx: j}
					idx++
				}
			}
			return parts
		},
		func() []Dependency {
			return []Dependency{
				NewNarrowDep(rdd1, func(pid int) []int {
					return []int{pid / n2}
				}),
				NewNarrowDep(rdd2, func(pid int) []int {
					return []int{pid % n2}
				}),
			}
		},
		func(partition Partition) Iterator[Pair[T, U]] {
			cp, ok := partition.(*cartesianPartition)
			if !ok {
				return EmptyIterator[Pair[T, U]]()
			}
			if cp.leftIdx >= n1 || cp.rightIdx >= n2 {
				return EmptyIterator[Pair[T, U]]()
			}
			leftData := CollectIterator(rdd1.Compute(r1Parts[cp.leftIdx]))
			rightData := CollectIterator(rdd2.Compute(r2Parts[cp.rightIdx]))
			result := make([]Pair[T, U], 0, len(leftData)*len(rightData))
			for _, l := range leftData {
				for _, r := range rightData {
					result = append(result, NewPair(l, r))
				}
			}
			return SliceIterator(result)
		},
	)
}
