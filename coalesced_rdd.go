package spark

func NewCoalescedRDD[T any](parent *RDD[T], numPartitions int, shuffle bool) *RDD[T] {
	if shuffle {
		return Coalesce(parent, numPartitions, true)
	}
	return newCoalescedRDD(parent, numPartitions)
}
