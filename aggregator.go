package spark

type Aggregator[K comparable, V any, C any] struct {
	CreateCombiner func(V) C
	MergeValue     func(C, V) C
	MergeCombiners func(C, C) C
}

func NewAggregator[K comparable, V any, C any](
	createCombiner func(V) C,
	mergeValue func(C, V) C,
	mergeCombiners func(C, C) C,
) *Aggregator[K, V, C] {
	return &Aggregator[K, V, C]{
		CreateCombiner: createCombiner,
		MergeValue:     mergeValue,
		MergeCombiners: mergeCombiners,
	}
}
