package spark

type DepType int

const (
	DepNarrow DepType = iota
	DepShuffle
)

type Dependency interface {
	DepType() DepType
	Parent() RDDAny
	GetParents(partitionID int) []int
}

type OneToOneDependency struct {
	parent RDDAny
}

func NewOneToOneDependency(parent RDDAny) *OneToOneDependency {
	return &OneToOneDependency{parent: parent}
}

func (d *OneToOneDependency) DepType() DepType         { return DepNarrow }
func (d *OneToOneDependency) Parent() RDDAny           { return d.parent }
func (d *OneToOneDependency) GetParents(pid int) []int { return []int{pid} }

type RangeDependency struct {
	parent      RDDAny
	parentStart int
	childStart  int
	length      int
}

func NewRangeDependency(parent RDDAny, parentStart, childStart, length int) *RangeDependency {
	return &RangeDependency{
		parent:      parent,
		parentStart: parentStart,
		childStart:  childStart,
		length:      length,
	}
}

func (d *RangeDependency) DepType() DepType { return DepNarrow }
func (d *RangeDependency) Parent() RDDAny   { return d.parent }
func (d *RangeDependency) GetParents(partitionID int) []int {
	offset := partitionID - d.childStart
	if 0 <= offset && offset < d.length {
		return []int{d.parentStart + offset}
	}
	return nil
}

type NarrowDep struct {
	parent       RDDAny
	getParentsFn func(partitionID int) []int
}

func NewNarrowDep(parent RDDAny, getParentsFn func(partitionID int) []int) *NarrowDep {
	return &NarrowDep{parent: parent, getParentsFn: getParentsFn}
}

func (d *NarrowDep) DepType() DepType         { return DepNarrow }
func (d *NarrowDep) Parent() RDDAny           { return d.parent }
func (d *NarrowDep) GetParents(pid int) []int { return d.getParentsFn(pid) }

type ShuffleDep struct {
	parent         RDDAny
	partitioner    Partitioner
	shuffleID      int
	mapSideCombine bool
	aggregator     *AggregatorAny
	keyExtractor   func(any) any
}

func NewShuffleDep(parent RDDAny, partitioner Partitioner, shuffleID int, mapSideCombine bool, agg *AggregatorAny, keyExtractor ...func(any) any) *ShuffleDep {
	var ke func(any) any
	if len(keyExtractor) > 0 {
		ke = keyExtractor[0]
	}
	return &ShuffleDep{
		parent:         parent,
		partitioner:    partitioner,
		shuffleID:      shuffleID,
		mapSideCombine: mapSideCombine,
		aggregator:     agg,
		keyExtractor:   ke,
	}
}

func (d *ShuffleDep) DepType() DepType { return DepShuffle }
func (d *ShuffleDep) Parent() RDDAny   { return d.parent }
func (d *ShuffleDep) ShuffleID() int   { return d.shuffleID }
func (d *ShuffleDep) GetPartitioner() Partitioner {
	return d.partitioner
}
func (d *ShuffleDep) GetParents(partitionID int) []int {
	return nil
}

func (d *ShuffleDep) ExtractKey(item any) any {
	if d.keyExtractor != nil {
		return d.keyExtractor(item)
	}
	return nil
}

func (d *ShuffleDep) MapSideCombine() bool { return d.mapSideCombine }

func (d *ShuffleDep) Aggregator() *AggregatorAny { return d.aggregator }

type AggregatorAny struct {
	CreateCombiner func(any) any
	MergeValue     func(any, any) any
	MergeCombiners func(any, any) any
}
