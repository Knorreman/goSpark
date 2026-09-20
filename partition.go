package spark

type Partition interface {
	Index() int
}

type PartitionSlice []Partition

func (ps PartitionSlice) Indices() []int {
	indices := make([]int, len(ps))
	for i, p := range ps {
		indices[i] = p.Index()
	}
	return indices
}

type basePartition struct {
	index int
}

func (p *basePartition) Index() int { return p.index }

func NewPartition(index int) Partition {
	return &basePartition{index: index}
}

func NewPartitions(n int) []Partition {
	partitions := make([]Partition, n)
	for i := 0; i < n; i++ {
		partitions[i] = NewPartition(i)
	}
	return partitions
}

type rangePartition struct {
	index int
	start int
	end   int
}

func (p *rangePartition) Index() int { return p.index }
func (p *rangePartition) Start() int { return p.start }
func (p *rangePartition) End() int   { return p.end }

func NewRangePartition(index, start, end int) Partition {
	return &rangePartition{index: index, start: start, end: end}
}

type cartesianPartition struct {
	index    int
	leftIdx  int
	rightIdx int
}

func (p *cartesianPartition) Index() int      { return p.index }
func (p *cartesianPartition) LeftIndex() int  { return p.leftIdx }
func (p *cartesianPartition) RightIndex() int { return p.rightIdx }
