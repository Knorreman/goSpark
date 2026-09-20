package spark

import (
	"fmt"
	"hash/fnv"
	"sort"
)

type Partitioner interface {
	NumPartitions() int
	GetPartition(key any) int
}

type HashPartitioner struct {
	NumPartitions_ int
}

func NewHashPartitioner(numPartitions int) *HashPartitioner {
	return &HashPartitioner{NumPartitions_: numPartitions}
}

func (p *HashPartitioner) NumPartitions() int {
	return p.NumPartitions_
}

func (p *HashPartitioner) GetPartition(key any) int {
	if key == nil {
		return 0
	}
	h := fnv.New32a()
	switch k := key.(type) {
	case string:
		h.Write([]byte(k))
	case int:
		var buf [4]byte
		buf[0] = byte(k)
		buf[1] = byte(k >> 8)
		buf[2] = byte(k >> 16)
		buf[3] = byte(k >> 24)
		h.Write(buf[:])
	case int32:
		var buf [4]byte
		buf[0] = byte(k)
		buf[1] = byte(k >> 8)
		buf[2] = byte(k >> 16)
		buf[3] = byte(k >> 24)
		h.Write(buf[:])
	case int64:
		var buf [8]byte
		for i := 0; i < 8; i++ {
			buf[i] = byte(k >> (i * 8))
		}
		h.Write(buf[:])
	case float64:
		var buf [8]byte
		for i := 0; i < 8; i++ {
			buf[i] = byte(uint64(k) >> (i * 8))
		}
		h.Write(buf[:])
	default:
		h.Write([]byte(fmt.Sprintf("%v", key)))
	}
	return int(h.Sum32()) % p.NumPartitions_
}

type RangePartitioner struct {
	NumPartitions_ int
	rangeBounds    []any
	lessFunc       func(a, b any) bool
}

func NewRangePartitioner(numPartitions int, lessFunc func(a, b any) bool) *RangePartitioner {
	return &RangePartitioner{
		NumPartitions_: numPartitions,
		lessFunc:       lessFunc,
	}
}

func (p *RangePartitioner) NumPartitions() int {
	return p.NumPartitions_
}

func (p *RangePartitioner) GetPartition(key any) int {
	if p.rangeBounds == nil {
		return 0
	}
	idx := sort.Search(len(p.rangeBounds), func(i int) bool {
		return p.lessFunc(key, p.rangeBounds[i])
	})
	if idx >= p.NumPartitions_ {
		idx = p.NumPartitions_ - 1
	}
	return idx
}

func (p *RangePartitioner) SetRangeBounds(bounds []any) {
	p.rangeBounds = bounds
}

type PartitionIdPassthrough struct {
	NumPartitions_ int
}

func NewPartitionIdPassthrough(numPartitions int) *PartitionIdPassthrough {
	return &PartitionIdPassthrough{NumPartitions_: numPartitions}
}

func (p *PartitionIdPassthrough) NumPartitions() int {
	return p.NumPartitions_
}

func (p *PartitionIdPassthrough) GetPartition(key any) int {
	if n, ok := key.(int); ok && n >= 0 && n < p.NumPartitions_ {
		return n
	}
	return 0
}
