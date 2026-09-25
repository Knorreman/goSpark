package spark

import (
	"sync"
)

type RDDAny interface {
	Partitions() []Partition
	Dependencies() []Dependency
	ComputeAny(partition Partition) IteratorAny
	GetPartitioner() Partitioner
	ID() int
	Ctx() *Context
	IsCached() bool
	Name() string
}

type IteratorAny = func() (any, bool)

type RDD[T any] struct {
	id              int
	ctx             *Context
	partitionsFn    func() []Partition
	dependenciesFn  func() []Dependency
	computeFn       func(partition Partition) Iterator[T]
	partitioner     Partitioner
	preferredLocsFn func(partition Partition) []string
	name            string
	storageLevel    StorageLevel

	mu             sync.Once
	cachedParts    []Partition
	checkpointRun  sync.Mutex
	checkpointMu   sync.RWMutex
	checkpointPath string
}

func NewRDD[T any](
	ctx *Context,
	partitionsFn func() []Partition,
	dependenciesFn func() []Dependency,
	computeFn func(partition Partition) Iterator[T],
	opts ...RDDOpt[T],
) *RDD[T] {
	var zero T
	registerRecord(zero)
	r := &RDD[T]{
		id:             ctx.nextRDDID(),
		ctx:            ctx,
		partitionsFn:   partitionsFn,
		dependenciesFn: dependenciesFn,
		computeFn:      computeFn,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type RDDOpt[T any] func(*RDD[T])

func WithPartitioner[T any](p Partitioner) RDDOpt[T] {
	return func(r *RDD[T]) { r.partitioner = p }
}

func WithPreferredLocs[T any](fn func(partition Partition) []string) RDDOpt[T] {
	return func(r *RDD[T]) { r.preferredLocsFn = fn }
}

func WithName[T any](name string) RDDOpt[T] {
	return func(r *RDD[T]) { r.name = name }
}

func (r *RDD[T]) ID() int { return r.id }

func (r *RDD[T]) Ctx() *Context { return r.ctx }

func (r *RDD[T]) Partitions() []Partition {
	r.mu.Do(func() {
		if r.partitionsFn != nil {
			r.cachedParts = r.partitionsFn()
		}
	})
	return r.cachedParts
}

func (r *RDD[T]) Dependencies() []Dependency {
	r.checkpointMu.RLock()
	defer r.checkpointMu.RUnlock()
	if r.checkpointPath != "" {
		return nil
	}
	if r.dependenciesFn != nil {
		return r.dependenciesFn()
	}
	return nil
}

func (r *RDD[T]) Compute(partition Partition) Iterator[T] {
	r.ctx.checkCanceled()
	it := r.computePartition(partition)
	return func() (T, bool) { r.ctx.checkCanceled(); v, ok := it(); r.ctx.checkCanceled(); return v, ok }
}

func (r *RDD[T]) computePartition(partition Partition) Iterator[T] {
	r.checkpointMu.RLock()
	path := r.checkpointPath
	r.checkpointMu.RUnlock()
	if path != "" {
		return checkpointIterator[T](r.ctx, path, partition.Index())
	}
	level := r.storageLevel
	if level == StorageNone || r.ctx == nil {
		return r.computeFn(partition)
	}
	idx := partition.Index()
	shared := r.ctx.distributedCache != nil && cacheAcrossTasks(r)
	if (level == StorageMemory || level == StorageMemoryAndDisk) && shared {
		if cached, ok := r.ctx.distributedCache.get(r.id, idx); ok {
			r.ctx.noteCachePresent(CachePartition{RDDID: r.id, PartitionID: idx})
			return cachedIterator[T](cached)
		}
	}
	if (level == StorageMemory || level == StorageMemoryAndDisk) && r.ctx.cache != nil {
		if cached, ok := r.ctx.cache.get(r.id, idx); ok {
			return cachedIterator[T](cached)
		}
	}
	if (level == StorageDisk || level == StorageMemoryAndDisk) && r.ctx.disk != nil {
		if cached, ok := r.ctx.disk.get(r.id, idx); ok {
			if level == StorageMemoryAndDisk && r.ctx.cache != nil {
				r.ctx.cache.put(r.id, idx, cached)
			}
			return cachedIterator[T](cached)
		}
	}
	data := CollectIterator(r.computeFn(partition))
	anyData := make([]any, len(data))
	for i, v := range data {
		anyData[i] = v
	}
	if (level == StorageMemory || level == StorageMemoryAndDisk) && r.ctx.cache != nil {
		r.ctx.cache.put(r.id, idx, anyData)
	}
	if (level == StorageMemory || level == StorageMemoryAndDisk) && shared {
		r.ctx.distributedCache.put(r.id, idx, anyData)
		r.ctx.noteCachePresent(CachePartition{RDDID: r.id, PartitionID: idx})
	}
	if (level == StorageDisk || level == StorageMemoryAndDisk) && r.ctx.disk != nil {
		r.ctx.disk.put(r.id, idx, anyData)
	}
	return SliceIterator(data)
}

func cachedIterator[T any](cached []any) Iterator[T] {
	out := make([]T, len(cached))
	for i, v := range cached {
		out[i] = v.(T)
	}
	return SliceIterator(out)
}

func (r *RDD[T]) GetPartitioner() Partitioner { return r.partitioner }

func (r *RDD[T]) ComputeAny(partition Partition) IteratorAny {
	iter := r.Compute(partition)
	return func() (any, bool) {
		v, ok := iter()
		return v, ok
	}
}

func (r *RDD[T]) IsCached() bool {
	if r.storageLevel == StorageNone || r.ctx == nil {
		return false
	}
	for _, p := range r.Partitions() {
		idx := p.Index()
		mem := r.ctx.cache != nil && r.ctx.cache.has(r.id, idx)
		if r.ctx.distributedCache != nil && cacheAcrossTasks(r) {
			mem = mem || r.ctx.distributedCache.has(r.id, idx)
		}
		disk := r.ctx.disk != nil && r.ctx.disk.has(r.id, idx)
		switch r.storageLevel {
		case StorageMemory:
			if !mem {
				return false
			}
		case StorageDisk:
			if !disk {
				return false
			}
		case StorageMemoryAndDisk:
			if !mem && !disk {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func (r *RDD[T]) cachedStorage() StorageLevel { return r.storageLevel }

func (r *RDD[T]) persistOutput() {
	if r.storageLevel == StorageNone {
		r.storageLevel = StorageMemory
	}
}

func (r *RDD[T]) SetName(name string) *RDD[T] {
	r.name = name
	return r
}

func (r *RDD[T]) GetName() string { return r.name }

func (r *RDD[T]) Name() string { return r.name }

func (r *RDD[T]) GetNumPartitions() int {
	return len(r.Partitions())
}
