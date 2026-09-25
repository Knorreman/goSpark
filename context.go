package spark

import (
	"context"
	"sync"
	"sync/atomic"
)

type Context struct {
	cleanups    []func()
	execution   context.Context
	conf        *Config
	parallelism int
	closed      atomic.Bool
	mu          sync.Mutex

	shuffleManager   ShuffleManager
	cache            *memoryCache
	distributedCache *memoryCache
	disk             *diskCache
	shuffleBudget    *diskBudget
	rddIDs           atomic.Int64
	shuffleIDs       atomic.Int64
	cacheChangesMu   sync.Mutex
	cacheTouched     map[CachePartition]bool
	cacheDropped     map[CachePartition]bool
	accumulators     accumulatorSet
	textSeq          int
	textInputs       map[textInputKey][]InputSplit
}

func NewContext(conf *Config) *Context {
	if conf == nil {
		conf = DefaultConfig()
	}
	parallelism := conf.NumPartitions
	if parallelism <= 0 {
		if conf.IsLocal() {
			cores, _ := conf.NumLocalCores()
			if cores <= 0 {
				cores = 2
			}
			parallelism = cores
		} else {
			parallelism = 4
		}
	}
	ctx := &Context{
		execution:   context.Background(),
		conf:        conf,
		parallelism: parallelism,
		cache:       newMemoryCache(),
		disk:        newDiskCache(),
	}
	ctx.shuffleManager = newFileShuffleManager(ctx)
	return ctx
}

func (c *Context) Config() *Config { return c.conf }

func (c *Context) noteCachePresent(key CachePartition) {
	c.cacheChangesMu.Lock()
	defer c.cacheChangesMu.Unlock()
	if c.cacheTouched == nil {
		c.cacheTouched = map[CachePartition]bool{}
	}
	c.cacheTouched[key] = true
	delete(c.cacheDropped, key)
}

func (c *Context) noteCacheRemoved(key CachePartition) {
	c.cacheChangesMu.Lock()
	defer c.cacheChangesMu.Unlock()
	if c.cacheDropped == nil {
		c.cacheDropped = map[CachePartition]bool{}
	}
	c.cacheDropped[key] = true
	delete(c.cacheTouched, key)
}

func (c *Context) cacheUpdates() (cached, dropped []CachePartition) {
	c.cacheChangesMu.Lock()
	defer c.cacheChangesMu.Unlock()
	for key := range c.cacheTouched {
		cached = append(cached, key)
	}
	for key := range c.cacheDropped {
		dropped = append(dropped, key)
	}
	return
}

// TaskContext is canceled when the driver's task RPC is canceled or times out.
// User callbacks performing blocking I/O should pass it to that I/O operation.
func (c *Context) TaskContext() context.Context { return c.execution }

type taskCanceled struct{ err error }

func (c *Context) checkCanceled() {
	if c != nil && c.execution != nil {
		if err := c.execution.Err(); err != nil {
			panic(taskCanceled{err})
		}
	}
}

func (c *Context) DefaultParallelism() int { return c.parallelism }

func (c *Context) Stop() {
	c.closed.Store(true)
	c.mu.Lock()
	for _, close := range c.cleanups {
		close()
	}
	c.cleanups = nil
	c.mu.Unlock()
	if c.cache != nil {
		c.cache.clear()
	}
	if c.disk != nil {
		c.disk.clear()
	}
}

func (c *Context) onClose(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups = append(c.cleanups, fn)
}

func (c *Context) IsClosed() bool {
	return c.closed.Load()
}

func (c *Context) ShuffleManager() ShuffleManager {
	return c.shuffleManager
}

func (c *Context) nextRDDID() int {
	return int(c.rddIDs.Add(1))
}

func (c *Context) nextShuffleID() int {
	return int(c.shuffleIDs.Add(1))
}

func Parallelize[T any](ctx *Context, data []T, numPartitions ...int) *RDD[T] {
	np := ctx.parallelism
	if len(numPartitions) > 0 && numPartitions[0] > 0 {
		np = numPartitions[0]
	}
	return newParallelCollectionRDD(ctx, data, np)
}

func MakeRange(ctx *Context, start, end, step int, numPartitions ...int) *RDD[int] {
	np := ctx.parallelism
	if len(numPartitions) > 0 && numPartitions[0] > 0 {
		np = numPartitions[0]
	}
	data := make([]int, 0, (end-start)/step+1)
	for i := start; i < end; i += step {
		data = append(data, i)
	}
	return newParallelCollectionRDD(ctx, data, np)
}

func TextFile(ctx *Context, path string, numPartitions ...int) *RDD[string] {
	np := ctx.parallelism
	if len(numPartitions) > 0 && numPartitions[0] > 0 {
		np = numPartitions[0]
	}
	return newTextFileRDD(ctx, path, np)
}

func EmptyRDD[T any](ctx *Context) *RDD[T] {
	return newEmptyRDD[T](ctx)
}
