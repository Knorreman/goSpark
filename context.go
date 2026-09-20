package spark

import (
	"sync"
	"sync/atomic"
)

type Context struct {
	conf        *Config
	parallelism int
	closed      atomic.Bool
	mu          sync.Mutex

	shuffleManager ShuffleManager
	cache          *memoryCache
	disk           *diskCache
	rddIDs         atomic.Int64
	shuffleIDs     atomic.Int64
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
		conf:           conf,
		parallelism:    parallelism,
		shuffleManager: NewLocalShuffleManager(),
		cache:          newMemoryCache(),
		disk:           newDiskCache(),
	}
	return ctx
}

func (c *Context) Config() *Config { return c.conf }

func (c *Context) DefaultParallelism() int { return c.parallelism }

func (c *Context) Stop() {
	c.closed.Store(true)
	if c.cache != nil {
		c.cache.clear()
	}
	if c.disk != nil {
		c.disk.clear()
	}
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
