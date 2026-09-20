package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type StorageLevel int

const (
	StorageNone StorageLevel = iota
	StorageMemory
	StorageDisk
	StorageMemoryAndDisk
)

type cacheKey struct {
	rddID int
	part  int
}

type memoryCache struct {
	mu   sync.RWMutex
	data map[cacheKey][]any
}

func newMemoryCache() *memoryCache {
	return &memoryCache{data: make(map[cacheKey][]any)}
}

func (c *memoryCache) get(rddID, part int) ([]any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[cacheKey{rddID: rddID, part: part}]
	return v, ok
}

func (c *memoryCache) put(rddID, part int, v []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[cacheKey{rddID: rddID, part: part}] = v
}

func (c *memoryCache) has(rddID, part int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.data[cacheKey{rddID: rddID, part: part}]
	return ok
}

func (c *memoryCache) removeRDD(rddID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.data {
		if k.rddID == rddID {
			delete(c.data, k)
		}
	}
}

func (c *memoryCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[cacheKey][]any)
}

func Cache[T any](rdd *RDD[T]) *RDD[T] {
	return Persist(rdd, StorageMemory)
}

func Persist[T any](rdd *RDD[T], level StorageLevel) *RDD[T] {
	rdd.storageLevel = level
	return rdd
}

func Unpersist[T any](rdd *RDD[T]) *RDD[T] {
	rdd.storageLevel = StorageNone
	if rdd.ctx != nil && rdd.ctx.cache != nil {
		rdd.ctx.cache.removeRDD(rdd.id)
	}
	if rdd.ctx != nil && rdd.ctx.disk != nil {
		rdd.ctx.disk.removeRDD(rdd.id)
	}
	return rdd
}

type diskCache struct {
	dir string
}

func newDiskCache() *diskCache {
	dir, err := os.MkdirTemp("", "gospark-disk-*")
	if err != nil {
		dir = filepath.Join(os.TempDir(), "gospark-disk")
		_ = os.MkdirAll(dir, 0755)
	}
	return &diskCache{dir: dir}
}

func (d *diskCache) path(rddID, part int) string {
	return filepath.Join(d.dir, fmt.Sprintf("%d-%d.gob", rddID, part))
}

func (d *diskCache) get(rddID, part int) ([]any, bool) {
	b, err := os.ReadFile(d.path(rddID, part))
	if err != nil {
		return nil, false
	}
	recs, err := decodeRecordSlice(b)
	if err != nil {
		return nil, false
	}
	return recs, true
}

func (d *diskCache) put(rddID, part int, v []any) {
	b, err := encodeRecordSlice(v)
	if err != nil {
		return
	}
	_ = os.WriteFile(d.path(rddID, part), b, 0644)
}

func (d *diskCache) has(rddID, part int) bool {
	_, err := os.Stat(d.path(rddID, part))
	return err == nil
}

func (d *diskCache) removeRDD(rddID int) {
	matches, _ := filepath.Glob(filepath.Join(d.dir, fmt.Sprintf("%d-*.gob", rddID)))
	for _, p := range matches {
		_ = os.Remove(p)
	}
}

func (d *diskCache) clear() {
	_ = os.RemoveAll(d.dir)
}
