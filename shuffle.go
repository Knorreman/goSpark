package spark

import (
	"sync"
)

type shuffleData struct {
	mu     sync.RWMutex
	blocks map[int]map[int][]any
}

func newShuffleData() *shuffleData {
	return &shuffleData{
		blocks: make(map[int]map[int][]any),
	}
}

func (d *shuffleData) Write(mapID int, partitionData map[int][]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blocks[mapID] = partitionData
}

func (d *shuffleData) Read(reduceID int, numMaps int) [][]any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var results [][]any
	for mapID := 0; mapID < numMaps; mapID++ {
		if partitions, ok := d.blocks[mapID]; ok {
			if data, ok := partitions[reduceID]; ok {
				results = append(results, data)
			}
		}
	}
	return results
}

type ShuffleManager interface {
	RegisterShuffle(shuffleID int)
	WriteMapOutput(shuffleID, mapID int, partitionData map[int][]any)
	ReadReduceOutput(shuffleID, reduceID, numMaps int) [][]any
}

type LocalShuffleManager struct {
	mu       sync.RWMutex
	shuffles map[int]*shuffleData
}

func NewLocalShuffleManager() *LocalShuffleManager {
	return &LocalShuffleManager{
		shuffles: make(map[int]*shuffleData),
	}
}

func (m *LocalShuffleManager) RegisterShuffle(shuffleID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.shuffles[shuffleID]; !ok {
		m.shuffles[shuffleID] = newShuffleData()
	}
}

func (m *LocalShuffleManager) GetShuffleData(shuffleID int) *shuffleData {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.shuffles[shuffleID]
}

func (m *LocalShuffleManager) WriteMapOutput(shuffleID, mapID int, partitionData map[int][]any) {
	m.mu.RLock()
	data, ok := m.shuffles[shuffleID]
	m.mu.RUnlock()
	if !ok {
		m.mu.Lock()
		data = newShuffleData()
		m.shuffles[shuffleID] = data
		m.mu.Unlock()
	}
	data.Write(mapID, partitionData)
}

func (m *LocalShuffleManager) ReadReduceOutput(shuffleID, reduceID, numMaps int) [][]any {
	m.mu.RLock()
	data, ok := m.shuffles[shuffleID]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return data.Read(reduceID, numMaps)
}

func writeShuffleMapOutput[K comparable, V any](
	ctx *Context,
	shuffleID int,
	mapID int,
	iter Iterator[Pair[K, V]],
	partitioner Partitioner,
	numPartitions int,
) {
	buckets := make(map[int][]any)
	for {
		pair, ok := iter()
		if !ok {
			break
		}
		reduceID := partitioner.GetPartition(pair.Key)
		buckets[reduceID] = append(buckets[reduceID], pair)
	}
	for i := 0; i < numPartitions; i++ {
		if _, ok := buckets[i]; !ok {
			buckets[i] = nil
		}
	}
	ctx.ShuffleManager().WriteMapOutput(shuffleID, mapID, buckets)
}

func readShuffleReduceOutput[K comparable, C any](
	ctx *Context,
	shuffleID int,
	reduceID int,
	numMaps int,
) Iterator[Pair[K, C]] {
	return iteratorFromStream[Pair[K, C]](ctx, shuffleStream(ctx, shuffleID, reduceID, numMaps))
}
