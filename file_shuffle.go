package spark

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type fileShuffleManager struct {
	ctx   *Context
	mu    sync.Mutex
	paths map[int]map[int]map[int]string
}

func newFileShuffleManager(ctx *Context) *fileShuffleManager {
	return &fileShuffleManager{ctx: ctx, paths: map[int]map[int]map[int]string{}}
}
func (m *fileShuffleManager) RegisterShuffle(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paths[id] == nil {
		m.paths[id] = map[int]map[int]string{}
	}
}
func (m *fileShuffleManager) install(id, mapID int, paths map[int]string) {
	m.RegisterShuffle(id)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.paths[id][mapID] = paths
}
func (m *fileShuffleManager) WriteMapOutput(id, mapID int, buckets map[int][]any) {
	dir, err := os.MkdirTemp(m.ctx.disk.dir, "shuffle-")
	must(err)
	paths := map[int]string{}
	for rid, recs := range buckets {
		p := filepath.Join(dir, bucketName(rid))
		_, err := writeBucketFile(p, DefaultCodec(), recs)
		must(err)
		paths[rid] = p
	}
	m.install(id, mapID, paths)
}
func (m *fileShuffleManager) stream(id, rid, numMaps int) (recordStream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	paths := make([]string, 0, numMaps)
	for i := 0; i < numMaps; i++ {
		p := m.paths[id][i][rid]
		if p == "" {
			return nil, fmt.Errorf("missing shuffle %d map %d reduce %d", id, i, rid)
		}
		paths = append(paths, p)
	}
	return &fileChain{paths: paths, codec: cancelCodec{ctx: m.ctx.TaskContext(), RecordCodec: DefaultCodec()}, max: m.ctx.recordBytes()}, nil
}
func (m *fileShuffleManager) ReadReduceOutput(id, rid, numMaps int) [][]any {
	s, err := m.stream(id, rid, numMaps)
	must(err)
	defer s.Close()
	var out []any
	for {
		v, err := s.Next()
		if err == io.EOF {
			break
		}
		must(err)
		out = append(out, v)
	}
	return [][]any{out}
}

type fileChain struct {
	paths   []string
	index   int
	current *bucketReader
	codec   RecordCodec
	max     int
}

func (s *fileChain) Next() (any, error) {
	for {
		if s.current == nil {
			if s.index >= len(s.paths) {
				return nil, io.EOF
			}
			r, err := openBucketFile(s.paths[s.index], s.codec, s.max)
			if err != nil {
				return nil, err
			}
			s.current = r
			s.index++
		}
		v, err := s.current.Next()
		if err == io.EOF {
			s.current.Close()
			s.current = nil
			continue
		}
		return v, err
	}
}
func (s *fileChain) Close() error {
	s.index = len(s.paths)
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

type sliceStream struct{ iter IteratorAny }

func (s *sliceStream) Next() (any, error) {
	v, ok := s.iter()
	if !ok {
		return nil, io.EOF
	}
	return v, nil
}
func (s *sliceStream) Close() error { return nil }
func shuffleStream(ctx *Context, id, rid, maps int) recordStream {
	if m, ok := ctx.ShuffleManager().(*fileShuffleManager); ok {
		s, err := m.stream(id, rid, maps)
		must(err)
		return s
	}
	blocks := ctx.ShuffleManager().ReadReduceOutput(id, rid, maps)
	var iters []Iterator[any]
	for _, b := range blocks {
		iters = append(iters, SliceIterator(b))
	}
	return &sliceStream{iter: ChainIterators(iters)}
}

// writeStreamMap uses one encoded record plus at most 16 buffered files. No
// bucket is reconstructed in memory when map output is published.
func writeStreamMap(ctx *Context, store *DiskShuffleStore, dep *ShuffleDep, part Partition, job string, mapID, attempt int) (manifest MapOutputManifest, err error) {
	n := dep.partitioner.NumPartitions()
	if n <= 0 {
		return manifest, fmt.Errorf("invalid reducer count")
	}
	var maxBytes int64
	if raw := os.Getenv("GOSPARK_SHUFFLE_MAP_MAX_BYTES"); raw != "" {
		maxBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || maxBytes <= 0 {
			return manifest, fmt.Errorf("invalid GOSPARK_SHUFFLE_MAP_MAX_BYTES %q", raw)
		}
	}
	usedBytes := int64(n) * 8 // each bucket starts with a framing header
	if maxBytes > 0 && usedBytes > maxBytes {
		return manifest, fmt.Errorf("shuffle map output exceeds %d byte limit", maxBytes)
	}
	dir := store.mapDir(job, dep.shuffleID, mapID, attempt)
	if err = os.MkdirAll(filepath.Dir(dir), 0755); err != nil {
		return manifest, err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(dir), "map-pending-")
	if err != nil {
		return manifest, err
	}
	defer store.budget.removeDir(tmp)
	writers := make([]*bucketWriter, n)
	active := []int{}
	defer func() {
		for _, w := range writers {
			if w != nil {
				_ = w.suspend()
			}
		}
	}()
	for i := range writers {
		writers[i], err = newBucketWriter(filepath.Join(tmp, bucketName(i)), store.budget)
		if err != nil {
			return manifest, err
		}
	}
	write := func(v any) error {
		key := dep.ExtractKey(v)
		if dep.keyExtractor != nil {
			_ = keyToken(key)
		}
		rid := 0
		if key != nil {
			rid = dep.partitioner.GetPartition(key)
		}
		if rid < 0 || rid >= n {
			return fmt.Errorf("invalid reduce ID %d", rid)
		}
		p, e := store.codec.Encode(v)
		if e != nil {
			return e
		}
		if e = recordLimit(len(p), ctx.recordBytes()); e != nil {
			return e
		}
		if maxBytes > 0 && int64(len(p))+8 > maxBytes-usedBytes {
			return fmt.Errorf("shuffle map output exceeds %d byte limit", maxBytes)
		}
		usedBytes += int64(len(p)) + 8
		if writers[rid].file == nil {
			if len(active) == 16 {
				if e = writers[active[0]].suspend(); e != nil {
					return e
				}
				active = active[1:]
			}
			active = append(active, rid)
		}
		if e = writers[rid].append(p); e != nil {
			return e
		}
		return nil
	}
	iter := dep.Parent().ComputeAny(part)
	var batch []any
	used := 0
	flush := func() error {
		for _, v := range combineMapOutput(batch, dep) {
			if err := write(v); err != nil {
				return err
			}
		}
		batch = nil
		used = 0
		return nil
	}
	for {
		ctx.checkCanceled()
		v, ok := iter()
		if !ok {
			break
		}
		if !dep.MapSideCombine() {
			if err = write(v); err != nil {
				return manifest, err
			}
			continue
		}
		p, e := store.codec.Encode(v)
		if e != nil {
			return manifest, e
		}
		if e = recordLimit(len(p), ctx.recordBytes()); e != nil {
			return manifest, e
		}
		if len(batch) > 0 && used+len(p)+64 > ctx.memoryBytes() {
			if err = flush(); err != nil {
				return manifest, err
			}
		}
		batch = append(batch, v)
		used += len(p) + 64
		if used >= ctx.memoryBytes() {
			if err = flush(); err != nil {
				return manifest, err
			}
		}
	}
	if err = flush(); err != nil {
		return manifest, err
	}
	manifest = MapOutputManifest{JobID: job, ShuffleID: dep.shuffleID, MapID: mapID, Attempt: attempt, Location: dir}
	for i, w := range writers {
		meta, e := w.finish()
		if e != nil {
			return manifest, e
		}
		meta.ReduceID = i
		manifest.Buckets = append(manifest.Buckets, meta)
	}
	ctx.checkCanceled()
	if err = store.budget.renameDir(tmp, dir); err != nil {
		return manifest, err
	}
	return manifest, nil
}
