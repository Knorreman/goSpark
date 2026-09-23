package spark

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type FSFileInfo struct {
	Name string
	Size int64
}

type ObjectStore interface {
	Put(key string, data []byte) error
	PutIfAbsent(key string, data []byte) error
	Get(key string) ([]byte, error)
	GetRange(key string, offset int64) (io.ReadCloser, error)
	Head(key string) (int64, error)
	List(prefix string) ([]string, error)
	Delete(key string) error
	Copy(src, dst string) error
}

type MemStore struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{files: make(map[string][]byte)}
}

func (m *MemStore) Put(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[key] = cp
	return nil
}

func (m *MemStore) PutIfAbsent(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.files[key]; ok {
		return fmt.Errorf("object %q already exists: %w", key, os.ErrExist)
	}
	m.files[key] = bytes.Clone(data)
	return nil
}

func (m *MemStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.files[key]
	if !ok {
		return nil, fmt.Errorf("memfs: key %q not found", key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (m *MemStore) Head(key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.files[key]
	if !ok {
		return 0, fmt.Errorf("memfs: key %q not found", key)
	}
	return int64(len(data)), nil
}

func (m *MemStore) List(prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.files {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *MemStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, key)
	return nil
}

func (m *MemStore) GetRange(key string, offset int64) (io.ReadCloser, error) {
	return storeGetRange(m, key, offset)
}

func (m *MemStore) Copy(src, dst string) error {
	data, err := m.Get(src)
	if err != nil {
		return err
	}
	return m.Put(dst, data)
}

var (
	memBucketsMu sync.Mutex
	memBuckets   = map[string]*MemStore{}
)

func RegisterMemBucket(bucket string) *MemStore {
	memBucketsMu.Lock()
	defer memBucketsMu.Unlock()
	st := NewMemStore()
	memBuckets[bucket] = st
	return st
}

func UnregisterMemBucket(bucket string) {
	memBucketsMu.Lock()
	defer memBucketsMu.Unlock()
	delete(memBuckets, bucket)
}

func lookupMemBucket(bucket string) *MemStore {
	memBucketsMu.Lock()
	defer memBucketsMu.Unlock()
	return memBuckets[bucket]
}

type memCloser struct {
	*bytes.Reader
}

func (m memCloser) Close() error { return nil }

func storeGetRange(st ObjectStore, key string, offset int64) (io.ReadCloser, error) {
	data, err := st.Get(key)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	return memCloser{bytes.NewReader(data[offset:])}, nil
}

type bufPutCloser struct {
	store     ObjectStore
	key       string
	buf       bytes.Buffer
	exclusive bool
	closed    bool
}

func (w *bufPutCloser) Write(p []byte) (int, error) {
	if w.closed {
		return 0, os.ErrClosed
	}
	return w.buf.Write(p)
}

func (w *bufPutCloser) Close() error {
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	if w.exclusive {
		return w.store.PutIfAbsent(w.key, w.buf.Bytes())
	}
	return w.store.Put(w.key, w.buf.Bytes())
}

func (w *bufPutCloser) Abort() error {
	w.closed = true
	w.buf.Reset()
	return nil
}

func objectStoreCreate(st ObjectStore, key string) (io.WriteCloser, error) {
	return &bufPutCloser{store: st, key: key}, nil
}

func objectStoreRename(st ObjectStore, oldKey, newKey string) error {
	if err := st.Copy(oldKey, newKey); err != nil {
		return err
	}
	return st.Delete(oldKey)
}

func objectStoreStat(st ObjectStore, key string) (FSFileInfo, error) {
	size, err := st.Head(key)
	if err != nil {
		return FSFileInfo{}, err
	}
	return FSFileInfo{Name: path.Base(key), Size: size}, nil
}

func objectStoreList(st ObjectStore, prefix string) ([]FSFileInfo, error) {
	keys, err := st.List(prefix)
	if err != nil {
		return nil, err
	}
	out := make([]FSFileInfo, 0, len(keys))
	for _, k := range keys {
		size, err := st.Head(k)
		if err != nil {
			continue
		}
		out = append(out, FSFileInfo{Name: k, Size: size})
	}
	return out, nil
}

func (LocalFS) Stat(filePath string) (FSFileInfo, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return FSFileInfo{}, err
	}
	return FSFileInfo{Name: fi.Name(), Size: fi.Size()}, nil
}

func (LocalFS) List(dirPath string) ([]FSFileInfo, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	out := make([]FSFileInfo, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, FSFileInfo{Name: e.Name(), Size: info.Size()})
	}
	return out, nil
}

func (LocalFS) OpenFrom(filePath string, offset int64) (io.ReadCloser, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}
