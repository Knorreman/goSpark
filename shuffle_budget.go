package spark

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// diskBudget tracks both published shuffle output and task scratch on a worker.
// Reservations happen before a write; tracked files release their reserved size
// when removed. A new worker seeds its usage from files left by previous runs.
type diskBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
	files map[string]int64
}

func newDiskBudget(root string) (*diskBudget, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	b := &diskBudget{files: map[string]int64{}}
	if raw := os.Getenv("GOSPARK_SHUFFLE_DISK_BYTES"); raw != "" {
		limit, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || limit <= 0 {
			return nil, fmt.Errorf("invalid GOSPARK_SHUFFLE_DISK_BYTES %q", raw)
		}
		b.limit = limit
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			b.files[path] = info.Size()
			b.used += info.Size()
		}
		return nil
	})
	return b, err
}

func (b *diskBudget) reserve(path string, n int64) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > math.MaxInt64-b.used || (b.limit > 0 && n > b.limit-b.used) {
		return fmt.Errorf("shuffle disk budget exceeded: used=%d requested=%d limit=%d", b.used, n, b.limit)
	}
	b.files[path] += n
	b.used += n
	return nil
}

func (b *diskBudget) removeFile(path string) error {
	if b == nil {
		return os.Remove(path)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	b.used -= b.files[path]
	delete(b.files, path)
	return nil
}

func (b *diskBudget) removeDir(dir string) error {
	if b == nil {
		return os.RemoveAll(dir)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	prefix := filepath.Clean(dir) + string(os.PathSeparator)
	for path, n := range b.files {
		if strings.HasPrefix(path, prefix) {
			b.used -= n
			delete(b.files, path)
		}
	}
	return nil
}

func (b *diskBudget) renameDir(from, to string) error {
	if b == nil {
		return os.Rename(from, to)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := os.Rename(from, to); err != nil {
		return err
	}
	prefix := filepath.Clean(from) + string(os.PathSeparator)
	for path, n := range b.files {
		if strings.HasPrefix(path, prefix) {
			delete(b.files, path)
			b.files[filepath.Join(to, strings.TrimPrefix(path, prefix))] = n
		}
	}
	return nil
}
