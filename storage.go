package spark

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type FileSystem interface {
	Create(filePath string) (io.WriteCloser, error)
	MkdirAll(dirPath string, perm os.FileMode) error
	Open(filePath string) (io.ReadCloser, error)
	OpenFrom(filePath string, offset int64) (io.ReadCloser, error)
	Stat(filePath string) (FSFileInfo, error)
	List(dirPath string) ([]FSFileInfo, error)
	Join(elem ...string) string
	Scheme() string
	Rename(oldPath, newPath string) error
	Remove(filePath string) error
}

type LocalFS struct{}

func (LocalFS) Create(filePath string) (io.WriteCloser, error) {
	return os.Create(filePath)
}

func (LocalFS) CreateExclusive(filePath string) (io.WriteCloser, error) {
	f, err := os.CreateTemp(filepath.Dir(filePath), ".gospark-commit-*")
	if err != nil {
		return nil, err
	}
	return &localExclusiveWriter{File: f, final: filePath}, nil
}

type localExclusiveWriter struct {
	*os.File
	final  string
	closed bool
}

func (w *localExclusiveWriter) Abort() error {
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	err := w.File.Close()
	_ = os.Remove(w.Name())
	return err
}

func (w *localExclusiveWriter) Close() error {
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	defer os.Remove(w.Name())
	if err := w.File.Sync(); err != nil {
		w.File.Close()
		return err
	}
	if err := w.File.Close(); err != nil {
		return err
	}
	return os.Link(w.Name(), w.final) // atomic create-if-absent on the same filesystem
}

func (LocalFS) MkdirAll(dirPath string, perm os.FileMode) error {
	return os.MkdirAll(dirPath, perm)
}

func (LocalFS) Open(filePath string) (io.ReadCloser, error) {
	return os.Open(filePath)
}

func (LocalFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (LocalFS) Scheme() string { return "file" }

func (LocalFS) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (LocalFS) Remove(filePath string) error {
	return os.Remove(filePath)
}

type S3FS struct {
	Bucket   string
	Region   string
	Endpoint string
	Config   S3Config
}

func (s S3FS) backend() (ObjectStore, error) {
	if st := lookupMemBucket(s.Bucket); st != nil {
		return st, nil
	}
	cfg := mergeS3Config(currentS3Config(), s.Config)
	if s.Region != "" {
		cfg.Region = s.Region
	}
	if s.Endpoint != "" {
		cfg.Endpoint = s.Endpoint
	}
	if !cfg.enabled() {
		return nil, fmt.Errorf("S3FS: no backend for bucket %s (RegisterMemBucket, SetS3Config, or AWS env credentials)", s.Bucket)
	}
	return newAWSS3Store(s.Bucket, cfg)
}

func (s S3FS) Create(filePath string) (io.WriteCloser, error) {
	st, err := s.backend()
	if err != nil {
		return nil, err
	}
	return objectStoreCreate(st, filePath)
}

func (s S3FS) CreateExclusive(filePath string) (io.WriteCloser, error) {
	st, err := s.backend()
	if err != nil {
		return nil, err
	}
	return &bufPutCloser{store: st, key: filePath, exclusive: true}, nil
}

func (s S3FS) MkdirAll(_ string, _ os.FileMode) error {
	return nil
}

func (s S3FS) Open(filePath string) (io.ReadCloser, error) {
	return s.OpenFrom(filePath, 0)
}

func (s S3FS) OpenFrom(filePath string, offset int64) (io.ReadCloser, error) {
	st, err := s.backend()
	if err != nil {
		return nil, err
	}
	return st.GetRange(filePath, offset)
}

func (s S3FS) Stat(filePath string) (FSFileInfo, error) {
	st, err := s.backend()
	if err != nil {
		return FSFileInfo{}, err
	}
	return objectStoreStat(st, filePath)
}

func (s S3FS) List(prefix string) ([]FSFileInfo, error) {
	st, err := s.backend()
	if err != nil {
		return nil, err
	}
	return objectStoreList(st, prefix)
}

func (s S3FS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (s S3FS) Scheme() string { return "s3a" }

func (s S3FS) Rename(oldPath, newPath string) error {
	st, err := s.backend()
	if err != nil {
		return err
	}
	return objectStoreRename(st, oldPath, newPath)
}

func (s S3FS) Remove(filePath string) error {
	st, err := s.backend()
	if err != nil {
		return err
	}
	return st.Delete(filePath)
}

type ResolvedPath struct {
	FS       FileSystem
	BasePath string
}

func ResolvePath(uri string) ResolvedPath {
	return resolvePath(uri, currentS3Config())
}

func resolvePath(uri string, s3cfg S3Config) ResolvedPath {
	if strings.HasPrefix(uri, "s3a://") || strings.HasPrefix(uri, "s3://") {
		rest := uri
		if strings.HasPrefix(uri, "s3a://") {
			rest = uri[6:]
		} else {
			rest = uri[5:]
		}
		slashIdx := strings.Index(rest, "/")
		bucket, prefix := rest, ""
		if slashIdx >= 0 {
			bucket = rest[:slashIdx]
			prefix = rest[slashIdx+1:]
		}
		return ResolvedPath{FS: S3FS{
			Bucket:   bucket,
			Region:   s3cfg.Region,
			Endpoint: s3cfg.Endpoint,
			Config:   s3cfg,
		}, BasePath: prefix}
	}
	localPath := uri
	if strings.HasPrefix(uri, "file://") {
		localPath = uri[7:]
	}
	return ResolvedPath{FS: LocalFS{}, BasePath: localPath}
}

func PartitionFileName(partitionIndex int) string {
	return fmt.Sprintf("part-%05d", partitionIndex)
}
