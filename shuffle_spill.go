package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var SpillRecordLimit = 100000

type spillAcc struct {
	recs  []any
	files []string
}

func (s *spillAcc) add(item any, dir string, codec RecordCodec) error {
	s.recs = append(s.recs, item)
	if SpillRecordLimit > 0 && len(s.recs) >= SpillRecordLimit {
		return s.flush(dir, codec)
	}
	return nil
}

func (s *spillAcc) flush(dir string, codec RecordCodec) error {
	if len(s.recs) == 0 {
		return nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("spill-%d-%d", time.Now().UnixNano(), spillSeq.Add(1)))
	if _, err := writeBucketFile(path, codec, s.recs); err != nil {
		return err
	}
	s.files = append(s.files, path)
	s.recs = nil
	return nil
}

func (s *spillAcc) collect(codec RecordCodec) ([]any, error) {
	var out []any
	for _, f := range s.files {
		recs, err := readBucketFile(f, codec)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		_ = os.Remove(f)
	}
	out = append(out, s.recs...)
	s.recs = nil
	s.files = nil
	return out, nil
}

var spillSeq atomic.Int64
