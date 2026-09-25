package spark

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

var shuffleMagic = [4]byte{'G', 'S', 'H', '1'}

type MapOutputManifest struct {
	JobID     string              `json:"job_id"`
	ShuffleID int                 `json:"shuffle_id"`
	MapID     int                 `json:"map_id"`
	Attempt   int                 `json:"attempt"`
	Location  string              `json:"location"`
	BaseURL   string              `json:"base_url,omitempty"`
	Buckets   []ShuffleBucketMeta `json:"buckets"`
}

type ShuffleBucketMeta struct {
	ReduceID int    `json:"reduce_id"`
	Bytes    int64  `json:"bytes"`
	Records  int    `json:"records"`
	Checksum uint32 `json:"checksum"`
}

type DiskShuffleStore struct {
	root   string
	codec  RecordCodec
	budget *diskBudget
}

func NewDiskShuffleStore(root string) *DiskShuffleStore {
	return &DiskShuffleStore{root: root, codec: DefaultCodec()}
}

func (s *DiskShuffleStore) WriteMap(jobID string, shuffleID, mapID, attempt, numReducers int, buckets map[int][]any) (MapOutputManifest, error) {
	if numReducers <= 0 {
		return MapOutputManifest{}, fmt.Errorf("numReducers must be > 0")
	}
	dir := s.mapDir(jobID, shuffleID, mapID, attempt)
	tmpDir := dir + ".tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return MapOutputManifest{}, err
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return MapOutputManifest{}, err
	}
	manifest := MapOutputManifest{
		JobID:     jobID,
		ShuffleID: shuffleID,
		MapID:     mapID,
		Attempt:   attempt,
		Location:  dir,
		Buckets:   make([]ShuffleBucketMeta, 0, numReducers),
	}
	for reduceID := 0; reduceID < numReducers; reduceID++ {
		recs := buckets[reduceID]
		path := filepath.Join(tmpDir, bucketName(reduceID))
		meta, err := writeBucketFile(path, s.codec, recs)
		if err != nil {
			os.RemoveAll(tmpDir)
			return MapOutputManifest{}, err
		}
		meta.ReduceID = reduceID
		manifest.Buckets = append(manifest.Buckets, meta)
	}
	if err := os.RemoveAll(dir); err != nil {
		os.RemoveAll(tmpDir)
		return MapOutputManifest{}, err
	}
	if err := os.Rename(tmpDir, dir); err != nil {
		os.RemoveAll(tmpDir)
		return MapOutputManifest{}, err
	}
	manifest.Location = dir
	return manifest, nil
}

func (s *DiskShuffleStore) ReadBucket(m MapOutputManifest, reduceID int) ([]any, error) {
	path := filepath.Join(m.Location, bucketName(reduceID))
	recs, err := readBucketFile(path, s.codec)
	if err != nil {
		return nil, fmt.Errorf("read shuffle job=%s shuffle=%d map=%d reduce=%d: %w", m.JobID, m.ShuffleID, m.MapID, reduceID, err)
	}
	return recs, nil
}

func (s *DiskShuffleStore) ReadReduce(shuffleID, reduceID int, maps []MapOutputManifest) ([][]any, error) {
	var out [][]any
	found := 0
	for _, m := range maps {
		if m.ShuffleID != shuffleID {
			continue
		}
		found++
		recs, err := s.ReadBucket(m, reduceID)
		if err != nil {
			return nil, err
		}
		out = append(out, recs)
	}
	if found == 0 {
		return nil, fmt.Errorf("no map outputs for shuffle %d", shuffleID)
	}
	return out, nil
}

func (s *DiskShuffleStore) mapDir(jobID string, shuffleID, mapID, attempt int) string {
	return filepath.Join(s.jobDir(jobID), fmt.Sprintf("s%d", shuffleID), fmt.Sprintf("m%d-a%d", mapID, attempt))
}

func (s *DiskShuffleStore) jobDir(jobID string) string { return filepath.Join(s.root, jobID) }

func bucketName(reduceID int) string {
	return fmt.Sprintf("r%d", reduceID)
}

func writeBucketFile(path string, codec RecordCodec, recs []any) (ShuffleBucketMeta, error) {
	f, err := os.Create(path)
	if err != nil {
		return ShuffleBucketMeta{}, err
	}
	defer f.Close()
	if _, err := f.Write(shuffleMagic[:]); err != nil {
		return ShuffleBucketMeta{}, err
	}
	n := uint32(len(recs))
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], n)
	if _, err := f.Write(hdr[:]); err != nil {
		return ShuffleBucketMeta{}, err
	}
	sum := crc32.NewIEEE()
	for _, rec := range recs {
		payload, err := codec.Encode(rec)
		if err != nil {
			return ShuffleBucketMeta{}, err
		}
		crc := crc32.ChecksumIEEE(payload)
		var recHdr [8]byte
		binary.BigEndian.PutUint32(recHdr[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(recHdr[4:8], crc)
		if _, err := f.Write(recHdr[:]); err != nil {
			return ShuffleBucketMeta{}, err
		}
		if _, err := f.Write(payload); err != nil {
			return ShuffleBucketMeta{}, err
		}
		sum.Write(payload)
	}
	info, err := f.Stat()
	if err != nil {
		return ShuffleBucketMeta{}, err
	}
	return ShuffleBucketMeta{Bytes: info.Size(), Records: len(recs), Checksum: sum.Sum32()}, nil
}

func readBucketFile(path string, codec RecordCodec) ([]any, error) {
	r, err := openBucketFile(path, codec, defaultRecordBytes)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []any
	for {
		v, err := r.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}
