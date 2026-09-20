package spark

import (
	"fmt"
	"sync"
)

func SaveAsTextFile[T any](rdd *RDD[T], path string) error {
	s3cfg := currentS3Config()
	if rdd != nil && rdd.ctx != nil && rdd.ctx.conf != nil {
		s3cfg = mergeS3Config(s3cfg, rdd.ctx.conf.S3)
	}
	resolved := resolvePath(path, s3cfg)
	fs := resolved.FS
	basePath := resolved.BasePath

	computeShuffleStages(rdd)
	partitions := rdd.Partitions()

	if err := fs.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", basePath, err)
	}
	committer := NewOutputCommitter(fs, basePath, 1)
	if err := committer.Setup(); err != nil {
		return fmt.Errorf("commit setup: %w", err)
	}

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			filePath := committer.TempPath(idx)
			w, err := fs.Create(filePath)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("create file for partition %d: %w", idx, err)
				}
				mu.Unlock()
				return
			}
			iter := rdd.Compute(partition)
			var writeErr error
			for {
				v, ok := iter()
				if !ok {
					break
				}
				if _, err := fmt.Fprintln(w, v); err != nil {
					writeErr = err
					break
				}
			}
			if err := writeCloserChecked(w, writeErr); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("write partition %d: %w", idx, err)
				}
				mu.Unlock()
				return
			}
			if err := committer.CommitPartition(idx); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("commit partition %d: %w", idx, err)
				}
				mu.Unlock()
			}
		}(i, p)
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return committer.CommitJob()
}

func ForeachPartition[T any](rdd *RDD[T], fn func(int, Iterator[T])) {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			fn(idx, rdd.Compute(partition))
		}(i, p)
	}
	wg.Wait()
}

type WritePartitionFunc func(basePath string, partitionIndex int, lines []string) error

func SaveAsTextFileWith[T any](rdd *RDD[T], path string, writePartition WritePartitionFunc) error {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			iter := rdd.Compute(partition)
			var lines []string
			for {
				v, ok := iter()
				if !ok {
					break
				}
				lines = append(lines, fmt.Sprint(v))
			}

			if err := writePartition(path, idx, lines); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i, p)
	}

	wg.Wait()
	return firstErr
}

type K8sPartitionWriter struct {
	PVCPath string
}

func (w K8sPartitionWriter) WritePartition(basePath string, partitionIndex int, lines []string) error {
	dir := basePath
	if w.PVCPath != "" {
		dir = fmt.Sprintf("%s/%s", w.PVCPath, basePath)
	}
	resolved := ResolvePath(dir)
	fs := resolved.FS
	fullPath := resolved.BasePath

	if err := fs.MkdirAll(fullPath, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", fullPath, err)
	}

	filePath := fs.Join(fullPath, PartitionFileName(partitionIndex))
	f, err := fs.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file for partition %d: %w", partitionIndex, err)
	}
	defer f.Close()
	for _, line := range lines {
		fmt.Fprintln(f, line)
	}
	return nil
}

func SaveAsTextFileK8s[T any](rdd *RDD[T], path string) error {
	return SaveAsTextFileWith(rdd, path, K8sPartitionWriter{}.WritePartition)
}

func SaveAsTextFileSharedVolume[T any](rdd *RDD[T], path string) error {
	resolved := ResolvePath(path)
	fs := resolved.FS
	if err := fs.MkdirAll(resolved.BasePath, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", resolved.BasePath, err)
	}
	return SaveAsTextFileWith(rdd, path, K8sPartitionWriter{}.WritePartition)
}

func RunPartition(rddAny RDDAny, partitionIndex int, outputPath string) error {
	resolved := ResolvePath(outputPath)
	fs := resolved.FS
	basePath := resolved.BasePath

	computeShuffleStages(rddAny)
	partitions := rddAny.Partitions()

	if partitionIndex < 0 || partitionIndex >= len(partitions) {
		return fmt.Errorf("partition index %d out of range [0, %d)", partitionIndex, len(partitions))
	}

	if err := fs.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", basePath, err)
	}

	filePath := fs.Join(basePath, PartitionFileName(partitionIndex))
	w, err := fs.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file for partition %d: %w", partitionIndex, err)
	}
	defer w.Close()

	iter := rddAny.ComputeAny(partitions[partitionIndex])
	for {
		v, ok := iter()
		if !ok {
			break
		}
		fmt.Fprintln(w, v)
	}
	return nil
}
