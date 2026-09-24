package spark

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type emptyPartition struct {
	index int
}

func (p *emptyPartition) Index() int { return p.index }

func newEmptyRDD[T any](ctx *Context) *RDD[T] {
	return NewRDD[T](
		ctx,
		func() []Partition { return nil },
		func() []Dependency { return nil },
		func(partition Partition) Iterator[T] { return EmptyIterator[T]() },
	)
}

func sliceData[T any](data []T, numSlices int) [][]T {
	if len(data) == 0 {
		return [][]T{{}}
	}
	if numSlices <= 0 {
		numSlices = 1
	}
	if numSlices > len(data) {
		numSlices = len(data)
	}
	result := make([][]T, numSlices)
	for i := range result {
		start := i * len(data) / numSlices
		end := (i + 1) * len(data) / numSlices
		result[i] = data[start:end:end]
	}
	return result
}

func newParallelCollectionRDD[T any](ctx *Context, data []T, numPartitions int) *RDD[T] {
	slices := sliceData(data, numPartitions)
	partitionData := make(map[int][]T)
	for i, s := range slices {
		if len(s) > 0 {
			partitionData[i] = s
		}
	}

	return NewRDD[T](
		ctx,
		func() []Partition {
			partitions := make([]Partition, 0, len(slices))
			for i := range slices {
				if len(slices[i]) > 0 {
					partitions = append(partitions, NewPartition(i))
				}
			}
			if len(partitions) == 0 {
				partitions = append(partitions, NewPartition(0))
			}
			return partitions
		},
		func() []Dependency { return nil },
		func(partition Partition) Iterator[T] {
			data, ok := partitionData[partition.Index()]
			if !ok {
				return EmptyIterator[T]()
			}
			return SliceIterator(data)
		},
	)
}

type textFilePartition struct {
	index  int
	path   string
	offset int64
	length int64
}

func (p *textFilePartition) Index() int { return p.index }

func newTextFileRDD(ctx *Context, path string, numPartitions int) *RDD[string] {
	s3cfg := currentS3Config()
	if ctx != nil && ctx.conf != nil {
		s3cfg = mergeS3Config(s3cfg, ctx.conf.S3)
	}
	resolved := resolvePath(path, s3cfg)
	fs := resolved.FS
	filePath := resolved.BasePath
	return NewRDD[string](
		ctx,
		func() []Partition {
			info, err := fs.Stat(filePath)
			if err != nil {
				return []Partition{&textFilePartition{index: 0, path: filePath, offset: 0, length: 0}}
			}
			fileSize := info.Size
			if fileSize == 0 {
				return []Partition{&textFilePartition{index: 0, path: filePath, offset: 0, length: 0}}
			}
			actualParts := numPartitions
			if actualParts <= 0 {
				actualParts = 1
			}
			partitions := make([]Partition, actualParts)
			chunkSize := fileSize / int64(actualParts)
			if chunkSize == 0 {
				chunkSize = fileSize
				partitions = partitions[:1]
				actualParts = 1
			}
			for i := 0; i < actualParts; i++ {
				offset := int64(i) * chunkSize
				length := chunkSize
				if i == actualParts-1 {
					length = fileSize - offset
				}
				partitions[i] = &textFilePartition{
					index:  i,
					path:   filePath,
					offset: offset,
					length: length,
				}
			}
			return partitions
		},
		func() []Dependency { return nil },
		func(partition Partition) Iterator[string] {
			p := partition.(*textFilePartition)
			return newTextFileIteratorFS(fs, p.path, p.offset, p.length, ctx)
		},
	)
}

func newTextFileIterator(path string, offset, length int64) Iterator[string] {
	return newTextFileIteratorFS(LocalFS{}, path, offset, length)
}

func newTextFileIteratorFS(fs FileSystem, path string, offset, length int64, contexts ...*Context) Iterator[string] {
	if length <= 0 {
		return EmptyIterator[string]()
	}
	skipFirst := false
	if offset > 0 {
		prev, err := readByteAt(fs, path, offset-1)
		if err != nil {
			return func() (string, bool) { panic(err) }
		}
		if prev != '\n' {
			skipFirst = true
		}
	}
	file, err := fs.OpenFrom(path, offset)
	if err != nil {
		return func() (string, bool) { panic(err) }
	}
	limit := defaultRecordBytes
	if len(contexts) > 0 {
		limit = contexts[0].recordBytes()
		contexts[0].onClose(func() { _ = file.Close() })
	}
	reader := bufio.NewReader(file)
	pos := offset
	end := offset + length
	if skipFirst {
		for {
			skipped, err := reader.ReadSlice('\n')
			pos += int64(len(skipped))
			if err == bufio.ErrBufferFull {
				continue
			}
			if err != nil {
				file.Close()
				if err != io.EOF {
					must(err)
				}
				return EmptyIterator[string]()
			}
			break
		}
	}
	done := false
	return func() (string, bool) {
		if done {
			return "", false
		}
		if pos >= end {
			file.Close()
			done = true
			return "", false
		}
		var data []byte
		var err error
		for {
			var frag []byte
			frag, err = reader.ReadSlice('\n')
			if len(data)+len(frag) > limit {
				file.Close()
				must(fmt.Errorf("TextFile line exceeds MaxRecordBytes=%d", limit))
			}
			data = append(data, frag...)
			if err != bufio.ErrBufferFull {
				break
			}
		}
		if err != nil && err != io.EOF {
			file.Close()
			must(err)
		}
		line := string(data)
		if len(line) == 0 {
			file.Close()
			done = true
			return "", false
		}
		pos += int64(len(line))
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if err != nil {
			file.Close()
			done = true
		}
		return line, true
	}
}

func readByteAt(fs FileSystem, path string, offset int64) (byte, error) {
	r, err := fs.OpenFrom(path, offset)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	var b [1]byte
	n, err := r.Read(b[:])
	if n == 1 {
		return b[0], nil
	}
	if err == nil {
		err = io.EOF
	}
	return 0, err
}
