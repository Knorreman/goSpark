package spark

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"sort"
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

type textSpan struct {
	path   string
	offset int64
	length int64
}

type textFilePartition struct {
	index int
	files []textSpan
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
			return textInputPartitions(fs, filePath, numPartitions)
		},
		func() []Dependency { return nil },
		func(partition Partition) Iterator[string] {
			p := partition.(*textFilePartition)
			var iters []Iterator[string]
			for _, file := range p.files {
				iters = append(iters, newTextFileIteratorFS(fs, file.path, file.offset, file.length, ctx))
			}
			return ChainIterators(iters)
		},
	)
}

func textInputPartitions(fs FileSystem, filePath string, numPartitions int) []Partition {
	info, statErr := fs.Stat(filePath)
	if statErr == nil && !info.Dir {
		return splitTextFile(filePath, info.Size, numPartitions)
	}
	files, err := collectTextFiles(fs, filePath)
	if err != nil || len(files) == 0 {
		return []Partition{&textFilePartition{index: 0}}
	}
	if len(files) == 1 {
		return splitTextFile(files[0].path, files[0].length, numPartitions)
	}
	if numPartitions <= 0 {
		numPartitions = 1
	}
	if numPartitions > len(files) {
		numPartitions = len(files)
	}
	groups := make([][]textSpan, numPartitions)
	used := make([]int64, numPartitions)
	for _, file := range files {
		slot := 0
		for i := 1; i < numPartitions; i++ {
			if used[i] < used[slot] {
				slot = i
			}
		}
		groups[slot] = append(groups[slot], file)
		used[slot] += file.length
	}
	parts := make([]Partition, 0, numPartitions)
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		parts = append(parts, &textFilePartition{index: len(parts), files: group})
	}
	return parts
}

func splitTextFile(filePath string, fileSize int64, numPartitions int) []Partition {
	if fileSize <= 0 {
		return []Partition{&textFilePartition{index: 0, files: []textSpan{{path: filePath}}}}
	}
	if numPartitions <= 0 {
		numPartitions = 1
	}
	chunkSize := fileSize / int64(numPartitions)
	if chunkSize == 0 {
		chunkSize = fileSize
		numPartitions = 1
	}
	parts := make([]Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		offset := int64(i) * chunkSize
		length := chunkSize
		if i == numPartitions-1 {
			length = fileSize - offset
		}
		parts[i] = &textFilePartition{index: i, files: []textSpan{{path: filePath, offset: offset, length: length}}}
	}
	return parts
}

func collectTextFiles(fs FileSystem, root string) ([]textSpan, error) {
	listPath := root
	if fs.Scheme() != "file" && listPath != "" && !strings.HasSuffix(listPath, "/") {
		listPath += "/"
	}
	entries, err := fs.List(listPath)
	if err != nil {
		return nil, err
	}
	var out []textSpan
	for _, entry := range entries {
		name := entry.Name
		if fs.Scheme() == "file" {
			name = fs.Join(root, entry.Name)
		}
		base := path.Base(strings.TrimSuffix(name, "/"))
		if base == "." || base == ".." || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_") {
			continue
		}
		if fs.Scheme() == "file" {
			info, err := fs.Stat(name)
			if err != nil {
				return nil, err
			}
			if info.Dir {
				nested, err := collectTextFiles(fs, name)
				if err != nil {
					return nil, err
				}
				out = append(out, nested...)
				continue
			}
			out = append(out, textSpan{path: name, length: info.Size})
			continue
		}
		if strings.HasSuffix(name, "/") {
			continue
		}
		if !strings.HasPrefix(name, listPath) {
			continue
		}
		out = append(out, textSpan{path: name, length: entry.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
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
