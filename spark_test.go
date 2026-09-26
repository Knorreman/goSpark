package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestContext() *Context {
	return NewContext(&Config{
		AppName:       "test",
		Master:        "local[*]",
		NumPartitions: 4,
	})
}

func TestParallelizeCollect(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rdd := Parallelize(ctx, data, 4)

	result := Collect(rdd)
	if len(result) != 10 {
		t.Fatalf("expected 10 elements, got %d", len(result))
	}

	sum := 0
	for _, v := range result {
		sum += v
	}
	if sum != 55 {
		t.Fatalf("expected sum 55, got %d", sum)
	}
}

func TestMap(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5}
	rdd := Parallelize(ctx, data, 2)
	mapped := Map(rdd, func(x int) int { return x * 2 })

	result := Collect(mapped)
	if len(result) != 5 {
		t.Fatalf("expected 5 elements, got %d", len(result))
	}

	sum := 0
	for _, v := range result {
		sum += v
	}
	if sum != 30 {
		t.Fatalf("expected sum 30, got %d", sum)
	}
}

func TestFlatMap(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []string{"hello world", "foo bar"}
	rdd := Parallelize(ctx, data, 2)
	words := FlatMap(rdd, func(s string) []string { return splitWords(s) })

	result := Collect(words)
	if len(result) != 4 {
		t.Fatalf("expected 4 words, got %d: %v", len(result), result)
	}
}

func splitWords(s string) []string {
	var words []string
	start := 0
	for i, c := range s {
		if c == ' ' {
			if i > start {
				words = append(words, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		words = append(words, s[start:])
	}
	return words
}

func TestFilter(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rdd := Parallelize(ctx, data, 2)
	evens := Filter(rdd, func(x int) bool { return x%2 == 0 })

	result := Collect(evens)
	if len(result) != 5 {
		t.Fatalf("expected 5 evens, got %d: %v", len(result), result)
	}
}

func TestCount(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5}
	rdd := Parallelize(ctx, data, 2)

	count := Count(rdd)
	if count != 5 {
		t.Fatalf("expected count 5, got %d", count)
	}
}

func TestReduce(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5}
	rdd := Parallelize(ctx, data, 2)

	sum, ok := Reduce(rdd, func(a, b int) int { return a + b })
	if !ok {
		t.Fatal("expected reduce to succeed")
	}
	if sum != 15 {
		t.Fatalf("expected sum 15, got %d", sum)
	}
}

func TestFold(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5}
	rdd := Parallelize(ctx, data, 2)

	sum := Fold(rdd, 0, func(a, b int) int { return a + b })
	if sum != 15 {
		t.Fatalf("expected sum 15, got %d", sum)
	}
}

func TestTakeFirst(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{10, 20, 30, 40, 50}
	rdd := Parallelize(ctx, data, 2)

	result := Take(rdd, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(result))
	}

	first, ok := First(rdd)
	if !ok {
		t.Fatal("expected First to succeed")
	}
	if first != 10 && first != 20 && first != 30 && first != 40 && first != 50 {
		t.Fatalf("unexpected first value: %d", first)
	}
}

func TestMapPartitions(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6}
	rdd := Parallelize(ctx, data, 2)

	doubledAndSummed := MapPartitions(rdd, func(iter Iterator[int]) Iterator[int] {
		sum := 0
		for {
			v, ok := iter()
			if !ok {
				break
			}
			sum += v * 2
		}
		return SliceIterator([]int{sum})
	})

	result := Collect(doubledAndSummed)
	sum := 0
	for _, v := range result {
		sum += v
	}
	if sum != 42 {
		t.Fatalf("expected sum 42, got %d", sum)
	}
}

func TestDistinct(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 2, 3, 3, 3, 4, 4, 4, 4}
	rdd := Parallelize(ctx, data, 2)

	distinct := Distinct(rdd)
	result := Collect(distinct)
	if len(result) != 4 {
		t.Fatalf("expected 4 distinct elements, got %d: %v", len(result), result)
	}
}

func TestUnion(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2, 3}, 2)
	rdd2 := Parallelize(ctx, []int{4, 5, 6}, 2)

	combined := Union(rdd1, rdd2)
	result := Collect(combined)
	if len(result) != 6 {
		t.Fatalf("expected 6 elements, got %d: %v", len(result), result)
	}
}

func TestCoalesce(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := make([]int, 100)
	for i := range data {
		data[i] = i
	}
	rdd := Parallelize(ctx, data, 10)

	coalesced := Coalesce(rdd, 3)
	result := Collect(coalesced)
	if len(result) != 100 {
		t.Fatalf("expected 100 elements after coalesce, got %d", len(result))
	}
	if coalesced.GetNumPartitions() > 3 {
		t.Fatalf("expected at most 3 partitions, got %d", coalesced.GetNumPartitions())
	}
}

func TestZip(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2, 3}, 2)
	rdd2 := Parallelize(ctx, []string{"a", "b", "c"}, 2)

	zipped := Zip(rdd1, rdd2)
	result := Collect(zipped)
	if len(result) != 3 {
		t.Fatalf("expected 3 pairs, got %d", len(result))
	}
}

func TestSubtract(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2)
	rdd2 := Parallelize(ctx, []int{3, 4}, 1)

	result := Subtract(rdd1, rdd2)
	collected := Collect(result)
	if len(collected) != 3 {
		t.Fatalf("expected 3 elements, got %d: %v", len(collected), collected)
	}
}

func TestIntersection(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2, 3, 4, 5}, 2)
	rdd2 := Parallelize(ctx, []int{3, 4, 5, 6}, 2)

	result := Intersection(rdd1, rdd2)
	collected := Collect(result)
	if len(collected) != 3 {
		t.Fatalf("expected 3 elements, got %d: %v", len(collected), collected)
	}

	dup1 := Parallelize(ctx, []int{1, 1, 2, 3, 3}, 2)
	dup2 := Parallelize(ctx, []int{1, 3, 7}, 1)
	distinct := Collect(Intersection(dup1, dup2))
	if len(distinct) != 2 {
		t.Fatalf("expected 2 distinct intersection elements, got %d: %v", len(distinct), distinct)
	}
}

func TestReduceByKey(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 1), NewPair("b", 2), NewPair("a", 3),
		NewPair("b", 4), NewPair("c", 5),
	}
	rdd := Parallelize(ctx, data, 2)

	reduced := ReduceByKey(rdd, NewHashPartitioner(2), func(a, b int) int { return a + b })
	result := Collect(reduced)

	counts := make(map[string]int)
	for _, p := range result {
		counts[p.Key] = p.Value
	}

	if counts["a"] != 4 {
		t.Fatalf("expected a=4, got a=%d", counts["a"])
	}
	if counts["b"] != 6 {
		t.Fatalf("expected b=6, got b=%d", counts["b"])
	}
	if counts["c"] != 5 {
		t.Fatalf("expected c=5, got c=%d", counts["c"])
	}
}

func TestGroupByKey(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 1), NewPair("b", 2), NewPair("a", 3),
		NewPair("b", 4), NewPair("c", 5),
	}
	rdd := Parallelize(ctx, data, 2)

	grouped := GroupByKey(rdd, NewHashPartitioner(2))
	result := Collect(grouped)

	groups := make(map[string][]int)
	for _, p := range result {
		groups[p.Key] = p.Value
	}

	if len(groups["a"]) != 2 {
		t.Fatalf("expected 2 values for a, got %d", len(groups["a"]))
	}
	if len(groups["b"]) != 2 {
		t.Fatalf("expected 2 values for b, got %d", len(groups["b"]))
	}
	if len(groups["c"]) != 1 {
		t.Fatalf("expected 1 value for c, got %d", len(groups["c"]))
	}
}

func TestCountByValue(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 2, 3, 3, 3}
	rdd := Parallelize(ctx, data, 2)

	counts := CountByValue(rdd)
	if counts[1] != 1 {
		t.Fatalf("expected count of 1: 1, got %d", counts[1])
	}
	if counts[2] != 2 {
		t.Fatalf("expected count of 2: 2, got %d", counts[2])
	}
	if counts[3] != 3 {
		t.Fatalf("expected count of 3: 3, got %d", counts[3])
	}
}

func TestMapValues(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 1), NewPair("b", 2),
	}
	rdd := Parallelize(ctx, data, 1)

	mapped := MapValues(rdd, func(v int) int { return v * 2 })
	result := Collect(mapped)

	for _, p := range result {
		if p.Key == "a" && p.Value != 2 {
			t.Fatalf("expected a=2, got a=%d", p.Value)
		}
		if p.Key == "b" && p.Value != 4 {
			t.Fatalf("expected b=4, got b=%d", p.Value)
		}
	}
}

func TestKeysValues(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 1), NewPair("b", 2), NewPair("c", 3),
	}
	rdd := Parallelize(ctx, data, 1)

	keys := Collect(Keys(rdd))
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	vals := Collect(Values(rdd))
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
}

func TestKeyBy(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []string{"hello", "world"}
	rdd := Parallelize(ctx, data, 1)

	keyed := KeyBy(rdd, func(s string) int { return len(s) })
	result := Collect(keyed)
	if len(result) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(result))
	}
}

func TestGlom(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6}
	rdd := Parallelize(ctx, data, 2)

	glommed := Glom(rdd)
	result := Collect(glommed)
	totalCount := 0
	for _, arr := range result {
		totalCount += len(arr)
	}
	if totalCount != 6 {
		t.Fatalf("expected 6 total elements, got %d", totalCount)
	}
}

func TestHashPartitioner(t *testing.T) {
	p := NewHashPartitioner(4)
	if p.NumPartitions() != 4 {
		t.Fatalf("expected 4 partitions, got %d", p.NumPartitions())
	}
}

func TestEmptyRDD(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd := EmptyRDD[int](ctx)
	result := Collect(rdd)
	if len(result) != 0 {
		t.Fatalf("expected 0 elements, got %d", len(result))
	}

	count := Count(rdd)
	if count != 0 {
		t.Fatalf("expected count 0, got %d", count)
	}
}

func TestForeach(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5}
	rdd := Parallelize(ctx, data, 2)

	var sum atomic.Int64
	ForEach(rdd, func(x int) { sum.Add(int64(x)) })
	if sum.Load() != 15 {
		t.Fatalf("expected sum 15, got %d", sum.Load())
	}
}

func TestMin(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{5, 3, 8, 1, 9, 2}
	rdd := Parallelize(ctx, data, 2)

	min, ok := Min(rdd, func(a, b int) bool { return a < b })
	if !ok {
		t.Fatal("expected Min to succeed")
	}
	if min != 1 {
		t.Fatalf("expected min 1, got %d", min)
	}

	max, ok := Max(rdd, func(a, b int) bool { return a < b })
	if !ok {
		t.Fatal("expected Max to succeed")
	}
	if max != 9 {
		t.Fatalf("expected max 9, got %d", max)
	}
}

func TestWordCount(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	lines := []string{"hello world", "hello spark", "world is great"}
	rdd := Parallelize(ctx, lines, 2)

	words := FlatMap(rdd, func(line string) []string {
		return splitWords(line)
	})

	pairs := Map(words, func(word string) Pair[string, int] {
		return NewPair(word, 1)
	})

	counts := ReduceByKey(pairs, NewHashPartitioner(2), func(a, b int) int { return a + b })

	result := Collect(counts)
	wordMap := make(map[string]int)
	for _, p := range result {
		wordMap[p.Key] = p.Value
	}

	if wordMap["hello"] != 2 {
		t.Fatalf("expected hello=2, got hello=%d", wordMap["hello"])
	}
	if wordMap["world"] != 2 {
		t.Fatalf("expected world=2, got world=%d", wordMap["world"])
	}
	if wordMap["spark"] != 1 {
		t.Fatalf("expected spark=1, got spark=%d", wordMap["spark"])
	}
}

func TestIteratorUtilities(t *testing.T) {
	iter := SliceIterator([]int{1, 2, 3, 4, 5})

	mapped := MapIterator(iter, func(x int) int { return x * 2 })
	result := CollectIterator(mapped)
	if len(result) != 5 {
		t.Fatalf("expected 5 elements, got %d", len(result))
	}

	iter2 := SliceIterator([]int{1, 2, 3, 4, 5})
	filtered := FilterIterator(iter2, func(x int) bool { return x%2 == 0 })
	result2 := CollectIterator(filtered)
	if len(result2) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(result2))
	}

	iter3 := SliceIterator([]int{1, 2, 3})
	fm := FlatMapIterator(iter3, func(x int) []int { return []int{x, x * 10} })
	result3 := CollectIterator(fm)
	if len(result3) != 6 {
		t.Fatalf("expected 6 elements, got %d", len(result3))
	}
}

func TestCartesian(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2}, 1)
	rdd2 := Parallelize(ctx, []string{"a", "b"}, 1)

	product := Cartesian(rdd1, rdd2)
	result := Collect(product)
	if len(result) != 4 {
		t.Fatalf("expected 4 pairs, got %d", len(result))
	}
}

func TestRepartition(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := make([]int, 100)
	for i := range data {
		data[i] = i
	}
	rdd := Parallelize(ctx, data, 2)

	repartitioned := Repartition(rdd, 4)
	result := Collect(repartitioned)
	if len(result) != 100 {
		t.Fatalf("expected 100 elements, got %d", len(result))
	}
	if repartitioned.GetNumPartitions() != 4 {
		t.Fatalf("expected 4 partitions, got %d", repartitioned.GetNumPartitions())
	}
}

func TestSaveAsTextFile(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	tmpDir, err := os.MkdirTemp("", "gospark-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	data := []string{"hello", "world", "foo", "bar"}
	rdd := Parallelize(ctx, data, 2)

	if err := SaveAsTextFile(rdd, tmpDir); err != nil {
		t.Fatalf("SaveAsTextFile failed: %v", err)
	}

	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	partFiles := 0
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[:4] == "part" {
			partFiles++
		}
	}

	if partFiles == 0 {
		t.Fatalf("expected partition files, got %d", partFiles)
	}

	totalLines := 0
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[:4] == "part" {
			content, err := os.ReadFile(filepath.Join(tmpDir, e.Name()))
			if err != nil {
				t.Fatalf("ReadFile failed: %v", err)
			}
			lines := 0
			for _, c := range content {
				if c == '\n' {
					lines++
				}
			}
			if lines > 0 && content[len(content)-1] != '\n' {
				lines++
			}
			totalLines += lines
		}
	}

	if totalLines != len(data) {
		t.Fatalf("expected %d total lines, got %d", len(data), totalLines)
	}
}

func TestForeachPartition(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8}
	rdd := Parallelize(ctx, data, 2)

	partitionCounts := make(map[int]int)
	var mu sync.Mutex

	ForeachPartition(rdd, func(idx int, iter Iterator[int]) {
		count := 0
		for {
			_, ok := iter()
			if !ok {
				break
			}
			count++
		}
		mu.Lock()
		partitionCounts[idx] = count
		mu.Unlock()
	})

	totalElements := 0
	for _, count := range partitionCounts {
		totalElements += count
	}

	if totalElements != len(data) {
		t.Fatalf("expected %d total elements across partitions, got %d", len(data), totalElements)
	}
}

func TestSaveAsTextFileWithPartitionWriter(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{10, 20, 30, 40}
	rdd := Parallelize(ctx, data, 2)

	tmpDir, err := os.MkdirTemp("", "gospark-test-pw-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	var writeCalls atomic.Int64
	err = SaveAsTextFileWith(rdd, tmpDir, func(basePath string, partitionIndex int, lines []string) error {
		writeCalls.Add(1)
		os.MkdirAll(basePath, 0755)
		f, err := os.Create(filepath.Join(basePath, fmt.Sprintf("part-%05d", partitionIndex)))
		if err != nil {
			return err
		}
		defer f.Close()
		for _, line := range lines {
			fmt.Fprintln(f, line)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("SaveAsTextFileWith failed: %v", err)
	}
	if writeCalls.Load() == 0 {
		t.Fatalf("expected at least 1 write call, got %d", writeCalls.Load())
	}
}

func TestResolvePathLocal(t *testing.T) {
	tests := []struct {
		uri      string
		scheme   string
		basePath string
	}{
		{"/tmp/output", "file", "/tmp/output"},
		{"file:///tmp/output", "file", "/tmp/output"},
		{"/home/user/data", "file", "/home/user/data"},
	}

	for _, tt := range tests {
		resolved := ResolvePath(tt.uri)
		if resolved.FS.Scheme() != tt.scheme {
			t.Errorf("ResolvePath(%q).FS.Scheme() = %q, want %q", tt.uri, resolved.FS.Scheme(), tt.scheme)
		}
		if resolved.BasePath != tt.basePath {
			t.Errorf("ResolvePath(%q).BasePath = %q, want %q", tt.uri, resolved.BasePath, tt.basePath)
		}
	}
}

func TestResolvePathS3(t *testing.T) {
	tests := []struct {
		uri      string
		scheme   string
		bucket   string
		basePath string
	}{
		{"s3a://my-bucket/output", "s3a", "my-bucket", "output"},
		{"s3a://my-bucket/path/to/output", "s3a", "my-bucket", "path/to/output"},
		{"s3a://my-bucket", "s3a", "my-bucket", ""},
		{"s3://other-bucket/data", "s3a", "other-bucket", "data"},
	}

	for _, tt := range tests {
		resolved := ResolvePath(tt.uri)
		if resolved.FS.Scheme() != tt.scheme {
			t.Errorf("ResolvePath(%q).FS.Scheme() = %q, want %q", tt.uri, resolved.FS.Scheme(), tt.scheme)
		}
		s3fs, ok := resolved.FS.(S3FS)
		if !ok {
			t.Fatalf("ResolvePath(%q).FS is not S3FS", tt.uri)
		}
		if s3fs.Bucket != tt.bucket {
			t.Errorf("ResolvePath(%q) bucket = %q, want %q", tt.uri, s3fs.Bucket, tt.bucket)
		}
		if resolved.BasePath != tt.basePath {
			t.Errorf("ResolvePath(%q).BasePath = %q, want %q", tt.uri, resolved.BasePath, tt.basePath)
		}
	}
}

func TestS3FSCreateReturnsError(t *testing.T) {
	UnregisterMemBucket("test-bucket")
	s3fs := S3FS{Bucket: "test-bucket"}
	_, err := s3fs.Create("some/key")
	if err == nil && !currentS3Config().enabled() {
		t.Error("expected S3FS.Create to return error without S3 config")
	}
}

func TestS3FSMkdirAllNoop(t *testing.T) {
	s3fs := S3FS{Bucket: "test-bucket"}
	err := s3fs.MkdirAll("some/prefix", 0755)
	if err != nil {
		t.Errorf("expected S3FS.MkdirAll to be no-op, got error: %v", err)
	}
}

func TestSaveAsTextFileWithFileScheme(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []string{"hello", "world", "foo"}
	rdd := Parallelize(ctx, data, 2)

	tmpDir, err := os.MkdirTemp("", "gospark-test-file-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	path := "file://" + tmpDir
	if err := SaveAsTextFile(rdd, path); err != nil {
		t.Fatalf("SaveAsTextFile with file:// scheme failed: %v", err)
	}

	entries, _ := os.ReadDir(tmpDir)
	partFiles := 0
	for _, e := range entries {
		if len(e.Name()) >= 4 && e.Name()[:4] == "part" {
			partFiles++
		}
	}
	if partFiles == 0 {
		t.Fatalf("expected partition files, got %d", partFiles)
	}
}

func TestPartitionFileName(t *testing.T) {
	tests := []struct {
		index int
		want  string
	}{
		{0, "part-00000"},
		{1, "part-00001"},
		{99, "part-00099"},
		{999, "part-00999"},
	}
	for _, tt := range tests {
		got := PartitionFileName(tt.index)
		if got != tt.want {
			t.Errorf("PartitionFileName(%d) = %q, want %q", tt.index, got, tt.want)
		}
	}
}

func TestTaskRegistry(t *testing.T) {
	RegisterPipeline("test-registry", func(ctx *Context, spec JobSpec) (*RDD[int], error) {
		return Parallelize(ctx, []int{1, 2, 3}, spec.NumPartitions), nil
	})
	result, err := RunPipelineLocal[int](JobSpec{TaskName: "test-registry", NumPartitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(result))
	}
}

func TestWorkerRunPartition(t *testing.T) {
	RegisterPipeline("test-run-partition", func(ctx *Context, spec JobSpec) (*RDD[string], error) {
		return Parallelize(ctx, []string{"a", "b", "c", "d", "e", "f"}, spec.NumPartitions), nil
	})

	tmpDir, err := os.MkdirTemp("", "gospark-test-runpart-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := newTestContext()
	defer ctx.Stop()

	factory, _ := getJob("test-run-partition")
	rddAny, err := factory(ctx, JobSpec{TaskName: "test-run-partition", NumPartitions: 3})
	if err != nil {
		t.Fatal(err)
	}

	err = RunPartition(rddAny, 0, tmpDir)
	if err != nil {
		t.Fatalf("RunPartition failed: %v", err)
	}

	entries, _ := os.ReadDir(tmpDir)
	found := false
	for _, e := range entries {
		if e.Name() == "part-00000" {
			found = true
			content, _ := os.ReadFile(filepath.Join(tmpDir, e.Name()))
			lines := 0
			for _, c := range content {
				if c == '\n' {
					lines++
				}
			}
			if lines == 0 {
				t.Error("expected at least one line in partition 0")
			}
		}
	}
	if !found {
		t.Error("expected part-00000 file to exist")
	}
}

func TestCartesianMultiPartition(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	rdd1 := Parallelize(ctx, []int{1, 2, 3, 4}, 2)
	rdd2 := Parallelize(ctx, []string{"a", "b"}, 2)
	result := Collect(Cartesian(rdd1, rdd2))
	if len(result) != 8 {
		t.Fatalf("expected 8 pairs, got %d: %v", len(result), result)
	}
}

func TestJoin(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	users := []Pair[int, string]{
		NewPair(1, "Alice"), NewPair(2, "Bob"), NewPair(3, "Carol"), NewPair(4, "Dave"),
	}
	orders := []Pair[int, int]{
		NewPair(1, 10), NewPair(1, 20), NewPair(2, 30), NewPair(4, 40), NewPair(4, 50),
	}
	partitioner := NewHashPartitioner(3)
	joined := Join(Parallelize(ctx, users, 2), Parallelize(ctx, orders, 2), partitioner)
	result := Collect(joined)
	if len(result) != 5 {
		t.Fatalf("expected 5 join rows, got %d: %v", len(result), result)
	}

	left := Collect(LeftOuterJoin(Parallelize(ctx, users, 2), Parallelize(ctx, orders, 2), partitioner))
	if len(left) != 6 {
		t.Fatalf("expected 6 left-outer rows (5 matches + Carol), got %d: %v", len(left), left)
	}
}

func TestCombineByKey(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 2), NewPair("a", 3), NewPair("b", 4),
	}
	rdd := Parallelize(ctx, data, 2)
	combined := CombineByKey(rdd,
		func(v int) int { return v },
		func(c int, v int) int { return c + v },
		func(a, b int) int { return a * b },
		NewHashPartitioner(2),
	)
	result := Collect(combined)
	got := make(map[string]int)
	for _, p := range result {
		got[p.Key] = p.Value
	}
	if got["a"] != 5 {
		t.Fatalf("expected a=5, got %d", got["a"])
	}
	if got["b"] != 4 {
		t.Fatalf("expected b=4, got %d", got["b"])
	}
}

func TestFoldByKey(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []Pair[string, int]{
		NewPair("a", 1), NewPair("a", 2), NewPair("b", 3),
	}
	rdd := Parallelize(ctx, data, 2)
	folded := FoldByKey(rdd, 10, func(a, b int) int { return a + b }, NewHashPartitioner(1))
	result := Collect(folded)
	got := make(map[string]int)
	for _, p := range result {
		got[p.Key] = p.Value
	}
	if got["a"] != 23 {
		t.Fatalf("expected a=23, got %d", got["a"])
	}
	if got["b"] != 13 {
		t.Fatalf("expected b=13, got %d", got["b"])
	}
}

func TestTextFile(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	tmpDir, err := os.MkdirTemp("", "gospark-textfile-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	const n = 50
	lines := make([]string, n)
	for i := 0; i < n; i++ {
		lines[i] = fmt.Sprintf("line-%02d", i)
	}
	path := filepath.Join(tmpDir, "input.txt")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	result := Collect(TextFile(ctx, path, 3))
	if len(result) != n {
		t.Fatalf("expected %d lines, got %d: %v", n, len(result), result)
	}
	seen := make(map[string]int)
	for _, line := range result {
		seen[line]++
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("line-%02d", i)
		if seen[want] != 1 {
			t.Errorf("line %q count = %d, want 1", want, seen[want])
		}
	}
}

func TestCache(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()

	data := []int{1, 2, 3, 4, 5, 6, 7, 8}
	rdd := Parallelize(ctx, data, 2)
	var n atomic.Int64
	mapped := Map(rdd, func(x int) int {
		n.Add(1)
		return x * 2
	})
	Cache(mapped)
	r1 := Collect(mapped)
	r2 := Collect(mapped)
	if n.Load() != int64(len(data)) {
		t.Fatalf("expected compute %d times, got %d", len(data), n.Load())
	}
	if len(r1) != len(data) || len(r2) != len(data) {
		t.Fatalf("unexpected lengths %d %d", len(r1), len(r2))
	}

	Unpersist(mapped)
	Collect(mapped)
	if n.Load() != int64(2*len(data)) {
		t.Fatalf("expected compute %d times after unpersist, got %d", 2*len(data), n.Load())
	}
}

func TestPersistDisk(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	data := []int{1, 2, 3, 4}
	var n atomic.Int64
	mapped := Map(Parallelize(ctx, data, 2), func(x int) int {
		n.Add(1)
		return x * 2
	})
	Persist(mapped, StorageDisk)
	_ = Collect(mapped)
	_ = Collect(mapped)
	if n.Load() != int64(len(data)) {
		t.Fatalf("disk persist recomputed: %d", n.Load())
	}
	Unpersist(mapped)
	_ = Collect(mapped)
	if n.Load() != int64(2*len(data)) {
		t.Fatalf("after unpersist: %d", n.Load())
	}
}

func TestSpillShuffleStillCorrect(t *testing.T) {
	ctx := newTestContext()
	ctx.Config().ShuffleMemoryBytes = 256
	defer ctx.Stop()
	pairs := Map(Parallelize(ctx, []string{"a", "a", "b"}, 2), func(s string) Pair[string, int] {
		return NewPair(s, 1)
	})
	got := Collect(ReduceByKey(pairs, NewHashPartitioner(2), func(a, b int) int { return a + b }))
	sums := map[string]int{}
	for _, p := range got {
		sums[p.Key] = p.Value
	}
	if sums["a"] != 2 || sums["b"] != 1 {
		t.Fatalf("spill wordcount %v", sums)
	}
}

func TestRunPartitionOutOfRange(t *testing.T) {
	RegisterPipeline("test-out-of-range", func(ctx *Context, spec JobSpec) (*RDD[int], error) {
		return Parallelize(ctx, []int{1, 2, 3}, spec.NumPartitions), nil
	})

	ctx := newTestContext()
	defer ctx.Stop()

	factory, _ := getJob("test-out-of-range")
	rddAny, err := factory(ctx, JobSpec{TaskName: "test-out-of-range", NumPartitions: 2})
	if err != nil {
		t.Fatal(err)
	}

	err = RunPartition(rddAny, 99, "/tmp/nonexistent")
	if err == nil {
		t.Error("expected error for out-of-range partition index")
	}
}
