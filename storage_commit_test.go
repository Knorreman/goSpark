package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTextFileSplitOnNewlineBoundary(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	dir := t.TempDir()
	path := filepath.Join(dir, "aligned.txt")
	content := "aaa\nbbb\nccc\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	got := Collect(TextFile(ctx, path, 3))
	if len(got) != 3 {
		t.Fatalf("expected 3 lines, got %d %v", len(got), got)
	}
	want := map[string]int{"aaa": 1, "bbb": 1, "ccc": 1}
	for _, line := range got {
		want[line]--
	}
	for k, v := range want {
		if v != 0 {
			t.Errorf("line %q count leftover %d", k, v)
		}
	}
}

func TestTextFileNoTrailingNewline(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	dir := t.TempDir()
	path := filepath.Join(dir, "nonewline.txt")
	if err := os.WriteFile(path, []byte("aaa\nbbb\nccc"), 0644); err != nil {
		t.Fatal(err)
	}
	got := Collect(TextFile(ctx, path, 2))
	if len(got) != 3 {
		t.Fatalf("expected 3 lines, got %d %v", len(got), got)
	}
}

func TestTextFileLongLine(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	dir := t.TempDir()
	path := filepath.Join(dir, "long.txt")
	long := strings.Repeat("x", 200)
	content := "short\n" + long + "\nend\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	got := Collect(TextFile(ctx, path, 4))
	foundLong := false
	if len(got) != 3 {
		t.Fatalf("expected 3 lines, got %d %v", len(got), got)
	}
	for _, line := range got {
		if line == long {
			foundLong = true
		}
	}
	if !foundLong {
		t.Fatalf("missing long line in %v", got)
	}
}

func TestSaveAsTextFileWritesSuccess(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	dir := t.TempDir()
	rdd := Parallelize(ctx, []string{"a", "b"}, 2)
	if err := SaveAsTextFile(rdd, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, SuccessFileName)); err != nil {
		t.Fatalf("missing _SUCCESS: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	parts := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "part-") {
			parts++
		}
	}
	if parts == 0 {
		t.Fatal("no part files after commit")
	}
}

func TestSaveAsTextFileNoSuccessOnError(t *testing.T) {
	ctx := newTestContext()
	defer ctx.Stop()
	rdd := Parallelize(ctx, []string{"a"}, 1)
	err := SaveAsTextFile(rdd, filepath.Join(t.TempDir(), "nope", "nested", "out"))
	if err == nil {
		if _, statErr := os.Stat(filepath.Join(t.TempDir(), SuccessFileName)); statErr == nil {
			t.Fatal("should not write _SUCCESS on failure")
		}
	}
	bad := filepath.Join("/proc/this-should-not-work-gospark", "out")
	err = SaveAsTextFile(rdd, bad)
	if err == nil {
		t.Fatal("expected save error")
	}
}

func TestMemS3TextFileWordCountSave(t *testing.T) {
	bucket := fmt.Sprintf("mem-wc-%d", os.Getpid())
	st := RegisterMemBucket(bucket)
	defer UnregisterMemBucket(bucket)

	input := "hello world\nhello spark\nworld spark hello\n"
	if err := st.Put("data/lines.txt", []byte(input)); err != nil {
		t.Fatal(err)
	}

	ctx := newTestContext()
	defer ctx.Stop()
	lines := TextFile(ctx, "s3://"+bucket+"/data/lines.txt", 2)
	words := FlatMap(lines, func(s string) []string { return strings.Fields(s) })
	pairs := Map(words, func(w string) Pair[string, int] { return NewPair(w, 1) })
	counts := ReduceByKey(pairs, NewHashPartitioner(2), func(a, b int) int { return a + b })
	if err := SaveAsTextFile(counts, "s3://"+bucket+"/out"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Head("out/_SUCCESS"); err != nil {
		t.Fatalf("missing s3 _SUCCESS: %v", err)
	}
	keys, err := st.List("out/")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, k := range keys {
		if !strings.Contains(k, "part-") {
			continue
		}
		data, err := st.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" {
				continue
			}
			got[line]++
		}
	}
	if len(got) == 0 {
		t.Fatalf("no output parts, keys=%v", keys)
	}
	sum := 0
	for _, p := range Collect(counts) {
		sum += p.Value
	}
	if sum != 7 {
		t.Fatalf("expected 7 tokens, got %d", sum)
	}
}
