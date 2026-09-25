package spark

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func init() {
	RegisterJob("dir-input-wc", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		lines := TextFile(ctx, spec.Params["path"], spec.NumPartitions)
		words := FlatMap(lines, func(line string) []string {
			if line == "" {
				return nil
			}
			return []string{line}
		})
		pairs := Map(words, func(word string) Pair[string, int] { return NewPair(word, 1) })
		return ReduceByKey(pairs, NewHashPartitioner(spec.NumPartitions), func(a, b int) int { return a + b }), nil
	})
}

func TestTextFileDirectoryAndNestedFiles(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "a\nb\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "a\nc\n")
	writeInput(t, filepath.Join(dir, "_SUCCESS"), "ignore\n")
	writeInput(t, filepath.Join(dir, ".hidden"), "hidden\n")
	if err := os.Mkdir(filepath.Join(dir, "part"), 0755); err != nil {
		t.Fatal(err)
	}
	writeInput(t, filepath.Join(dir, "part", "c.txt"), "c\n")
	ctx := NewContext(&Config{AppName: "dir-input", Master: MasterLocal})
	defer ctx.Stop()
	got := Collect(TextFile(ctx, dir, 2))
	if !reflect.DeepEqual(countStrings(got), map[string]int{"a": 2, "b": 1, "c": 2}) {
		t.Fatalf("directory input = %v", got)
	}
	if len(TextFile(ctx, dir, 2).Partitions()) != 2 {
		t.Fatal("expected files to be packed into the requested partitions")
	}
}

func TestTextFileKeepsSingleObjectSeparateFromPrefix(t *testing.T) {
	bucket := fmt.Sprintf("text-prefix-%d", os.Getpid())
	store := RegisterMemBucket(bucket)
	defer UnregisterMemBucket(bucket)
	if err := store.Put("data/lines.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("data/lines.txt.extra", []byte("two\n")); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("data/part-000/rows.txt", []byte("three\n")); err != nil {
		t.Fatal(err)
	}
	ctx := NewContext(&Config{AppName: "s3-input", Master: MasterLocal})
	defer ctx.Stop()
	one := Collect(TextFile(ctx, "s3://"+bucket+"/data/lines.txt", 2))
	if !reflect.DeepEqual(one, []string{"one"}) {
		t.Fatalf("single object included prefix siblings: %v", one)
	}
	all := Collect(TextFile(ctx, "s3://"+bucket+"/data/", 2))
	if !reflect.DeepEqual(countStrings(all), map[string]int{"one": 1, "two": 1, "three": 1}) {
		t.Fatalf("prefix input = %v", all)
	}
}

func TestDistributedDirectoryInput(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, filepath.Join(dir, "a.txt"), "alpha\n")
	writeInput(t, filepath.Join(dir, "b.txt"), "alpha\nbeta\n")
	recs, err := Schedule(JobSpec{
		TaskName: "dir-input-wc", Action: ActionCollect, NumPartitions: 2,
		Params: map[string]string{"path": dir},
	}, []TaskRunner{startTestWorker(t), startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, rec := range recs {
		p := rec.(Pair[string, int])
		got[p.Key] += p.Value
	}
	if !reflect.DeepEqual(got, map[string]int{"alpha": 2, "beta": 1}) {
		t.Fatalf("distributed directory input = %v", got)
	}
}

func writeInput(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

func countStrings(values []string) map[string]int {
	out := map[string]int{}
	for _, value := range values {
		out[value]++
	}
	return out
}
