package spark

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// externalSort sorts budget-sized runs and compacts them with binary merges.
// Only two runs are open in a merge; the run catalogue is logarithmic in input.
func externalSort(ctx *Context, input recordStream, less func(any, any) bool) (recordStream, int) {
	defer input.Close()
	dir, err := os.MkdirTemp(ctx.disk.dir, "sort-")
	must(err)
	codec := cancelCodec{ctx: ctx.TaskContext(), RecordCodec: DefaultCodec()}
	seq := 0
	path := func() string { seq++; return filepath.Join(dir, fmt.Sprint(seq)) }
	var levels []string
	merge := func(left, right string) string {
		l, err := openBucketFile(left, codec, ctx.recordBytes())
		must(err)
		defer l.Close()
		r, err := openBucketFile(right, codec, ctx.recordBytes())
		must(err)
		defer r.Close()
		target := path()
		w, err := newBucketWriter(target)
		must(err)
		defer w.suspend()
		a, ea := l.Next()
		b, eb := r.Next()
		for ea != io.EOF || eb != io.EOF {
			ctx.checkCanceled()
			if ea != nil && ea != io.EOF {
				must(ea)
			}
			if eb != nil && eb != io.EOF {
				must(eb)
			}
			var v any
			if eb == io.EOF || (ea != io.EOF && !less(b, a)) {
				v = a
				a, ea = l.Next()
			} else {
				v = b
				b, eb = r.Next()
			}
			p, err := codec.Encode(v)
			must(err)
			must(w.append(p))
		}
		_, err = w.finish()
		must(err)
		must(l.Close())
		must(r.Close())
		must(os.Remove(left))
		must(os.Remove(right))
		return target
	}
	push := func(run string) {
		for level := 0; ; level++ {
			if level == len(levels) {
				levels = append(levels, run)
				break
			}
			if levels[level] == "" {
				levels[level] = run
				break
			}
			run = merge(levels[level], run)
			levels[level] = ""
		}
	}
	var chunk []any
	bytes, count := 0, 0
	flush := func() {
		if len(chunk) == 0 {
			return
		}
		sort.SliceStable(chunk, func(i, j int) bool { return less(chunk[i], chunk[j]) })
		run := path()
		w, err := newBucketWriter(run)
		must(err)
		func() {
			defer w.suspend()
			for _, v := range chunk {
				p, err := codec.Encode(v)
				must(err)
				must(w.append(p))
			}
			_, err = w.finish()
			must(err)
		}()
		chunk = nil
		bytes = 0
		push(run)
	}
	for {
		ctx.checkCanceled()
		v, err := input.Next()
		if err == io.EOF {
			break
		}
		must(err)
		p, err := codec.Encode(v)
		must(err)
		must(recordLimit(len(p), ctx.recordBytes()))
		cost := len(p) + 64
		if cost > ctx.memoryBytes() {
			must(fmt.Errorf("record cannot fit shuffle memory budget: %d > %d", cost, ctx.memoryBytes()))
		}
		if bytes+cost > ctx.memoryBytes() {
			flush()
		}
		chunk = append(chunk, v)
		bytes += cost
		count++
	}
	flush()
	final := ""
	for i := len(levels) - 1; i >= 0; i-- {
		if levels[i] == "" {
			continue
		}
		if final == "" {
			final = levels[i]
		} else {
			final = merge(final, levels[i])
		}
	}
	if final == "" {
		final = path()
		w, err := newBucketWriter(final)
		must(err)
		_, err = w.finish()
		must(err)
	}
	r, err := openBucketFile(final, codec, ctx.recordBytes())
	must(err)
	s := &sortedRun{bucketReader: r, dir: dir}
	ctx.onClose(func() { _ = s.Close() })
	return s, count
}

type sortedRun struct {
	*bucketReader
	dir  string
	done bool
}

func (s *sortedRun) Next() (any, error) {
	v, err := s.bucketReader.Next()
	if err != nil {
		s.Close()
	}
	return v, err
}
func (s *sortedRun) Close() error {
	if s.done {
		return nil
	}
	s.done = true
	err := s.bucketReader.Close()
	removeErr := os.RemoveAll(s.dir)
	if err != nil {
		return err
	}
	return removeErr
}
