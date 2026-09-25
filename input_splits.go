package spark

import "sort"

type InputSplit struct {
	Source    string `json:"source"`
	Call      int    `json:"call"`
	Path      string `json:"path"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
	Partition int    `json:"partition"`
}

type textInputKey struct {
	source string
	call   int
}

var hideTextListing func(path string) bool

func (c *Context) nextTextInputSeq() int {
	n := c.textSeq
	c.textSeq++
	return n
}

func (c *Context) installInputSplits(splits []InputSplit) {
	if c == nil || len(splits) == 0 {
		return
	}
	c.textInputs = make(map[textInputKey][]InputSplit, len(splits))
	for _, split := range splits {
		key := textInputKey{split.Source, split.Call}
		c.textInputs[key] = append(c.textInputs[key], split)
	}
}

func plannedTextPartitions(ctx *Context, source string, call int) ([]Partition, bool) {
	if ctx == nil || ctx.textInputs == nil {
		return nil, false
	}
	splits, ok := ctx.textInputs[textInputKey{source, call}]
	if !ok {
		return nil, false
	}
	byPart := map[int][]textSpan{}
	var ids []int
	seen := map[int]bool{}
	for _, split := range splits {
		if !seen[split.Partition] {
			seen[split.Partition] = true
			ids = append(ids, split.Partition)
		}
		if split.Path != "" {
			byPart[split.Partition] = append(byPart[split.Partition], textSpan{
				path: split.Path, offset: split.Offset, length: split.Length,
			})
		}
	}
	sort.Ints(ids)
	parts := make([]Partition, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, &textFilePartition{
			index: id, source: source, seq: call, files: byPart[id],
		})
	}
	return parts, true
}

func captureInputSplits(root RDDAny) []InputSplit {
	if root == nil {
		return nil
	}
	seenRDD := map[int]bool{}
	seenPart := map[Partition]bool{}
	var splits []InputSplit
	var walk func(RDDAny)
	walk = func(rdd RDDAny) {
		if rdd == nil || seenRDD[rdd.ID()] {
			return
		}
		seenRDD[rdd.ID()] = true
		for _, part := range rdd.Partitions() {
			tp, ok := part.(*textFilePartition)
			if !ok || part == nil || seenPart[part] {
				continue
			}
			seenPart[part] = true
			if len(tp.files) == 0 {
				splits = append(splits, InputSplit{Source: tp.source, Call: tp.seq, Partition: tp.index})
				continue
			}
			for _, file := range tp.files {
				splits = append(splits, InputSplit{
					Source:    tp.source,
					Call:      tp.seq,
					Path:      file.path,
					Offset:    file.offset,
					Length:    file.length,
					Partition: tp.index,
				})
			}
		}
		for _, dep := range rdd.Dependencies() {
			if dep != nil {
				walk(dep.Parent())
			}
		}
	}
	walk(root)
	if len(splits) == 0 {
		return nil
	}
	return splits
}

func inputSplitLess(a, b InputSplit) bool {
	if a.Source != b.Source {
		return a.Source < b.Source
	}
	if a.Call != b.Call {
		return a.Call < b.Call
	}
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	if a.Offset != b.Offset {
		return a.Offset < b.Offset
	}
	return a.Length < b.Length
}
