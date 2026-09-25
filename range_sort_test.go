package spark

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func init() {
	RegisterJob("range-sort-check", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		var input []Pair[int, int]
		for i := 127; i >= 0; i-- {
			input = append(input, NewPair(i, i*10))
		}
		for i := 0; i < 7; i++ {
			input = append(input, NewPair(50, i))
		}
		return SortByKey(Parallelize(ctx, input, 4), func(a, b int) bool { return a < b }, spec.Params["descending"] != "true", 4), nil
	})
	RegisterJob("range-sort-edge", func(ctx *Context, spec JobSpec) (RDDAny, error) {
		var input []Pair[int, int]
		if spec.Params["empty"] != "true" {
			for i := 0; i < 500; i++ {
				input = append(input, NewPair(7, i))
			}
		}
		return SortByKey(Parallelize(ctx, input, 4), func(a, b int) bool { return a < b }, true, 4), nil
	})
}

func TestDistributedRangeSortPartitions(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprint("descending=", descending), func(t *testing.T) {
			spec := JobSpec{TaskName: "range-sort-check", Action: ActionCollect, NumPartitions: 4,
				Params: map[string]string{"descending": fmt.Sprint(descending)}}
			var mu sync.Mutex
			bucketRecords := make([]int, 4)
			mapTasks := 0
			parts := make(map[int][]any)
			recs, err := ScheduleWith(spec, []TaskRunner{startTestWorker(t), startTestWorker(t)}, ScheduleOpts{
				OnTaskComplete: func(task Task, result ExecResult) error {
					mu.Lock()
					defer mu.Unlock()
					if result.Manifest != nil {
						mapTasks++
						if len(result.Manifest.Buckets) != 4 {
							return fmt.Errorf("expected four range buckets, got %d", len(result.Manifest.Buckets))
						}
						for _, b := range result.Manifest.Buckets {
							bucketRecords[b.ReduceID] += b.Records
						}
					} else {
						parts[task.PartitionID] = result.Records
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if mapTasks != 4 || len(parts) != 4 {
				t.Fatalf("mapTasks=%d resultPartitions=%d", mapTasks, len(parts))
			}
			occupied := 0
			for _, n := range bucketRecords {
				if n > 0 {
					occupied++
				}
			}
			if occupied < 3 {
				t.Fatalf("sampled range partitioning collapsed into %d buckets: %v", occupied, bucketRecords)
			}
			var keys []int
			for p := 0; p < 4; p++ {
				if len(parts[p]) != bucketRecords[p] {
					t.Fatalf("partition %d has %d results but %d map records", p, len(parts[p]), bucketRecords[p])
				}
				for _, rec := range parts[p] {
					keys = append(keys, rec.(Pair[int, int]).Key)
				}
			}
			if len(recs) != len(keys) || len(keys) != 135 {
				t.Fatalf("lost records: scheduled=%d partitioned=%d", len(recs), len(keys))
			}
			for i := 1; i < len(keys); i++ {
				if (!descending && keys[i] < keys[i-1]) || (descending && keys[i] > keys[i-1]) {
					t.Fatalf("out of order at %d: %v", i, keys)
				}
			}
			var want []int
			for i := 0; i < 128; i++ {
				want = append(want, i)
			}
			for i := 0; i < 7; i++ {
				want = append(want, 50)
			}
			if !reflect.DeepEqual(countKeys(keys), countKeys(want)) {
				t.Fatalf("missing or duplicate values: %v", countKeys(keys))
			}
		})
	}
}

func countKeys(keys []int) map[int]int {
	out := map[int]int{}
	for _, key := range keys {
		out[key]++
	}
	return out
}

type failFirstSample struct {
	inner TaskRunner
	once  sync.Once
}

func (r *failFirstSample) Exec(task Task) (ExecResult, error) {
	if task.Sample {
		failed := false
		r.once.Do(func() { failed = true })
		if failed {
			return ExecResult{}, fmt.Errorf("injected sample failure")
		}
	}
	return r.inner.Exec(task)
}

func TestRangeSortSampleRetryAndMissingBounds(t *testing.T) {
	spec := JobSpec{TaskName: "range-sort-check", Action: ActionCollect, NumPartitions: 4}
	plan, err := PlanJob(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Stages[0].RangeSort {
		t.Fatal("range shuffle not marked in plan")
	}
	_, err = ExecuteTask(Task{JobID: "unsampled-sort", Job: spec, StageID: plan.Stages[0].ID, StoreDir: t.TempDir(), Fingerprint: plan.Fingerprint})
	if err == nil || !strings.Contains(err.Error(), "missing sampled boundaries") {
		t.Fatalf("unsampled shuffle was accepted: %v", err)
	}
	recs, err := ScheduleWith(spec, []TaskRunner{&failFirstSample{inner: localRunner{storeDir: t.TempDir()}}}, ScheduleOpts{MaxAttempts: 2})
	if err != nil || len(recs) != 135 {
		t.Fatalf("sample retry failed: count=%d err=%v", len(recs), err)
	}
}

func TestRangeSortEmptyAndSkewedKeys(t *testing.T) {
	for _, empty := range []bool{false, true} {
		spec := JobSpec{TaskName: "range-sort-edge", Action: ActionCollect, NumPartitions: 4,
			Params: map[string]string{"empty": fmt.Sprint(empty)}}
		partitions := map[int]int{}
		recs, err := ScheduleWith(spec, []TaskRunner{startTestWorker(t), startTestWorker(t)}, ScheduleOpts{
			OnTaskComplete: func(task Task, result ExecResult) error {
				if result.Kind == StageResult {
					partitions[task.PartitionID] = len(result.Records)
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		want := 500
		if empty {
			want = 0
		}
		if len(recs) != want || len(partitions) != 4 {
			t.Fatalf("empty=%t records=%d partitions=%v", empty, len(recs), partitions)
		}
		if !empty && partitions[3] != 500 {
			t.Fatalf("equal keys crossed a range boundary: %v", partitions)
		}
		ctx := NewContext(&Config{AppName: "range-sort-edge", Master: MasterLocal, NumPartitions: 4})
		factory, _ := GetJob(spec.TaskName)
		rdd, err := factory(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		local := Collect(rdd.(*RDD[Pair[int, int]]))
		ctx.Stop()
		if len(local) != want {
			t.Fatalf("local sort empty=%t got %d want %d", empty, len(local), want)
		}
	}
}

func TestRangeSortGlobalSampleBound(t *testing.T) {
	var samples []any
	var seen uint64
	for i := 0; i < 10000; i++ {
		samples = appendSortSamples(samples, []any{i}, &seen)
	}
	if len(samples) != sortGlobalSamples || seen != 10000 {
		t.Fatalf("unbounded driver sample: kept=%d seen=%d", len(samples), seen)
	}
}

type loseBeforeSample struct {
	inner TaskRunner
	once  sync.Once
}

func (r *loseBeforeSample) Exec(task Task) (ExecResult, error) {
	if task.Sample && len(task.Upstream) > 0 {
		var err error
		r.once.Do(func() { err = os.RemoveAll(task.Upstream[0].Location) })
		if err != nil {
			return ExecResult{}, err
		}
	}
	return r.inner.Exec(task)
}

func TestRangeSortRepairsLostMapDuringSampling(t *testing.T) {
	runner := &loseBeforeSample{inner: localRunner{storeDir: t.TempDir()}}
	repairs := 0
	recs, err := ScheduleWith(JobSpec{TaskName: "multi-stage-check", Action: ActionCollect, NumPartitions: 2},
		[]TaskRunner{runner}, ScheduleOpts{MaxRecoveries: 3, OnRepair: func(FetchError) { repairs++ }})
	if err != nil {
		t.Fatal(err)
	}
	if repairs == 0 || fmt.Sprint(recs) != "[{a 4} {b 2}]" {
		t.Fatalf("lost sample input not repaired: repairs=%d records=%v", repairs, recs)
	}
}
