package spark

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestResultFetchesOnlyRequiredBuckets(t *testing.T) {
	for _, job := range []string{"sched-wc", "sched-join", "multi-stage-check"} {
		t.Run(job, func(t *testing.T) {
			spec := JobSpec{TaskName: job, Action: ActionCollect, NumPartitions: 2}
			plan, err := PlanJob(spec)
			if err != nil {
				t.Fatal(err)
			}
			// Materialize each stage on disk with the normal scheduler, then
			// replay the result tasks against an instrumented shuffle server.
			var manifests []MapOutputManifest
			_, err = ScheduleWith(spec, []TaskRunner{localRunner{storeDir: t.TempDir()}}, ScheduleOpts{
				OnTaskComplete: func(_ Task, r ExecResult) error {
					if r.Manifest != nil {
						manifests = append(manifests, *r.Manifest)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var requested []string
			var activePartition atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/shuffle/"), "/")
				if len(parts) != 5 {
					http.Error(w, "bad path", http.StatusBadRequest)
					return
				}
				shuffle, _ := strconv.Atoi(parts[1])
				mapID, _ := strconv.Atoi(parts[2])
				attempt, _ := strconv.Atoi(parts[3])
				reducer, _ := strconv.Atoi(parts[4])
				mu.Lock()
				requested = append(requested, fmt.Sprintf("%d/r%s", shuffle, parts[4]))
				mu.Unlock()
				wanted := int(activePartition.Load())
				if job == "multi-stage-check" {
					wanted = 0
				}
				if reducer != wanted {
					http.Error(w, "unrelated bucket", http.StatusNotFound)
					return
				}
				for _, m := range manifests {
					if m.ShuffleID == shuffle && m.MapID == mapID && m.Attempt == attempt {
						http.ServeFile(w, r, filepath.Join(m.Location, "r"+parts[4]))
						return
					}
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			for i := range manifests {
				manifests[i].BaseURL = server.URL
			}
			result, _ := stageByID(plan, plan.ResultStageID)
			var upstream []MapOutputManifest
			for _, id := range result.Parents {
				parent, _ := stageByID(plan, id)
				for _, m := range manifests {
					if m.ShuffleID == parent.ShuffleID {
						upstream = append(upstream, m)
					}
				}
			}
			for part := 0; part < result.NumPartitions; part++ {
				activePartition.Store(int64(part))
				mu.Lock()
				requested = nil
				mu.Unlock()
				_, err := ExecuteTask(Task{JobID: upstream[0].JobID, Job: spec, StageID: result.ID, PartitionID: part, StoreDir: t.TempDir(), Upstream: upstream, Fingerprint: plan.Fingerprint})
				if err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				got := append([]string(nil), requested...)
				mu.Unlock()
				if len(got) == 0 {
					t.Fatal("no shuffle buckets fetched")
				}
				for _, req := range got {
					// SortByKey has a single global reducer bucket, even for
					// result partitions with other indices.
					want := part
					if job == "multi-stage-check" {
						want = 0
					}
					if !strings.HasSuffix(req, fmt.Sprintf("/r%d", want)) {
						t.Fatalf("partition %d fetched unrelated bucket %s (all: %v)", part, req, got)
					}
				}
			}
		})
	}
}

func TestRequiredShuffleBucketsFollowsNarrowMapping(t *testing.T) {
	ctx := NewContext(&Config{AppName: "mapping", NumPartitions: 3})
	defer ctx.Stop()
	input := Parallelize(ctx, []Pair[int, int]{NewPair(0, 1), NewPair(1, 2), NewPair(2, 3)}, 3)
	shuffled := ReduceByKey(input, NewHashPartitioner(3), func(a, b int) int { return a + b })
	reversed := NewRDD[Pair[int, int]](ctx,
		func() []Partition { return NewPartitions(3) },
		func() []Dependency {
			return []Dependency{NewNarrowDep(shuffled, func(pid int) []int { return []int{2 - pid} })}
		},
		func(p Partition) Iterator[Pair[int, int]] {
			return shuffled.Compute(NewPartition(2 - p.Index()))
		})
	dep := shuffled.Dependencies()[0].(*ShuffleDep)
	for p := 0; p < 3; p++ {
		want := requiredShuffleBuckets(reversed, p)[dep.ShuffleID()]
		if len(want) != 1 || !want[2-p] {
			t.Fatalf("partition %d needs bucket %d, got %v", p, 2-p, want)
		}
	}
}
