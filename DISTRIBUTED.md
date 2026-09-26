# Distributed RDD correctness

`go test -race ./...` includes exact-result tests using two worker OS
processes with HTTP shuffle. The coverage includes inner Join, Cogroup,
Repartition, and a ReduceByKey → Repartition → SortByKey pipeline.
Remote manifests always use HTTP, even when their paths happen to exist
on the receiving host. These tests do not substitute for Kubernetes tests.

Tasks submitted by Schedule carry the driver plan fingerprint. Workers compare
it with their reconstructed plan and check reconstruction consistency before
record execution. Missing map outputs, duplicate maps, foreign job manifests,
incomplete bucket manifests, and invalid task partitions fail explicitly.
Fingerprints describe the existing planner's stage structure and JobSpec;
they do not hash Go function bodies or guarantee arbitrary closures are equal.
Use identical application builds and deterministic factories on all workers.
`RegisterPipeline` permits one compiled factory to declare `ReduceBroadcast`
dependencies on RDDs. `RunPipeline` executes one job per reduction (one
partial per partition), merges each accepted result on the driver, and sends
its value in the next job's broadcast spec. `RunPipeline` collects the final
RDD; `RunPipelineSave` instead commits distributed partition output.
`PipelineValue.Value()` must be called during transformation execution, not
while constructing the factory; Go closures still are not serialized. Job IDs
and retries are independent for each phase, and a reduction of an empty RDD
fails. The final `Collect` still materializes results on the driver.
Named broadcasts travel in the job spec as gob bytes, up to 8 MiB combined.
Their hashes are part of the plan fingerprint. Read them in the factory; do
not treat them as shuffled data.

Create `NewInt64Accumulator(ctx, "items")` or
`NewFloat64Accumulator(ctx, "weight")` on the driver, then call
`BindAccumulators(&spec, ctx)` before `Schedule`. Registered job factories
recreate handles by the same names on workers; task callbacks call `Add`, and
only driver handles can call `Value`. Local `Collect` updates handles directly.
Distributed contributions are returned with task results and merged from the
final accepted attempts, so lost responses, retries, and shuffle repairs do
not double-count. Sampling callbacks may be replayed and are not counted.

The driver discovers text and S3 input splits once while planning and ships
the descriptors (path, offset, length, and partition index) in the job spec.
Workers install them before reconstructing the factory, so TextFile returns
the same partitions and the fingerprint matches even when a worker cannot
list the path. Workers still open those files at execution time; a missing
file fails the task instead of listing again. Collect and PlanJob without
shipped splits keep the current listing behavior.

SortByKey is lazy and executable by remote tasks. For multiple output
partitions, workers first sample up to 64 keys per input partition (up to 4 KiB
encoded per key); the driver caps the combined sample at 4,096 keys and
chooses range boundaries using the user comparator. Map tasks then reread
their input, shuffle into range buckets, and result tasks externally sort
only their own bucket. Both Schedule and Collect concatenate partitions in
order. Runs spill under the configured byte budget and merge two at a time.
Equal keys always go to the same side of a boundary; highly skewed keys can
still create large, uneven buckets. Input is read twice for sampling and map
publication, so callbacks and input sources must tolerate replay. A single
output partition skips sampling and sorts one bucket.

Upstream installation downloads only the reducer buckets reached by the task's
partition through narrow dependencies (including non-identity mappings) and
uses bucket 0 for a single-bucket shuffle. Map
manifests are still validated in full. Fetches use bounded buffers, validate
frame/manifest checksums, and read records incrementally. Unmapped shuffles
fall back to fetching all buckets; distributed cache reuse remains open.

## Recovery and cancellation

The driver assigns one random job ID per submission and only accepts results
matching its stage, partition and attempt. A worker returns a typed `FetchError`
when an accepted shuffle block disappears. The scheduler invalidates that map
partition and all affected downstream stages, preserving unrelated branches and
healthy map partitions. Recomputed maps use new attempt IDs to avoid reusing
paths. Recovery is bounded by `ScheduleOpts.MaxRecoveries` (default two). Once
repaired, all result partitions are checked again before returning records.
This is at-least-once task execution; external callbacks must be idempotent.

`RunPipelineContext` cancels pending dispatch and propagates deadlines over worker
HTTP RPCs. The driver probes worker health while a task runs; heartbeat failure
cancels the request. Workers receive the HTTP request context through
`Context.TaskContext()`, check cancellation between iterator records and shuffle
codec operations, and avoid publishing map outputs canceled before completion.
Callbacks that block inside a single record must use `TaskContext()` to cancel
their own I/O. The default task RPC timeout is two minutes.

Tests kill a real worker process after publication, delete selected shuffle
outputs, verify healthy maps are retained, and test worker-side cancellation.
`k8s/recovery.sh` runs in CI after `two-pod.sh`: it waits for map publication,
deletes executor 1, then checks repair and exact word counts from the same
successful driver Job. Manual kind/podman testing verified actual executor
replacement, selective map reconstruction (attempt 1), and exact word counts
with the schedule process running on executor 0. The separate driver Job path
still requires the Docker-based CI run: newly created Job pods on the local
podman cluster cannot reliably reach the pod network.

For registered jobs, workers retain memory-cached RDD partitions whose lineage
contains no shuffle across tasks of the same job. To retain them across Schedule
calls, opt in on the driver:

```go
cache := NewScheduleCache(workers)
spec, err := cache.Persist("input-v1", JobSpec{TaskName: "my-job", Action: ActionCollect})
// Handle err, then call RunPipeline(spec, workers) repeatedly.
// When finished: cache.Unpersist(ctx, "input-v1") or cache.Stop(ctx).
```

Persist marks a registered job output (when it has narrow-only lineage) and
any explicitly `Cache`d narrow-lineage inputs in that job graph. Reuse requires
the same cache identity and plan fingerprint; change the identity when input
data or callback behavior changes. Fingerprints do not hash input contents or
Go function bodies. This is a **same worker process, pre-shuffle memory cache
only**: it is not a distributed durable store, and shuffle-dependent and disk
caches are not reusable. Worker loss or replacement recomputes its partitions.
Job cleanup still removes shuffle data and job-scoped cache, but reusable data
remains until explicit Unpersist or Stop (or the worker exits).

Successful task responses report touched and evicted (RDD ID, partition)
locations; the driver prefers a
healthy worker holding a partition needed through narrow dependencies. Lost
workers and replacements at the same DNS address are detected using a worker
incarnation ID; their old locations are discarded, and missing cached
partitions are recomputed from lineage. Job-scoped cache data is released by
job cleanup.
Disk-persisted caches remain task-local. Shuffle-dependent caches remain
task-local until their versions can be invalidated when a shuffle map is
repaired. Job IDs scope default caches; reusable caches use identity and plan
fingerprint. Identical application builds and deterministic factories remain required.

Independent partitions within a stage run in bounded waves (at most one task
per configured runner); the driver accepts wave completions in partition order
and handles retries and shuffle repairs between waves. Stages still execute in
dependency order. Fine-grained narrow-partition invalidation and elastic
executor discovery remain future work.

## Distributed output commits

Use `RunPipelineSave(JobSpec{TaskName: "..."}, workers, "s3://bucket/output")`
to run a registered pipeline. Each worker writes its result partition to a unique
`_temporary/<job-id>/stage-<id>/part-<index>-attempt-<number>` path and closes
the writer before reporting a digest and byte count. The driver checks every
selected attempt, then conditionally creates `_SUCCESS` as a JSON manifest.
Local filesystems publish it via a same-directory hard link; S3 uses
`PutObject` with `If-None-Match: *`. An identical commit is idempotent; a
different job must use a new output path. Failed runs cannot publish a
partially successful manifest.

`ReadCommittedOutput(path)` lists the accepted partition keys in index order.
Readers must follow that manifest; listing `_temporary` includes abandoned
and superseded attempts. `SaveAsTextFile` is an older local API with different
publication behavior. Object-store credentials/endpoint come from `AWS_*`
environment variables on **both** workers and driver (typically Kubernetes
Secrets); do not embed credentials in `JobSpec`.

`TestMinIODistributedSave` runs against a real MinIO endpoint when
`GOSPARK_TEST_S3_ENDPOINT` is set; it tests cross-process writes, a lost
worker response, competing conditional commits, corruption, and reruns.
The K8s workflow provisions MinIO and runs `k8s/minio-output.sh` after the
executor-recovery test. Local kind/podman validation confirmed pod-to-pod
shuffle, injected save retry, and exact MinIO results with the scheduler on
executor 0. The separate driver Job path requires Docker-based CI because
newly created Job pods cannot reliably reach the local podman CNI.

S3 partition output uses sequential multipart uploads with an 8 MiB buffer and
backpressure. Close completes the upload; cancellation, write failure, or Abort
aborts unfinished multipart uploads. Small/empty objects use PutObject. Conditional
_SUCCESS writes still use a single PutObject. The current part size limits a
single upload to 10,000 parts (about 78 GiB); larger objects fail explicitly.
Temporary orphan cleanup and per-bucket IAM restrictions remain future work.

## Memory budgets and limits

Configure these on `Config`, or on `ctx.Config()` inside a registered job factory
so every worker reconstructs the same settings:

```go
ctx.Config().ShuffleMemoryBytes = 1 << 20 // encoded sort/combining run budget
ctx.Config().MaxRecordBytes = 4 << 20     // largest encoded record / TextFile line
ctx.Config().MaxGroupBytes = 1 << 20      // materialized group/combiner limit
```

Defaults are 16 MiB for sort/combining runs and groups, and 4 MiB per record.
The old global `SpillRecordLimit` has been replaced by per-context byte budgets.
Budgets measure encoded data plus per-record bookkeeping, **not total heap/RSS**:
allow headroom for decoded Go objects, the runtime, codec buffers, and callbacks.
A single record must fit the run budget; input records and groups that exceed
limits fail explicitly. Callback allocations are outside these budgets.

Local and remote shuffle maps write framed records straight to disk with at most
16 open, 32 KiB buffered files. ReduceByKey and CombineByKey externally order
records by key, then aggregate one key at a time. GroupByKey enforces an encoded
per-group size limit because its `[]V` API cannot represent an arbitrarily large
group without materializing it. Cogroup streams groups; joins emit cross-products
lazily. ReduceByKey map-side combining flushes bounded batches. Grouping supports
value keys (strings, numbers, booleans, structs, arrays); process-local pointer/
channel keys and NaNs are rejected.

`Collect`, `RunPipeline`, explicit caching, and user-created slices still
materialize data by design. Use `RunPipelineSave`, iterator consumption, or Count
for outputs larger than memory. Sort/fetch scratch files are owned by the
Context and cleaned on Stop (task contexts Stop on both success and failure);
published map outputs must remain for reducers.

After a distributed job finishes or fails, the driver asks each configured
worker to delete that job's published shuffle files. A worker refuses cleanup
while it still has active tasks; the driver retries briefly with an independent
context so cancellation does not prevent cleanup. Cleanup is best effort:
unreachable workers, crashed drivers, and custom runners without cleanup support
may leave orphaned job directories. Never remove a live job's shuffle files to
reclaim disk; lost blocks trigger recovery but can exhaust its retry budget.

Set `GOSPARK_SHUFFLE_MAP_MAX_BYTES` (positive bytes) on every executor to fail
a single shuffle map attempt before its encoded output exceeds that size,
including bucket headers. Failed attempts remove their unpublished temporary
files. Set `GOSPARK_SHUFFLE_DISK_BYTES` (positive bytes) on each `serve` worker
to bound the **combined** bytes reserved for published map outputs, unfinished
map attempts, fetched buckets, and external-sort runs across concurrent jobs.
Writes fail before crossing the configured budget; task scratch lives under
`GOSPARK_STORE` and releases its reservations on task completion or failure.
Job cleanup releases published output reservations; worker startup counts
files left in `GOSPARK_STORE` by previous runs rather than treating the store
as empty. Keep headroom for other files, the filesystem, and non-shuffle cache
data; this is a shuffle budget, not a filesystem or pod-wide hard quota.

The memory CI job streams a **320 MiB** dataset through external sort and a
high-cardinality/skewed ReduceByKey inside a **128 MiB** container (swap disabled,
GOMEMLIMIT=64MiB), checking every sorted record and aggregate. Local validation
passed in about 41 seconds with a sampled peak Go heap around 4 MiB. This verifies
that fixture, not an RSS bound for arbitrary Go types/callbacks.
`TestMinIOMultipartStreaming` verifies 20 MiB uploads plus cancellation, abort,
failed part uploads, and that no unfinished multipart uploads remain.
