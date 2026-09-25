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

SortByKey is lazy and executable by remote tasks. It currently shuffles records
into one bucket; each output task externally sorts the global input and emits
its contiguous ordered slice. Both Schedule and Collect concatenate partitions
in order. Runs spill under the configured byte budget and merge two at a time.
This bounds sorting memory, but still repeats sorting and I/O for each result
partition: sampled range partitioning remains future work.

Upstream installation downloads only the reducer buckets reached by the task's
partition through narrow dependencies (including non-identity mappings) and
uses bucket 0 for a single-bucket global shuffle such as SortByKey. Map
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

`ScheduleContext` cancels pending dispatch and propagates deadlines over worker
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

Persisted cache locations are not tracked across workers; lost cache partitions
are recomputed from lineage on future tasks. Independent partitions within a
stage run in bounded waves (at most one task per configured runner); the driver
accepts wave completions in partition order and handles retries and shuffle
repairs between waves. Stages still execute in dependency order. Fine-grained
narrow-partition invalidation, locality-aware scheduling, and elastic executor
discovery remain future work.

## Distributed output commits

Use `ScheduleSave(JobSpec{TaskName: "...", Action: ActionSave,
Params: map[string]string{"path": "s3://bucket/output"}}, workers)` to run a
registered compiled job. Each worker writes its result partition to a unique
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

`Collect`, `Schedule` with collect, explicit caching, and user-created slices still
materialize data by design. Use `ScheduleSave`, iterator consumption, or Count
for outputs larger than memory. Disk space is not quota-managed yet. Sort/fetch
scratch files are owned by the Context and cleaned on Stop (task contexts Stop
on both success and failure); published map outputs must remain for reducers.

The memory CI job streams a **320 MiB** dataset through external sort and a
high-cardinality/skewed ReduceByKey inside a **128 MiB** container (swap disabled,
GOMEMLIMIT=64MiB), checking every sorted record and aggregate. Local validation
passed in about 41 seconds with a sampled peak Go heap around 4 MiB. This verifies
that fixture, not an RSS bound for arbitrary Go types/callbacks.
`TestMinIOMultipartStreaming` verifies 20 MiB uploads plus cancellation, abort,
failed part uploads, and that no unfinished multipart uploads remain.
