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

SortByKey is now lazy and executable by remote tasks. This initial algorithm
shuffles all records into one bucket; each output task sorts the global input
and emits its contiguous ordered slice. Schedule collects those slices in
partition order. This is a correctness baseline, not a scalable range sort:
memory per task is proportional to the full input. Sampled range boundaries,
external merge sorting, and bounded-memory reducer execution remain open.

Upstream installation currently loads all buckets to support narrow dependencies
whose partition mapping differs from the task index (e.g. coalescing). Optimized
dependency-aware fetching remains open. Executor-loss recovery for arbitrary
multi-stage DAGs and distributed cache reuse also remain separate work.

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
are recomputed from lineage on future tasks. Execution remains sequential;
fine-grained narrow-partition invalidation and scheduling many tasks in parallel
remain future work.
