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
