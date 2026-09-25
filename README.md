# goSpark

[![CI](https://github.com/Knorreman/goSpark/actions/workflows/ci.yml/badge.svg)](https://github.com/Knorreman/goSpark/actions/workflows/ci.yml)
[![Kubernetes tests](https://github.com/Knorreman/goSpark/actions/workflows/k8s.yml/badge.svg)](https://github.com/Knorreman/goSpark/actions/workflows/k8s.yml)

**Spark-style batch processing in Go, locally or on Kubernetes.**

Build typed RDD pipelines with Go functions, run registered jobs on executor
pods, and read/write S3-compatible storage using AWS SDK v2.

goSpark is an evolving Go implementation of RDD-style processing. Existing
Scala/Python Spark applications need to be rewritten in Go; Spark SQL,
DataFrames, streaming, and MLlib are outside the current scope.

## What you can do

- Transform typed data with `Map`, `FlatMap`, `Filter`, and `MapPartitions`.
- Aggregate, join, group, repartition, and sort keyed data.
- Execute compiled jobs through a Kubernetes driver Job and executor StatefulSet.
- Recover lost shuffle output and cancel stalled worker requests.
- Spill sorting and aggregation to disk, with configurable record/group limits.
- Save distributed results using attempt-specific files and a committed manifest.
- Use S3 or MinIO, including bounded multipart uploads.

## Contents

- [Install and build](#install-and-build)
- [Run locally](#run-locally)
- [Deploy on Kubernetes](#deploy-on-kubernetes)
- [Read and write S3](#read-and-write-s3)
- [Write your own distributed job](#write-your-own-distributed-job)
- [Configuration](#configuration)
- [Future work](#future-work)
- [Tests and troubleshooting](#tests-and-troubleshooting)

## Install and build

### Requirements

| Use case | Requirements |
|---|---|
| Local examples and library | Git and Go 1.24+ |
| Container image | Docker, or Podman |
| Kubernetes deployment | `kubectl`, access to a cluster, and an image available to its nodes |
| Local Kubernetes test environment | Docker, `kind`, `kubectl`, Bash, and Go on Linux/amd64 |

`go.mod` currently requests **Go 1.26.8** as its toolchain. With
`GOTOOLCHAIN=auto`, Go downloads it when needed. Install that toolchain yourself
if automatic downloads are disabled.

```bash
git clone https://github.com/Knorreman/goSpark.git
cd goSpark
go mod download
go build ./...

mkdir -p bin
go build -o bin/gospark-worker ./cmd/gospark-worker
./bin/gospark-worker smoke
```

Expected smoke-test summary:

```text
Smoke test PASSED: sum=55, evens=5
```

To install the worker executable into your Go binary directory, run
`go install ./cmd/gospark-worker` **from this checkout**. Make sure `GOBIN`, or
`$(go env GOPATH)/bin` when `GOBIN` is unset, is on your `PATH`.

### Use the library in another Go module

The current module path is **`goSpark`**, and the package name is **`spark`**.
Use a local replacement when consuming this checkout from another module:

```bash
# Run inside your application's Go module. Replace the path with your checkout.
go mod edit -require=goSpark@v0.0.0
go mod edit -replace=goSpark=/absolute/path/to/goSpark
```

Import it as `spark "goSpark"`, add your code, then run `go mod tidy`.
The documented installation path is a source checkout, not a published
`go install github.com/Knorreman/goSpark/...@latest` release.

## Run locally

From the repository root:

```bash
go run ./examples/wordcount
go run ./examples/joins
go run ./examples/textfile
```

The joins example demonstrates inner/left joins, cogroup, set operations,
Cartesian products, and zip. The text-file example creates its own temporary
input and output. More examples are in [`examples/`](examples/).

### A complete wordcount application

```go
package main

import (
	"fmt"
	"strings"

	spark "goSpark"
)

func main() {
	ctx := spark.NewContext(&spark.Config{
		AppName:       "WordCount",
		Master:        "local[*]",
		NumPartitions: 2,
	})
	defer ctx.Stop()

	lines := spark.Parallelize(ctx, []string{
		"hello world", "hello spark", "world spark hello",
	}, 2)
	words := spark.FlatMap(lines, strings.Fields)
	pairs := spark.Map(words, func(word string) spark.Pair[string, int] {
		return spark.NewPair(word, 1)
	})
	counts := spark.ReduceByKey(pairs, spark.NewHashPartitioner(2),
		func(a, b int) int { return a + b })

	for _, pair := range spark.Collect(counts) {
		fmt.Printf("%s: %d\n", pair.Key, pair.Value)
	}
}
```

Results are `hello: 3`, `world: 2`, and `spark: 2`; key order is unspecified.
Use `spark.TextFile(ctx, "input.txt", 2)` instead of `Parallelize` for a local
text file. `Collect` brings the whole result into the caller's memory.

### Run the bundled worker in a container

```bash
docker build -t gospark/worker:local .
docker run --rm gospark/worker:local smoke
docker run --rm gospark/worker:local wordcount
```

These commands run local computations inside one container. Distributed
execution uses `serve` executors and a `schedule` driver, as shown next.

## Deploy on Kubernetes

The supported distributed path is:

```text
driver Job (schedule)
    ├── executor 0 (serve) ──┐
    └── executor 1 (serve) ──┴── HTTP shuffle between executors
```

Driver and executors must use the **same application image** containing the
registered job. Setting `Master: "k8s://..."` alone does not submit local RDD
actions to Kubernetes; use the scheduler/driver path below.

### Quick start: local kind cluster

On Linux/amd64 with Docker, kind, and kubectl installed:

```bash
./k8s/two-pod.sh
```

This creates or reuses the kind cluster **`kind-cluster`**, builds and loads the
worker image, starts two executors, and submits wordcount and join driver Jobs.
It **deletes and recreates** the test namespace `gospark-two-pod` and changes
your kubectl context to the kind cluster. Use this for a disposable test setup.

Successful runs end with:

```text
=== two-pod distributed test PASSED ===
```

Docker is the engine used by GitHub CI. `GOSPARK_ENGINE=podman` selects the
experimental Podman path; this has shown pod-networking problems on the local
development host.

### Deploy manually to kind or an existing cluster

Build the local CLI first (`go build -o bin/gospark-worker ./cmd/gospark-worker`).
The following uses a dedicated namespace and a versioned image tag:

```bash
export GOSPARK_NAMESPACE=gospark-demo
export GOSPARK_IMAGE=gospark/worker:demo-v1

docker build -t "$GOSPARK_IMAGE" .

# For kind (create the cluster first if needed):
# kind create cluster --name kind-cluster --wait 180s
kind load docker-image "$GOSPARK_IMAGE" --name kind-cluster

kubectl create namespace "$GOSPARK_NAMESPACE"
./bin/gospark-worker print-k8s exec | kubectl apply -f -
kubectl rollout status statefulset/gospark-exec \
  -n "$GOSPARK_NAMESPACE" --timeout=180s

GOSPARK_TASK=k8s-wc ./bin/gospark-worker print-k8s driver | kubectl apply -f -
kubectl wait --for=condition=complete job/gospark-driver \
  -n "$GOSPARK_NAMESPACE" --timeout=600s

POD=$(kubectl get pods -n "$GOSPARK_NAMESPACE" -l job-name=gospark-driver \
  --field-selector=status.phase=Succeeded -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n "$GOSPARK_NAMESPACE" "$POD"
```

For an existing remote cluster, set `GOSPARK_IMAGE` to an image in your registry,
build and **push** it, and omit `kind load`. Configure image-pull credentials
in the manifests if your registry requires them. Driver-to-executor and
executor-to-executor connectivity on TCP **8080**, plus cluster DNS, is required.

The generated manifests currently use **two executors and two partitions**, a
headless Service called `gospark-exec`, and a Job called `gospark-driver`.
They are starter manifests: export/edit them for resource requests, limits,
volumes, service accounts, or a different replica/partition count. Use the Go
manifest helpers when generating other sizes. Worker shuffle files and task
scratch data use container-local temporary storage by default.

To submit the built-in join job after wordcount:

```bash
kubectl delete job gospark-driver -n "$GOSPARK_NAMESPACE" --wait=true
GOSPARK_TASK=k8s-join ./bin/gospark-worker print-k8s driver | kubectl apply -f -
```

Use the same wait/log commands above. Expected join matches are
`1 → (one, 10)`, `1 → (one, 11)`, and `2 → (two, 20)`.

Clean up your demonstration deployment when finished:

```bash
kubectl delete namespace "$GOSPARK_NAMESPACE"
# Only if you also want to remove the local test cluster:
# kind delete cluster --name kind-cluster
```

## Read and write S3

Both `s3://` and `s3a://` URIs use AWS SDK v2. Buckets must already exist.

### Credentials and endpoints

Set these in the processes performing storage I/O:

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID='your-access-key'
export AWS_SECRET_ACCESS_KEY='your-secret-key'
# export AWS_SESSION_TOKEN='your-session-token' # temporary credentials

# For MinIO or another S3-compatible endpoint:
# export AWS_ENDPOINT_URL=http://minio:9000
# export AWS_S3_PATH_STYLE=true
```

Alternatively enable the SDK credential chain with
`GOSPARK_S3_DEFAULT_CHAIN=true`, or set `S3Config{UseDefaultChain: true}` in Go
(shared profiles, workload identity, or other SDK-supported providers).
For the environment-based switch, `AWS_EC2_METADATA_DISABLED=true` currently
disables goSpark's default-chain opt-in; use the Go setting when you need both.

`spark.TextFile(ctx, "s3://bucket/input.txt", 2)` reads a single object through
the filesystem abstraction. Use it inside your registered job to process S3
input; the bundled `k8s-wc` job uses a small built-in dataset.

### Save distributed results to S3

With the executors deployed, create a Secret and give it to **both** workers
and driver. The example expects the credential variables above to be set:

```bash
kubectl create secret generic gospark-s3 -n "$GOSPARK_NAMESPACE" \
  --from-literal=AWS_REGION="$AWS_REGION" \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

kubectl set env statefulset/gospark-exec -n "$GOSPARK_NAMESPACE" --from=secret/gospark-s3
kubectl rollout status statefulset/gospark-exec -n "$GOSPARK_NAMESPACE" --timeout=180s

kubectl delete job gospark-driver -n "$GOSPARK_NAMESPACE" --ignore-not-found --wait=true
GOSPARK_TASK=k8s-wc \
  GOSPARK_OUTPUT=s3://your-bucket/runs/wordcount-001 \
  GOSPARK_S3_SECRET=gospark-s3 \
  ./bin/gospark-worker print-k8s driver | kubectl apply -f -
```

Include `AWS_SESSION_TOKEN`, `AWS_ENDPOINT_URL`, and `AWS_S3_PATH_STYLE` in
the Secret when needed. Then wait for the driver Job as above. Successful
save logs contain `OUTPUT_COMMITTED` and `Schedule PASSED!`.

Workers upload attempt-specific files. Only the files referenced by the
driver's JSON **`_SUCCESS` manifest** form the committed output. Read that
manifest with `spark.ReadCommittedOutput(uri)`; do not discover data by listing
`_temporary`. Use a fresh output prefix for each new run—an existing commit
cannot be overwritten by a different job.

For small results, the bundled CLI can verify and print the committed data:

```bash
kubectl exec -n "$GOSPARK_NAMESPACE" gospark-exec-0 -- \
  /gospark-worker verify-output s3://your-bucket/runs/wordcount-001
```

`verify-output` reads each selected partition into memory, so it is a diagnostic
command for modest outputs. Applications can stream each manifest key via
`FileSystem.Open` instead.

For a disposable MinIO demonstration after `two-pod.sh`, run
`./k8s/minio-output.sh`. It creates test storage/credentials and deliberately
injects a lost save response to verify retry-safe publication.

## Write your own distributed job

Go closures are not serialized and shipped to workers. A **registered factory**
reconstructs the pipeline in each process from compiled code and `JobSpec`.
Build it into the same image used by the driver and every executor.

For a first custom job, add another registration inside `init()` in
[`cmd/gospark-worker/main.go`](cmd/gospark-worker/main.go):

```go
spark.RegisterJob("my-doubles", func(ctx *spark.Context, spec spark.JobSpec) (spark.RDDAny, error) {
	data := spark.Parallelize(ctx, []int{1, 2, 3, 4}, spec.NumPartitions)
	return spark.Map(data, func(n int) int { return n * 2 }), nil
})
```

Rebuild the CLI and image, load/push the new image, update the executor
deployment, then submit with `GOSPARK_TASK=my-doubles`. Use a new image tag so
`IfNotPresent` does not leave old worker code running. The bundled Dockerfile
copies `main.go` explicitly; extend its `COPY` instructions if you split your
application into more files/packages.

In your own Go driver, submit parameterized jobs with the scheduler API:

```go
workers := []spark.TaskRunner{
	&spark.WorkerClient{BaseURL: "http://worker-a:8080"},
	&spark.WorkerClient{BaseURL: "http://worker-b:8080"},
}
records, err := spark.Schedule(spark.JobSpec{
	TaskName:      "my-doubles",
	Action:        spark.ActionCollect,
	NumPartitions: 2,
}, workers)
// Handle err; records contains []any values returned by the job.
_ = records
_ = err
```

Use `JobSpec.Params` for explicit inputs/parameters and `ScheduleSave` for
distributed output. `ScheduleContext` and `ScheduleSaveContext` accept a
`context.Context` and retry options. Factories must build deterministic graphs
on driver and workers; do not perform actions such as `Collect` while building
them. Use `ctx.TaskContext()` for cancellable I/O in callbacks.

The supplied `schedule` CLI accepts the built-in collect/save modes; passing
arbitrary `JobSpec.Params` requires your own Go driver.

## Configuration

### Worker and driver environment

| Variable | Used by | Default / purpose |
|---|---|---|
| `GOSPARK_LISTEN` | `serve` | `127.0.0.1:0`; generated pods use `0.0.0.0:8080` |
| `GOSPARK_ADVERTISE_URL` | `serve` | Reachable shuffle URL; generated pods use their StatefulSet DNS name |
| `GOSPARK_STORE` | `serve` | OS temp directory + `/gospark-shuffle` |
| `GOSPARK_SHUFFLE_MAP_MAX_BYTES` | shuffle-map executors | Optional positive byte cap per map attempt; unset means no cap |
| `GOSPARK_TASK` | `schedule`, manifest generation | Registered job name; default `k8s-wc` |
| `GOSPARK_WORKERS` | `schedule` | Required comma-separated executor HTTP URLs |
| `GOSPARK_PARTITIONS` | `schedule` | `2`; `print-k8s` currently generates a fixed value of `2` |
| `GOSPARK_WAIT_WORKERS` | `schedule` | Readiness timeout in seconds; default `120` |
| `GOSPARK_ACTION` | `schedule` | `collect` or `save` |
| `GOSPARK_OUTPUT` | save / manifest generation | Output URI; also selects save when generating the driver manifest |
| `GOSPARK_NAMESPACE` | manifest generation | `gospark-two-pod` |
| `GOSPARK_IMAGE` | manifest generation | `gospark/worker:latest` |
| `GOSPARK_S3_SECRET` | save manifest generation | Kubernetes Secret injected into the driver; configure workers separately |

### Memory and retry settings

Set these on `Config`, or on `ctx.Config()` in your job factory:

| Setting | Default |
|---|---|
| `ShuffleMemoryBytes` | 16 MiB encoded sort/combining budget |
| `MaxRecordBytes` | 4 MiB encoded record / text-line limit |
| `MaxGroupBytes` | `ShuffleMemoryBytes` when not set |

These are encoded-data budgets, not total RSS limits. Allow runtime and
callback headroom. `GroupByKey` materializes one `[]V` group and rejects
oversized groups. `Collect`, explicit caching, and user-created slices still
materialize data. `SortByKey` uses external sorting but repeats global sorting
per output partition; range-partitioned sorting is still future work.

`ScheduleOpts` defaults to three task attempts and two lost-shuffle repairs.
`WorkerClient.Timeout` defaults to two minutes; `HeartbeatInterval` defaults
to one second. The driver schedules independent partitions in parallel within
a stage, up to one in-flight task per configured worker; stages run in
dependency order.

For detailed execution, recovery, output, and memory semantics, see
[`DISTRIBUTED.md`](DISTRIBUTED.md).

## Future work

For a more complete **distributed Spark RDD replacement**, the highest-impact
gaps are:

1. **Locality-aware and elastic scheduling:** Place tasks near cached data and
   shuffle blocks, improve work distribution across concurrent jobs, and
   support executor discovery and elastic scaling. Today the driver runs
   partitions concurrently in bounded waves against a fixed worker list.
2. **Durable driver recovery:** Persist job plans, accepted attempts, and
   shuffle/output metadata so a replacement driver can resume after a crash.
   Current recovery handles executor/shuffle loss while the driver remains alive.
3. **Distributed cache and shuffle lifecycle:** Track cached partition locations,
   reuse them across tasks, recompute only missing partitions, and manage shuffle
   retention, worker-wide disk quotas, orphan cleanup after driver crashes,
   and backpressure. Tasks fetch only needed shuffle buckets and successful
   drivers request job-scoped cleanup on completion or failure, but cached data
   and shuffle files still live on individual executors' local storage.
4. **Scalable partitioned I/O and sorting:** Split large input files across
   workers, support partitioned datasets and common storage formats, and use
   range partitioning for `SortByKey`. The current text reader handles a single
   object, while sorting repeats global work for each output partition.
5. **RDD API and execution compatibility:** Fill gaps in transformations,
   actions, partitioner semantics, broadcast variables, and accumulators;
   define serialization and task-side-effect guarantees clearly. Cross-language
   or binary compatibility with Apache Spark is a separate undertaking.
6. **Production observability and scale qualification:** Expose per-stage/task
   metrics, traces and a job UI; test worker churn, skew, large shuffles, and
   concurrent jobs under sustained load.

Additional directions:

- **Kubernetes custom resource (CRD) and controller:** Define jobs declaratively,
  with an operator to create and monitor driver Jobs and executor pods, manage
  retries and cleanup, and report job status through Kubernetes. Today you
  deploy the generated manifests and submit Jobs yourself.
- **MLlib-style machine learning:** Add distributed algorithms and reusable
  feature-processing pipelines on top of the RDD engine. There is currently no
  MLlib API or model-training framework.

## Tests and troubleshooting

```bash
go test ./...
go test -race ./...
go vet ./...

# Disposable kind integration environment:
./k8s/two-pod.sh
./k8s/recovery.sh       # deliberately deletes an executor
./k8s/minio-output.sh  # MinIO commits and injected save retries
```

CI covers local and cross-process execution, real MinIO uploads/commits,
Kubernetes driver Jobs and executor-loss recovery, plus a **320 MiB** sorting
and aggregation fixture in a **128 MiB** container. These tests establish the
tested workloads, not complete Apache Spark compatibility.

| Symptom | Check |
|---|---|
| `task/job not registered` | Driver and executors must contain the same registration and application build. |
| `ImagePullBackOff` | Verify registry access, image tag, and kind image loading. |
| Worker readiness timeout | Check DNS and driver → executor TCP 8080 connectivity; inspect pod logs. |
| Shuffle fetch failures | Check `GOSPARK_ADVERTISE_URL`, executor → executor connectivity, and available local disk. |
| `output already committed` | Choose a new output prefix; reruns cannot overwrite another commit. |
| Group/record limit error | Check skew/record size and configure appropriate budgets or change the pipeline. |

For failed Jobs, inspect all attempts with
`kubectl logs -n "$GOSPARK_NAMESPACE" -l job-name=gospark-driver --tail=100`.
