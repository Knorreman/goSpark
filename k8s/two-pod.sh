#!/bin/bash
set -euo pipefail

# Two-executor distributed smoke test on a Kubernetes cluster.
# Runs wordcount (1 shuffle) and join (2 shuffles) across two pods.
# Exits non-zero unless every schedule prints "Schedule PASSED!".

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NAMESPACE="${GOSPARK_NAMESPACE:-gospark-two-pod}"
IMAGE="${GOSPARK_IMAGE:-gospark/worker:latest}"
ENGINE="${GOSPARK_ENGINE:-docker}"

cd "$PROJECT_DIR"
echo "=== goSpark two-pod distributed test ==="
echo "namespace=$NAMESPACE image=$IMAGE engine=$ENGINE"

# Offline gate: two OS processes + manifest generation must pass first.
go test -count=1 -run 'TestScheduleTwoProcessesHTTPShuffle|TestK8sExecutorManifest|TestK8sDriverManifest' .

if ! command -v kubectl >/dev/null 2>&1 || ! kubectl cluster-info >/dev/null 2>&1; then
  echo "No kubectl cluster. Offline tests passed; cluster run skipped."
  exit 0
fi

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/gospark-worker ./cmd/gospark-worker/

if [ "$ENGINE" = "podman" ]; then
  ENGINE_FLAGS="--driver podman"
  PODMAN_FLAGS=""
else
  ENGINE_FLAGS=""
  PODMAN_FLAGS=""
fi
KIND_EXPERIMENTAL_PROVIDER="$ENGINE" kind create cluster --name kind-cluster --image kindest/node:v1.31.0 2>/dev/null \
  || KIND_EXPERIMENTAL_PROVIDER="$ENGINE" kind get clusters | grep -q kind-cluster \
  || { echo "kind cluster unavailable"; exit 1; }

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true --timeout=120s || true
kubectl create namespace "$NAMESPACE"

echo "Building and loading image..."
if [ "$ENGINE" = "podman" ]; then
  podman build -q -t "$IMAGE" -f Dockerfile .
  KIND_EXPERIMENTAL_PROVIDER=podman kind load docker-image "localhost/$IMAGE" --name kind-cluster
else
  docker build -q -t "$IMAGE" -f Dockerfile .
  kind load docker-image "$IMAGE" --name kind-cluster
fi

echo "Applying executors..."
GOSPARK_NAMESPACE="$NAMESPACE" GOSPARK_IMAGE="$IMAGE" ./bin/gospark-worker print-k8s exec | kubectl apply -f -

echo "Waiting for executors..."
kubectl rollout status statefulset/gospark-exec -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=ready pod -l app=gospark-exec -n "$NAMESPACE" --timeout=180s

FAIL=0
run_schedule() {
  local task="$1"
  echo "--- schedule $task (exec-0 -> exec-0 + exec-1) ---"
  local out
  if ! out=$(kubectl exec -n "$NAMESPACE" gospark-exec-0 -- \
    env GOSPARK_TASK="$task" GOSPARK_PARTITIONS=2 \
    GOSPARK_WORKERS="http://127.0.0.1:8080,http://gospark-exec-1.gospark-exec:8080" \
    /gospark-worker schedule 2>&1); then
    echo "$out"
    echo "schedule $task: EXECUTION FAILED"
    FAIL=1
    return
  fi
  echo "$out"
  if ! grep -q "Schedule PASSED!" <<<"$out"; then
    echo "schedule $task: missing 'Schedule PASSED!' marker"
    FAIL=1
  fi
}

run_schedule k8s-wc
run_schedule k8s-join

if [ "$FAIL" -ne 0 ]; then
  echo "=== two-pod distributed test FAILED ==="
  exit 1
fi
echo "=== two-pod distributed test PASSED ==="