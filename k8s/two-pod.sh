#!/bin/bash
set -euo pipefail

# Distributed smoke test on Kubernetes:
#   executors (StatefulSet) + driver (Job) submit -> run -> commit output.
# The driver Job is the primary path (real submission, no exec workaround).
# Exits non-zero unless every driver log contains "Schedule PASSED!".

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

# Ensure a kind cluster exists (create only if missing).
if ! KIND_EXPERIMENTAL_PROVIDER="$ENGINE" kind get clusters 2>/dev/null | grep -q "^kind-cluster$"; then
  echo "Creating kind cluster..."
  KIND_EXPERIMENTAL_PROVIDER="$ENGINE" kind create cluster --name kind-cluster --image kindest/node:v1.31.0
fi

kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true --timeout=120s || true
kubectl create namespace "$NAMESPACE"

echo "Building and loading image..."
if [ "$ENGINE" = "podman" ]; then
  podman build -q -t "$IMAGE" -f Dockerfile .
  KIND_EXPERIMENTAL_PROVIDER=podman kind load docker-image "$IMAGE" --name kind-cluster
  # podman loads the image under a localhost/... tag; re-tag inside
  # containerd so the cluster resolves it as docker.io/<image>.
  NODE=$(podman ps --format '{{.Names}}' | grep kind-cluster-control-plane | head -1)
  podman exec --privileged "$NODE" ctr --namespace=k8s.io images rm "docker.io/$IMAGE" >/dev/null 2>&1 || true
  podman exec --privileged "$NODE" ctr --namespace=k8s.io images tag "localhost/$IMAGE" "docker.io/$IMAGE" >/dev/null 2>&1 || true
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
run_task() {
  local task="$1"
  echo "--- driver Job for $task (full submission pipeline) ---"
  kubectl delete job gospark-driver -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s || true
  GOSPARK_NAMESPACE="$NAMESPACE" GOSPARK_IMAGE="$IMAGE" GOSPARK_TASK="$task" \
    ./bin/gospark-worker print-k8s driver | kubectl apply -f -

  if ! kubectl wait --for=condition=complete job/gospark-driver -n "$NAMESPACE" --timeout=300s; then
    echo "driver job for $task did not complete"
    kubectl logs -n "$NAMESPACE" -l job-name=gospark-driver --tail=20 || true
    FAIL=1
    return
  fi
  local out
  out=$(kubectl logs -n "$NAMESPACE" job/gospark-driver)
  echo "$out"
  if ! grep -q "Schedule PASSED!" <<<"$out"; then
    echo "driver for $task: missing 'Schedule PASSED!' marker"
    FAIL=1
  fi
}

run_task k8s-wc
run_task k8s-join

if [ "$FAIL" -ne 0 ]; then
  echo "=== two-pod distributed test FAILED ==="
  exit 1
fi
echo "=== two-pod distributed test PASSED ==="