#!/usr/bin/env bash
set -euo pipefail

# Extra kind coverage after two-pod.sh has a healthy executor StatefulSet.
# Mounts multi-file input and runs broadcast, text, sort, logistic, and linear jobs.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NAMESPACE="${GOSPARK_NAMESPACE:-gospark-two-pod}"
IMAGE="${GOSPARK_IMAGE:-gospark/worker:latest}"
TEXT_CM=gospark-text
LOGISTIC_CM=gospark-logistic
LINEAR_CM=gospark-linear
export GOSPARK_TEXT_CONFIGMAP="$TEXT_CM"
export GOSPARK_LOGISTIC_CONFIGMAP="$LOGISTIC_CM"
export GOSPARK_LINEAR_CONFIGMAP="$LINEAR_CM"

kubectl rollout status statefulset/gospark-exec -n "$NAMESPACE" --timeout=180s

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
printf 'hello world\n' >"$tmpdir/a.txt"
printf 'hello spark\n' >"$tmpdir/b.txt"
printf 'ignore\n' >"$tmpdir/_SUCCESS"
printf '0 0\n0 1\n0 2\n' >"$tmpdir/class0.txt"
printf '1 8\n1 9\n1 12\n' >"$tmpdir/class1.txt"
printf '1 0\n3 1\n' >"$tmpdir/line-a.txt"
printf '5 2\n7 3\n' >"$tmpdir/line-b.txt"

kubectl delete configmap "$TEXT_CM" "$LOGISTIC_CM" "$LINEAR_CM" -n "$NAMESPACE" --ignore-not-found
kubectl create configmap "$TEXT_CM" -n "$NAMESPACE" \
  --from-file=a.txt="$tmpdir/a.txt" \
  --from-file=b.txt="$tmpdir/b.txt" \
  --from-file=_SUCCESS="$tmpdir/_SUCCESS"
kubectl create configmap "$LOGISTIC_CM" -n "$NAMESPACE" \
  --from-file=class0.txt="$tmpdir/class0.txt" \
  --from-file=class1.txt="$tmpdir/class1.txt"
kubectl create configmap "$LINEAR_CM" -n "$NAMESPACE" \
  --from-file=line-a.txt="$tmpdir/line-a.txt" \
  --from-file=line-b.txt="$tmpdir/line-b.txt"

echo "Remounting executors with input ConfigMaps..."
GOSPARK_NAMESPACE="$NAMESPACE" GOSPARK_IMAGE="$IMAGE" \
  GOSPARK_TEXT_CONFIGMAP="$TEXT_CM" \
  GOSPARK_LOGISTIC_CONFIGMAP="$LOGISTIC_CM" \
  GOSPARK_LINEAR_CONFIGMAP="$LINEAR_CM" \
  ./bin/gospark-worker print-k8s exec | kubectl apply -f -
kubectl rollout status statefulset/gospark-exec -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=ready pod -l app=gospark-exec -n "$NAMESPACE" --timeout=180s

run_task() {
  local task="$1"
  local input="${2:-}"
  local marker="$3"
  local timeout="${4:-300}"
  echo "--- driver Job for $task ---"
  kubectl delete job gospark-driver -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s || true
  GOSPARK_NAMESPACE="$NAMESPACE" GOSPARK_IMAGE="$IMAGE" GOSPARK_TASK="$task" GOSPARK_INPUT="$input" \
    ./bin/gospark-worker print-k8s driver | kubectl apply -f -
  if ! kubectl wait --for=condition=complete job/gospark-driver -n "$NAMESPACE" --timeout="${timeout}s"; then
    echo "driver job for $task did not complete" >&2
    kubectl logs -n "$NAMESPACE" -l job-name=gospark-driver --tail=40 || true
    exit 1
  fi
  local pod out
  pod=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-driver --field-selector=status.phase=Succeeded -o jsonpath='{.items[0].metadata.name}')
  out=$(kubectl logs -n "$NAMESPACE" "$pod")
  echo "$out"
  grep -F -q "Schedule PASSED!" <<<"$out"
  grep -F -q "$marker" <<<"$out"
}

run_task k8s-broadcast "" "BROADCAST a=3 b=5"
run_task k8s-sort "" "SORTED [1 2 3 4]"
run_task k8s-text /data/text "TEXT hello=2 world=1 spark=1"
run_task k8s-logistic /data/logistic "LOGISTIC low=0 high=1" 600
run_task k8s-linear /data/linear "LINEAR intercept=" 600

echo "=== kind feature jobs PASSED ==="
