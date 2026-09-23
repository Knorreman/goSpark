#!/usr/bin/env bash
set -euo pipefail

# Requires the two healthy executors created by two-pod.sh. The driver
# publishes map 1 on exec-1, pauses, and the test deletes that pod. Recovery
# must read from a replacement and recompute only the lost map output.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NAMESPACE="${GOSPARK_NAMESPACE:-gospark-two-pod}"
IMAGE="${GOSPARK_IMAGE:-gospark/worker:latest}"

kubectl rollout status statefulset/gospark-exec -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=Ready pod/gospark-exec-1 -n "$NAMESPACE" --timeout=180s
kubectl delete job gospark-driver -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=60s
GOSPARK_NAMESPACE="$NAMESPACE" GOSPARK_IMAGE="$IMAGE" GOSPARK_TASK=k8s-wc \
  GOSPARK_TEST_PAUSE_AFTER_MAP=60 ./bin/gospark-worker print-k8s driver | kubectl apply -f -

echo "Waiting for published map output before executor loss..."
driver=""
published=0
for _ in $(seq 1 120); do
  driver=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-driver \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "$driver" ] && kubectl logs -n "$NAMESPACE" "$driver" 2>/dev/null | grep -q '^MAP_PUBLISHED '; then
    published=1
    break
  fi
  sleep 1
done
if [ "$published" -ne 1 ]; then
  echo "No shuffle output was published" >&2
  exit 1
fi
old_uid=$(kubectl get pod gospark-exec-1 -n "$NAMESPACE" -o jsonpath='{.metadata.uid}')
echo "Removing executor gospark-exec-1 (UID $old_uid) after map publication..."
kubectl delete pod gospark-exec-1 -n "$NAMESPACE" --grace-period=0 --force --wait=false
for _ in $(seq 1 120); do
  new_uid=$(kubectl get pod gospark-exec-1 -n "$NAMESPACE" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [ -n "$new_uid" ] && [ "$new_uid" != "$old_uid" ]; then break; fi
  sleep 1
done
if [ -z "${new_uid:-}" ] || [ "$new_uid" = "$old_uid" ]; then
  echo "Executor pod was not replaced" >&2
  exit 1
fi
kubectl rollout status statefulset/gospark-exec -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=Ready pod/gospark-exec-1 -n "$NAMESPACE" --timeout=180s
kubectl wait --for=condition=complete job/gospark-driver -n "$NAMESPACE" --timeout=300s
successful=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-driver \
  --field-selector=status.phase=Succeeded -o jsonpath='{.items[0].metadata.name}')
if [ "$successful" != "$driver" ]; then
  echo "Driver retried instead of repairing its accepted map output" >&2
  exit 1
fi
output=$(kubectl logs -n "$NAMESPACE" "$driver")
echo "$output"
for marker in 'MAP_PUBLISHED ' 'SHUFFLE_LOST ' 'MAP_REBUILT ' 'Key:"hello", Value:3' \
  'Key:"world", Value:2' 'Key:"spark", Value:2' 'RESULT_COUNT=3' 'Schedule PASSED!'; do
  if ! grep -Fq "$marker" <<<"$output"; then
    echo "Missing required recovery result: $marker" >&2
    exit 1
  fi
done
echo "=== Kubernetes executor-loss recovery PASSED ==="
