#!/usr/bin/env bash
set -euo pipefail

# Requires two-pod.sh to have created a healthy kind cluster and executors.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
NS="${GOSPARK_NAMESPACE:-gospark-two-pod}"
ENGINE="${GOSPARK_ENGINE:-docker}"
IMAGE="localhost/gospark-minio:test"
OUTPUT="s3://gospark-output/ci-output"

if [ "$ENGINE" = podman ]; then
  podman build -f ci/Minio.Dockerfile -t "$IMAGE" .
  KIND_EXPERIMENTAL_PROVIDER=podman kind load docker-image "$IMAGE" --name kind-cluster
else
  docker build -f ci/Minio.Dockerfile -t "$IMAGE" .
  kind load docker-image "$IMAGE" --name kind-cluster
fi

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: $NS
spec:
  selector:
    app: gospark-minio
  ports:
  - port: 9000
    targetPort: 9000
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gospark-minio
  template:
    metadata:
      labels:
        app: gospark-minio
    spec:
      containers:
      - name: minio
        image: $IMAGE
        imagePullPolicy: IfNotPresent
        args: ["server", "/data", "--address", ":9000"]
        env:
        - name: MINIO_ROOT_USER
          value: gosparktest
        - name: MINIO_ROOT_PASSWORD
          value: gosparktest-secret
        ports:
        - containerPort: 9000
        readinessProbe:
          httpGet:
            path: /minio/health/live
            port: 9000
          periodSeconds: 2
EOF
kubectl rollout status deployment/minio -n "$NS" --timeout=180s

kubectl create secret generic gospark-s3-test -n "$NS" \
  --from-literal=AWS_ACCESS_KEY_ID=gosparktest \
  --from-literal=AWS_SECRET_ACCESS_KEY=gosparktest-secret \
  --from-literal=AWS_REGION=us-east-1 \
  --from-literal=AWS_ENDPOINT_URL=http://minio:9000 \
  --from-literal=AWS_S3_PATH_STYLE=true --dry-run=client -o yaml | kubectl apply -f -
kubectl set env statefulset/gospark-exec -n "$NS" --from=secret/gospark-s3-test
kubectl set env statefulset/gospark-exec -n "$NS" GOSPARK_TEST_FAIL_SAVE_ONCE=true
kubectl rollout status statefulset/gospark-exec -n "$NS" --timeout=180s
kubectl wait --for=condition=Ready pod -l app=gospark-exec -n "$NS" --timeout=180s
kubectl exec -n "$NS" gospark-exec-0 -- /gospark-worker create-bucket gospark-output

kubectl delete job gospark-driver -n "$NS" --ignore-not-found --wait=true --timeout=60s
GOSPARK_NAMESPACE="$NS" GOSPARK_IMAGE="${GOSPARK_IMAGE:-gospark/worker:latest}" GOSPARK_TASK=k8s-wc \
  GOSPARK_OUTPUT="$OUTPUT" GOSPARK_S3_SECRET=gospark-s3-test \
  ./bin/gospark-worker print-k8s driver | kubectl apply -f -
kubectl wait --for=condition=complete job/gospark-driver -n "$NS" --timeout=300s
driver=$(kubectl get pods -n "$NS" -l job-name=gospark-driver \
  --field-selector=status.phase=Succeeded -o jsonpath='{.items[0].metadata.name}')
logs=$(kubectl logs -n "$NS" "$driver")
echo "$logs"
grep -Fq 'OUTPUT_COMMITTED ' <<<"$logs"
grep -Fq 'Schedule PASSED!' <<<"$logs"

verified=$(kubectl exec -n "$NS" gospark-exec-0 -- /gospark-worker verify-output "$OUTPUT")
echo "$verified"
for marker in 'PARTITION index=0 attempt=1' 'PARTITION index=1 attempt=0' \
  '{hello 3}' '{world 2}' '{spark 2}' 'OUTPUT_VERIFIED parts=2'; do
  if ! grep -Fq "$marker" <<<"$verified"; then
    echo "missing committed output: $marker" >&2
    exit 1
  fi
done

# A second driver submission must fail rather than publish a different job.
if duplicate=$(kubectl exec -n "$NS" gospark-exec-0 -- env GOSPARK_TASK=k8s-wc \
  GOSPARK_PARTITIONS=2 GOSPARK_ACTION=save GOSPARK_OUTPUT="$OUTPUT" \
  GOSPARK_WORKERS="http://127.0.0.1:8080,http://gospark-exec-1.gospark-exec:8080" \
  /gospark-worker schedule 2>&1); then
  echo "duplicate output was accepted" >&2
  exit 1
fi
if ! grep -Fq 'output already committed' <<<"$duplicate"; then
  echo "duplicate submission failed for the wrong reason: $duplicate" >&2
  exit 1
fi
kubectl exec -n "$NS" gospark-exec-0 -- /gospark-worker verify-output "$OUTPUT" >/dev/null
echo "=== distributed MinIO output commit PASSED ==="
