#!/bin/bash
set -e

echo "=== goSpark K8s Integration Test ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

NAMESPACE="gospark-test"
IMAGE="gospark/worker:latest"
CLUSTER="kind-cluster"

echo "1. Building worker binary..."
cd "$PROJECT_DIR"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/gospark-worker ./cmd/gospark-worker/
echo "   Built: ./bin/gospark-worker"

echo "2. Building container image..."
podman build -t "$IMAGE" -f Dockerfile . 2>&1 | tail -3
echo "   Built image: $IMAGE"

echo "3. Loading image into kind cluster..."
kind load docker-image "$IMAGE" --name "$CLUSTER" 2>&1 | tail -3
echo "   Image loaded into cluster"

echo "4. Setting up namespace: $NAMESPACE"
kubectl delete namespace "$NAMESPACE" --ignore-not-found --grace-period=0 2>/dev/null || true
sleep 2
kubectl create namespace "$NAMESPACE" 2>&1
echo "   Namespace created"

echo "5. Running smoke test (single pod)..."
kubectl delete job gospark-smoke -n "$NAMESPACE" --ignore-not-found 2>/dev/null || true
sleep 1

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: gospark-smoke
  namespace: $NAMESPACE
spec:
  template:
    spec:
      containers:
      - name: smoke
        image: $IMAGE
        imagePullPolicy: IfNotPresent
        command: ["./gospark-worker", "smoke"]
      restartPolicy: OnFailure
EOF

echo "   Waiting for smoke test to complete..."
SECONDS=0
while true; do
    SUCCEEDED=$(kubectl get job gospark-smoke -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
    FAILED=$(kubectl get job gospark-smoke -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
    if [ "$SUCCEEDED" = "1" ]; then
        echo "   Smoke test PASSED!"
        POD=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-smoke -o jsonpath='{.items[0].metadata.name}')
        kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | head -10
        break
    fi
    if [ "$FAILED" != "0" ] && [ -n "$FAILED" ]; then
        echo "   Smoke test FAILED!"
        POD=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-smoke -o jsonpath='{.items[0].metadata.name}')
        kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | tail -20
        exit 1
    fi
    if [ $SECONDS -gt 120 ]; then
        echo "   Timeout waiting for smoke test"
        POD=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-smoke -o jsonpath='{.items[0].metadataName}')
        kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | tail -20
        exit 1
    fi
    sleep 3
done

echo ""
echo "6. Running distributed word count (3 parallel pods)..."
kubectl delete job gospark-wordcount -n "$NAMESPACE" --ignore-not-found 2>/dev/null || true
sleep 1

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: gospark-wordcount
  namespace: $NAMESPACE
spec:
  parallelism: 3
  completions: 3
  completionMode: Indexed
  template:
    spec:
      containers:
      - name: worker
        image: $IMAGE
        imagePullPolicy: IfNotPresent
        command: ["./gospark-worker", "wordcount"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
      restartPolicy: OnFailure
EOF

echo "   Waiting for distributed word count..."
SECONDS=0
while true; do
    SUCCEEDED=$(kubectl get job gospark-wordcount -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
    FAILED=$(kubectl get job gospark-wordcount -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
    if [ "$SUCCEEDED" = "3" ]; then
        echo "   Distributed word count PASSED!"
        echo ""
        echo "   Worker pod results:"
        PODS=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-wordcount -o jsonpath='{.items[*].metadata.name}')
        for POD in $PODS; do
            echo "   --- Pod: $POD ---"
            kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | head -15
            echo ""
        done
        break
    fi
    if [ "$FAILED" != "0" ] && [ -n "$FAILED" ]; then
        echo "   Distributed word count FAILED!"
        PODS=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-wordcount -o jsonpath='{.items[*].metadata.name}')
        for POD in $PODS; do
            echo "   --- Pod: $POD ---"
            kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | tail -20
        done
        exit 1
    fi
    if [ $SECONDS -gt 180 ]; then
        echo "   Timeout waiting for distributed word count"
        kubectl get pods -n "$NAMESPACE" -l job-name=gospark-wordcount 2>&1
        PODS=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-wordcount -o jsonpath='{.items[*].metadata.name}')
        for POD in $PODS; do
            kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | tail -20
        done
        exit 1
    fi
    sleep 3
done

echo ""
echo "7. Running distributed computation (3 parallel pods)..."
kubectl delete job gospark-distributed -n "$NAMESPACE" --ignore-not-found 2>/dev/null || true
sleep 1

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: gospark-distributed
  namespace: $NAMESPACE
spec:
  parallelism: 3
  completions: 3
  completionMode: Indexed
  template:
    spec:
      containers:
      - name: worker
        image: $IMAGE
        imagePullPolicy: IfNotPresent
        command: ["./gospark-worker", "distributed"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
      restartPolicy: OnFailure
EOF

echo "   Waiting for distributed computation..."
SECONDS=0
while true; do
    SUCCEEDED=$(kubectl get job gospark-distributed -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")
    FAILED=$(kubectl get job gospark-distributed -n "$NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
    if [ "$SUCCEEDED" = "3" ]; then
        echo "   Distributed computation PASSED!"
        echo ""
        echo "   Worker pod results:"
        PODS=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-distributed -o jsonpath='{.items[*].metadata.name}')
        for POD in $PODS; do
            echo "   --- Pod: $POD ---"
            kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | head -20
            echo ""
        done
        break
    fi
    if [ "$FAILED" != "0" ] && [ -n "$FAILED" ]; then
        echo "   Distributed computation FAILED!"
        PODS=$(kubectl get pods -n "$NAMESPACE" -l job-name=gospark-distributed -o jsonpath='{.items[*].metadata.name}')
        for POD in $PODS; do
            echo "   --- Pod: $POD ---"
            kubectl logs "$POD" -n "$NAMESPACE" 2>&1 | tail -20
        done
        exit 1
    fi
    if [ $SECONDS -gt 180 ]; then
        echo "   Timeout waiting for distributed computation"
        kubectl get pods -n "$NAMESPACE" -l job-name=gospark-distributed 2>&1
        exit 1
    fi
    sleep 3
done

echo ""
echo "8. Cleanup..."
echo "   Namespace $NAMESPACE left for inspection (kubectl get all -n $NAMESPACE)"
echo "   To clean up: kubectl delete namespace $NAMESPACE"

echo ""
echo "=== All K8s integration tests PASSED! ==="