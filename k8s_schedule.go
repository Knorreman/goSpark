package spark

import (
	"fmt"
	"strings"
)

const WorkerListenPort = 8080

func K8sExecutorManifest(namespace, image string, replicas int) string {
	if namespace == "" {
		namespace = "default"
	}
	if replicas <= 0 {
		replicas = 2
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Service
metadata:
  name: gospark-exec
  namespace: %s
spec:
  clusterIP: None
  ports:
  - name: worker
    port: %d
    targetPort: %d
  selector:
    app: gospark-exec
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: gospark-exec
  namespace: %s
spec:
  serviceName: gospark-exec
  replicas: %d
  selector:
    matchLabels:
      app: gospark-exec
  template:
    metadata:
      labels:
        app: gospark-exec
    spec:
      containers:
      - name: worker
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["/gospark-worker", "serve"]
        env:
        - name: GOSPARK_LISTEN
          value: "0.0.0.0:%d"
        - name: GOSPARK_STORE
          value: "/tmp/gospark-shuffle"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NS
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: GOSPARK_ADVERTISE_URL
          value: "http://$(POD_NAME).gospark-exec.$(POD_NS).svc.cluster.local:%d"
        ports:
        - containerPort: %d
          name: worker
        readinessProbe:
          httpGet:
            path: /health
            port: worker
          initialDelaySeconds: 1
          periodSeconds: 2
`, namespace, WorkerListenPort, WorkerListenPort, namespace, replicas, image, WorkerListenPort, WorkerListenPort, WorkerListenPort)
}

func K8sDriverManifest(namespace, image, taskName string, partitions, replicas int) string {
	if namespace == "" {
		namespace = "default"
	}
	if replicas <= 0 {
		replicas = 2
	}
	if partitions <= 0 {
		partitions = replicas
	}
	return fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: gospark-driver
  namespace: %s
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: driver
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["/gospark-worker", "schedule"]
        env:
        - name: GOSPARK_TASK
          value: %q
        - name: GOSPARK_PARTITIONS
          value: "%d"
        - name: GOSPARK_WORKERS
          value: %q
`, namespace, image, taskName, partitions, strings.Join(K8sWorkerURLs("gospark-exec", namespace, replicas), ","))
}

func K8sWorkerURLs(service, namespace string, replicas int) []string {
	urls := make([]string, replicas)
	for i := 0; i < replicas; i++ {
		host := fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", service, i, service, namespace)
		urls[i] = fmt.Sprintf("http://%s:%d", host, WorkerListenPort)
	}
	return urls
}

func K8sWorkerPodManifest(namespace, image, podName string) string {
	if namespace == "" {
		namespace = "default"
	}
	return fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app: gospark-exec
spec:
  restartPolicy: Never
  containers:
  - name: worker
    image: %s
    imagePullPolicy: IfNotPresent
    command: ["/gospark-worker", "serve"]
    env:
    - name: GOSPARK_LISTEN
      value: "0.0.0.0:%d"
    - name: GOSPARK_STORE
      value: "/tmp/gospark-shuffle"
    ports:
    - containerPort: %d
      name: worker
`, podName, namespace, image, WorkerListenPort, WorkerListenPort)
}
