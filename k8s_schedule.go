package spark

import (
	"fmt"
	"strings"
)

const WorkerListenPort = 8080

// K8sMount attaches a ConfigMap as a directory of input files.
type K8sMount struct {
	Name      string
	ConfigMap string
	Path      string
}

func K8sExecutorManifest(namespace, image string, replicas int) string {
	return k8sExecutorManifest(namespace, image, replicas, nil)
}

// K8sExecutorManifestWithData is the executor manifest plus input mounts.
func K8sExecutorManifestWithData(namespace, image string, replicas int, mounts []K8sMount) string {
	return k8sExecutorManifest(namespace, image, replicas, mounts)
}

func k8sExecutorManifest(namespace, image string, replicas int, mounts []K8sMount) string {
	if namespace == "" {
		namespace = "default"
	}
	if replicas <= 0 {
		replicas = 2
	}
	volumeMounts := ""
	volumes := ""
	if len(mounts) > 0 {
		volumeMounts = "        volumeMounts:\n"
		volumes = "      volumes:\n"
		for _, m := range mounts {
			volumeMounts += fmt.Sprintf("        - name: %s\n          mountPath: %s\n", m.Name, m.Path)
			volumes += fmt.Sprintf("      - name: %s\n        configMap:\n          name: %s\n", m.Name, m.ConfigMap)
		}
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
%s
%s
`, namespace, WorkerListenPort, WorkerListenPort, namespace, replicas, image, WorkerListenPort, WorkerListenPort, WorkerListenPort, volumeMounts, volumes)
}

func K8sDriverManifest(namespace, image, taskName string, partitions, replicas int) string {
	return k8sDriverManifest(namespace, image, taskName, partitions, replicas, 0)
}

// K8sFailureTestDriverManifest pauses once the second map output is published,
// allowing the integration test to remove that executor before reduce starts.
func K8sFailureTestDriverManifest(namespace, image, taskName string, partitions, replicas, pauseSeconds int) string {
	return k8sDriverManifest(namespace, image, taskName, partitions, replicas, pauseSeconds)
}

func k8sDriverManifest(namespace, image, taskName string, partitions, replicas, pauseSeconds int) string {
	return k8sDriverManifestWithSave(namespace, image, taskName, partitions, replicas, pauseSeconds, "", "", "", nil)
}

// K8sDriverManifestWithInput sets GOSPARK_INPUT and mounts the same input
// directories the executors see. The driver must plan against those files.
func K8sDriverManifestWithInput(namespace, image, taskName, inputPath string, partitions, replicas int, mounts []K8sMount) string {
	extra := ""
	if inputPath != "" {
		extra = fmt.Sprintf("        - name: GOSPARK_INPUT\n          value: %q\n", inputPath)
	}
	return k8sDriverManifestWithSave(namespace, image, taskName, partitions, replicas, 0, "", "", extra, mounts)
}

// K8sSaveDriverManifest submits an output action; s3Secret names an optional
// Secret containing AWS_* settings for the driver (and separately for workers).
func K8sSaveDriverManifest(namespace, image, taskName, outputPath, s3Secret string, partitions, replicas int) string {
	return k8sDriverManifestWithSave(namespace, image, taskName, partitions, replicas, 0, outputPath, s3Secret, "", nil)
}

func k8sDriverManifestWithSave(namespace, image, taskName string, partitions, replicas, pauseSeconds int, outputPath, s3Secret, extraEnv string, mounts []K8sMount) string {
	if namespace == "" {
		namespace = "default"
	}
	if replicas <= 0 {
		replicas = 2
	}
	if partitions <= 0 {
		partitions = replicas
	}
	testEnv := ""
	if pauseSeconds > 0 {
		testEnv = fmt.Sprintf("        - name: GOSPARK_TEST_PAUSE_AFTER_MAP\n          value: %q\n", fmt.Sprint(pauseSeconds))
	}
	if outputPath != "" {
		testEnv += fmt.Sprintf("        - name: GOSPARK_ACTION\n          value: save\n        - name: GOSPARK_OUTPUT\n          value: %q\n", outputPath)
	}
	if s3Secret != "" {
		testEnv += fmt.Sprintf("        envFrom:\n        - secretRef:\n            name: %s\n", s3Secret)
	}
	testEnv += extraEnv
	volumeMounts, volumes := k8sMountYAML(mounts, "        ", "      ")
	return fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: gospark-driver
  namespace: %s
spec:
  backoffLimit: 3
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
%s
%s
%s`, namespace, image, taskName, partitions, strings.Join(K8sWorkerURLs("gospark-exec", namespace, replicas), ","), testEnv, volumeMounts, volumes)
}

func k8sMountYAML(mounts []K8sMount, mountIndent, volumeIndent string) (string, string) {
	if len(mounts) == 0 {
		return "", ""
	}
	volumeMounts := mountIndent + "volumeMounts:\n"
	volumes := volumeIndent + "volumes:\n"
	for _, m := range mounts {
		volumeMounts += fmt.Sprintf("%s- name: %s\n%s  mountPath: %s\n", mountIndent, m.Name, mountIndent, m.Path)
		volumes += fmt.Sprintf("%s- name: %s\n%s  configMap:\n%s    name: %s\n", volumeIndent, m.Name, volumeIndent, volumeIndent, m.ConfigMap)
	}
	return volumeMounts, volumes
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
