package spark

import (
	"strings"
	"testing"
)

func TestK8sExecutorManifest(t *testing.T) {
	m := K8sExecutorManifest("gospark", "gospark/worker:latest", 2)
	for _, want := range []string{
		"kind: StatefulSet",
		"kind: Service",
		"clusterIP: None",
		"/gospark-worker",
		"serve",
		"replicas: 2",
		"namespace: gospark",
		"path: /health",
		"GOSPARK_ADVERTISE_URL",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("manifest missing %q", want)
		}
	}
}

func TestK8sExecutorManifestWithData(t *testing.T) {
	m := K8sExecutorManifestWithData("gospark", "gospark/worker:latest", 2, []K8sMount{{
		Name: "text", ConfigMap: "gospark-text", Path: "/data/text",
	}})
	for _, want := range []string{"mountPath: /data/text", "name: gospark-text", "volumeMounts:"} {
		if !strings.Contains(m, want) {
			t.Errorf("data manifest missing %q", want)
		}
	}
}

func TestK8sDriverManifest(t *testing.T) {
	m := K8sDriverManifest("gospark", "gospark/worker:latest", "k8s-wc", 2, 2)
	for _, want := range []string{
		"kind: Job",
		"schedule",
		"k8s-wc",
		"gospark-exec-0.gospark-exec.gospark.svc.cluster.local",
		"gospark-exec-1.gospark-exec.gospark.svc.cluster.local",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("driver manifest missing %q\n%s", want, m)
		}
	}
}

func TestK8sWorkerURLs(t *testing.T) {
	urls := K8sWorkerURLs("gospark-exec", "ns", 2)
	if len(urls) != 2 {
		t.Fatalf("got %d urls", len(urls))
	}
	if !strings.Contains(urls[0], "gospark-exec-0.gospark-exec.ns.svc.cluster.local:8080") {
		t.Fatalf("url0=%s", urls[0])
	}
}
