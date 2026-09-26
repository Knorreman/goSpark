package broadcastaverage

import (
	"testing"

	spark "goSpark"
)

func TestLocalBroadcastAverage(t *testing.T) {
	result, err := Local()
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(result); err != nil {
		t.Fatal(err)
	}
}

func TestDistributedBroadcastAverage(t *testing.T) {
	var workers []spark.TaskRunner
	for i := 0; i < 2; i++ {
		server, addr, err := spark.ServeWorker(t.TempDir(), "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Close() })
		workers = append(workers, &spark.WorkerClient{BaseURL: "http://" + addr})
	}
	result, err := Distributed(workers, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(result); err != nil {
		t.Fatal(err)
	}
}
