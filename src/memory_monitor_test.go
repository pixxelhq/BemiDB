package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

func TestSampleMemory(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	sample := sampleMemory(config, duckdb)

	// RSS is read from cgroup/proc, which exist only on Linux (production). Elsewhere
	// (e.g. a macOS dev machine) it is best-effort 0; assert positivity only where the
	// readers apply so the test is portable.
	if runtime.GOOS == "linux" && sample.RssBytes <= 0 {
		t.Errorf("expected positive RSS on linux, got %d", sample.RssBytes)
	}
	if sample.RssBytes < 0 {
		t.Errorf("RSS must never be negative, got %d", sample.RssBytes)
	}
	if sample.GoHeapBytes <= 0 {
		t.Errorf("expected positive Go heap, got %d", sample.GoHeapBytes)
	}
	// DuckDB memory must be queryable (>= 0), not the -1 "unavailable" sentinel — this
	// is what pins that duckdb_memory() exists in the pinned engine.
	if sample.DuckDbBytes < 0 {
		t.Errorf("expected DuckDB memory to be queryable, got sentinel %d", sample.DuckDbBytes)
	}
	if sample.Goroutines <= 0 {
		t.Errorf("expected at least one goroutine, got %d", sample.Goroutines)
	}
}

func TestMetricsHandlerExposition(t *testing.T) {
	config := loadTestConfig()
	config.MetricsPort = "9090"
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	// Exercise the exact handler StartMetricsServer registers, without binding a port.
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := sampleMemory(config, duckdb)
		metricsExposition(w, s)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, metric := range []string{
		"bemidb_process_rss_bytes",
		"bemidb_go_heap_bytes",
		"bemidb_duckdb_bytes",
		"bemidb_untracked_bytes",
		"bemidb_goroutines",
	} {
		if !strings.Contains(text, metric+" ") {
			t.Errorf("expected metric %q in exposition, got:\n%s", metric, text)
		}
		if !strings.Contains(text, "# TYPE "+metric+" gauge") {
			t.Errorf("expected TYPE line for %q", metric)
		}
	}
}
