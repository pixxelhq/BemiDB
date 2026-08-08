package main

import (
	"context"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
)

func TestSampleMemory(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	sample := sampleMemory(context.Background(), duckdb)

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
	if sample.GoSysBytes < sample.GoHeapBytes {
		t.Errorf("expected Go sys (total from OS) >= Go heap, got sys=%d heap=%d", sample.GoSysBytes, sample.GoHeapBytes)
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

// The sync command starts the monitor with no DuckDB handle; the sample must degrade
// to the -1 sentinel without panicking, and the sentinel must not leak into the
// untracked arithmetic (untracked would swallow the whole unknown DuckDB footprint).
func TestSampleMemoryWithoutDuckdb(t *testing.T) {
	sample := sampleMemory(context.Background(), nil)

	if sample.DuckDbBytes != -1 {
		t.Errorf("expected -1 sentinel without DuckDB, got %d", sample.DuckDbBytes)
	}
	if sample.GoHeapBytes <= 0 {
		t.Errorf("expected positive Go heap, got %d", sample.GoHeapBytes)
	}
	if sample.UntrackedBytes < 0 {
		t.Errorf("untracked must never be negative, got %d", sample.UntrackedBytes)
	}
}

func TestMetricsExposition(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	// metricsExposition is the whole body of the /metrics handler; a ResponseRecorder
	// exercises it (including the Content-Type header) without binding a port.
	recorder := httptest.NewRecorder()
	metricsExposition(recorder, sampleMemory(context.Background(), duckdb))
	text := recorder.Body.String()

	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", contentType)
	}
	for _, metric := range []string{
		"bemidb_process_rss_bytes",
		"bemidb_go_heap_bytes",
		"bemidb_go_sys_bytes",
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

// When DuckDB's number is unavailable the gauge must be absent — a -1 sample would
// silently corrupt Prometheus sum/avg aggregations.
func TestMetricsExpositionWithoutDuckdb(t *testing.T) {
	recorder := httptest.NewRecorder()
	metricsExposition(recorder, MemorySample{RssBytes: 1, GoHeapBytes: 1, DuckDbBytes: -1})

	if strings.Contains(recorder.Body.String(), "bemidb_duckdb_bytes") {
		t.Errorf("expected duckdb gauge to be absent when unavailable, got:\n%s", recorder.Body.String())
	}
}
