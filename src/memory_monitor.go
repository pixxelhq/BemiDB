package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Memory observability. BemiDB is Go (visible to runtime.MemStats) wrapping DuckDB's
// C++ engine (invisible to Go). An OOM kill is judged by the kernel on the process's
// total RSS, so diagnosing one needs three numbers that no single tool provides:
//
//   - Go heap        (runtime.MemStats)      — result buffering, wire encoding
//   - DuckDB memory  (duckdb_memory())       — scans, joins, the buffer pool
//   - process RSS    (cgroup / /proc)        — the true total the OOM killer sees
//
// RSS minus the other two is the "untracked" remainder (C++ string materialization,
// allocator fragmentation) that neither ledger accounts for.

type MemorySample struct {
	RssBytes       int64
	GoHeapBytes    int64
	GoSysBytes     int64
	DuckDbBytes    int64
	UntrackedBytes int64
	Goroutines     int
}

func sampleMemory(config *Config, duckdb *Duckdb) MemorySample {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	rss := processRssBytes()
	duckDbBytes := duckDbMemoryBytes(duckdb)

	// Untracked = RSS we can't attribute to the Go heap or DuckDB's own accounting.
	untracked := rss - int64(memStats.HeapAlloc) - duckDbBytes
	if untracked < 0 {
		untracked = 0
	}

	return MemorySample{
		RssBytes:       rss,
		GoHeapBytes:    int64(memStats.HeapAlloc),
		GoSysBytes:     int64(memStats.Sys),
		DuckDbBytes:    duckDbBytes,
		UntrackedBytes: untracked,
		Goroutines:     runtime.NumGoroutine(),
	}
}

// processRssBytes returns the memory the OOM killer accounts for. cgroup v2's
// memory.current is the truest match inside a container; /proc/self/status VmRSS is
// the fallback outside one. Returns 0 if neither is readable.
func processRssBytes() int64 {
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			return v
		}
	}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
						return kb * 1024
					}
				}
			}
		}
	}
	return 0
}

// duckDbMemoryBytes sums DuckDB's own tracked memory. Returns -1 if the query fails
// (e.g. a future engine drops the function) so the caller can tell "unavailable"
// apart from a real zero.
func duckDbMemoryBytes(duckdb *Duckdb) int64 {
	rows, err := duckdb.QueryContext(context.Background(), "SELECT COALESCE(sum(memory_usage_bytes), 0) FROM duckdb_memory()")
	if err != nil {
		return -1
	}
	defer rows.Close()

	var bytes int64
	if rows.Next() {
		if err := rows.Scan(&bytes); err != nil {
			return -1
		}
	}
	return bytes
}

// StartMemoryMonitor logs a memory sample every config.MemorySampleSeconds. This is
// the black-box recorder: an OOM leaves no dying words, but the last sample before
// the kill survives in the previous container's logs (kubectl logs --previous).
func StartMemoryMonitor(config *Config, duckdb *Duckdb) {
	if config.MemorySampleSeconds <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(config.MemorySampleSeconds) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s := sampleMemory(config, duckdb)
			LogInfo(config, "Memory sample:",
				"rss="+formatMiB(s.RssBytes),
				"go_heap="+formatMiB(s.GoHeapBytes),
				"duckdb="+formatMiB(s.DuckDbBytes),
				"untracked="+formatMiB(s.UntrackedBytes),
				"goroutines="+strconv.Itoa(s.Goroutines),
			)
		}
	}()
}

func metricsExposition(w io.Writer, s MemorySample) {
	if rw, ok := w.(http.ResponseWriter); ok {
		rw.Header().Set("Content-Type", "text/plain; version=0.0.4")
	}
	fmt.Fprintf(w, "# HELP bemidb_process_rss_bytes Resident set size the OOM killer accounts for.\n")
	fmt.Fprintf(w, "# TYPE bemidb_process_rss_bytes gauge\nbemidb_process_rss_bytes %d\n", s.RssBytes)
	fmt.Fprintf(w, "# HELP bemidb_go_heap_bytes Go heap in use (runtime.MemStats HeapAlloc).\n")
	fmt.Fprintf(w, "# TYPE bemidb_go_heap_bytes gauge\nbemidb_go_heap_bytes %d\n", s.GoHeapBytes)
	fmt.Fprintf(w, "# HELP bemidb_duckdb_bytes DuckDB tracked memory (duckdb_memory()); -1 if unavailable.\n")
	fmt.Fprintf(w, "# TYPE bemidb_duckdb_bytes gauge\nbemidb_duckdb_bytes %d\n", s.DuckDbBytes)
	fmt.Fprintf(w, "# HELP bemidb_untracked_bytes RSS not attributable to the Go heap or DuckDB.\n")
	fmt.Fprintf(w, "# TYPE bemidb_untracked_bytes gauge\nbemidb_untracked_bytes %d\n", s.UntrackedBytes)
	fmt.Fprintf(w, "# HELP bemidb_goroutines Number of Go goroutines (proxy for concurrent connections/queries).\n")
	fmt.Fprintf(w, "# TYPE bemidb_goroutines gauge\nbemidb_goroutines %d\n", s.Goroutines)
}

func formatMiB(bytes int64) string {
	if bytes < 0 {
		return "n/a"
	}
	return strconv.FormatInt(bytes/(1024*1024), 10) + "MiB"
}

// StartMetricsServer serves Prometheus metrics at /metrics on config.MetricsPort.
// Hand-written exposition format avoids a new dependency for three gauges.
func StartMetricsServer(config *Config, duckdb *Duckdb) {
	if config.MetricsPort == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsExposition(w, sampleMemory(config, duckdb))
	})
	go func() {
		LogInfo(config, "Metrics: serving /metrics on :"+config.MetricsPort)
		server := &http.Server{Addr: ":" + config.MetricsPort, Handler: mux}
		if err := server.ListenAndServe(); err != nil {
			LogError(config, "Metrics server stopped:", err)
		}
	}()
}
