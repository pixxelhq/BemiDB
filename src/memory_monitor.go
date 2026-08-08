package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
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

// sampleMemory gathers one sample. duckdb may be nil (the sync command creates
// short-lived DuckDB instances per merge, so there is no engine to interrogate);
// DuckDbBytes is then the -1 "unavailable" sentinel.
func sampleMemory(ctx context.Context, duckdb *Duckdb) MemorySample {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	rss := processRssBytes()
	duckDbBytes := duckDbMemoryBytes(ctx, duckdb)

	// Untracked = RSS we can't attribute to the Go heap or DuckDB's own accounting.
	// The -1 sentinel must not join the arithmetic: subtracting it would reclassify
	// the entire (unknown) DuckDB footprint as "untracked".
	untracked := rss - int64(memStats.HeapAlloc)
	if duckDbBytes >= 0 {
		untracked -= duckDbBytes
	}
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

// processRssBytes returns the memory the OOM killer accounts for: cgroup v2's
// memory.current, then cgroup v1's memory.usage_in_bytes — both charge page cache
// (e.g. DuckDB temp spill) to the container the way the OOM killer does. VmRSS is
// the last-resort fallback outside cgroups; it counts only mapped pages, so it can
// understate OOM-relevant usage. Returns 0 if nothing is readable.
func processRssBytes() int64 {
	cgroupPaths := []string{
		"/sys/fs/cgroup/memory.current",               // cgroup v2
		"/sys/fs/cgroup/memory/memory.usage_in_bytes", // cgroup v1
	}
	for _, path := range cgroupPaths {
		if data, err := os.ReadFile(path); err == nil {
			if v, err := StringToInt64(strings.TrimSpace(string(data))); err == nil {
				return v
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := StringToInt64(fields[1]); err == nil {
						return kb * 1024
					}
				}
			}
		}
	}
	return 0
}

// duckDbMemoryBytes sums DuckDB's own tracked memory. Returns -1 when it is
// unavailable — nil duckdb, query error (e.g. a future engine drops the function),
// or timeout — so the caller can tell "unavailable" apart from a real zero. The
// timeout matters: without it, an engine wedged by the very memory pressure being
// observed would hang the sampler right when its output is needed.
func duckDbMemoryBytes(ctx context.Context, duckdb *Duckdb) int64 {
	if duckdb == nil {
		return -1
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rows, err := duckdb.QueryContext(ctx, "SELECT COALESCE(sum(memory_usage_bytes), 0) FROM duckdb_memory()")
	if err != nil {
		return -1
	}
	defer rows.Close()

	if !rows.Next() {
		return -1
	}
	var bytes int64
	if err := rows.Scan(&bytes); err != nil {
		return -1
	}
	if rows.Err() != nil {
		return -1
	}
	return bytes
}

// StartMemoryMonitor logs a memory sample every config.MemorySampleSeconds. This is
// the black-box recorder: an OOM leaves no dying words, but the last sample before
// the kill survives in the previous container's logs (kubectl logs --previous).
func StartMemoryMonitor(config *Config, duckdb *Duckdb) {
	// Guarding the computed Duration (not the int) also rejects values large enough
	// to overflow the multiplication, which would panic time.NewTicker.
	interval := time.Duration(config.MemorySampleSeconds) * time.Second
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			s := sampleMemory(context.Background(), duckdb)
			LogInfo(config, "Memory sample:",
				"rss="+formatMiB(s.RssBytes),
				"go_heap="+formatMiB(s.GoHeapBytes),
				"go_sys="+formatMiB(s.GoSysBytes),
				"duckdb="+formatMiB(s.DuckDbBytes),
				"untracked="+formatMiB(s.UntrackedBytes),
				"goroutines="+IntToString(s.Goroutines),
			)
		}
	}()
}

func metricsExposition(w http.ResponseWriter, s MemorySample) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP bemidb_process_rss_bytes Resident set size the OOM killer accounts for.\n")
	fmt.Fprintf(w, "# TYPE bemidb_process_rss_bytes gauge\nbemidb_process_rss_bytes %d\n", s.RssBytes)
	fmt.Fprintf(w, "# HELP bemidb_go_heap_bytes Go heap in use (runtime.MemStats HeapAlloc).\n")
	fmt.Fprintf(w, "# TYPE bemidb_go_heap_bytes gauge\nbemidb_go_heap_bytes %d\n", s.GoHeapBytes)
	fmt.Fprintf(w, "# HELP bemidb_go_sys_bytes Total memory the Go runtime obtained from the OS (runtime.MemStats Sys).\n")
	fmt.Fprintf(w, "# TYPE bemidb_go_sys_bytes gauge\nbemidb_go_sys_bytes %d\n", s.GoSysBytes)
	// Absent — not -1 — when unavailable: a sentinel sample would silently corrupt
	// Prometheus sum/avg aggregations, while an absent series models "unknown".
	if s.DuckDbBytes >= 0 {
		fmt.Fprintf(w, "# HELP bemidb_duckdb_bytes DuckDB tracked memory (duckdb_memory()); absent if unavailable.\n")
		fmt.Fprintf(w, "# TYPE bemidb_duckdb_bytes gauge\nbemidb_duckdb_bytes %d\n", s.DuckDbBytes)
	}
	fmt.Fprintf(w, "# HELP bemidb_untracked_bytes RSS not attributable to the Go heap or DuckDB's tracked memory.\n")
	fmt.Fprintf(w, "# TYPE bemidb_untracked_bytes gauge\nbemidb_untracked_bytes %d\n", s.UntrackedBytes)
	fmt.Fprintf(w, "# HELP bemidb_goroutines Number of Go goroutines (proxy for concurrent connections/queries).\n")
	fmt.Fprintf(w, "# TYPE bemidb_goroutines gauge\nbemidb_goroutines %d\n", s.Goroutines)
}

func formatMiB(bytes int64) string {
	if bytes < 0 {
		return "n/a"
	}
	return Int64ToString(bytes/(1024*1024)) + "MiB"
}

// StartMetricsServer serves Prometheus metrics at /metrics on config.MetricsPort.
// Hand-written exposition format avoids a new dependency for a handful of gauges.
func StartMetricsServer(config *Config, duckdb *Duckdb) {
	if config.MetricsPort == "" {
		return
	}
	// Bind synchronously so a taken or malformed port fails loudly at startup,
	// instead of logging success and then dying quietly in a goroutine.
	listener, err := net.Listen("tcp", ":"+config.MetricsPort)
	if err != nil {
		LogError(config, "Metrics: failed to listen on :"+config.MetricsPort+":", err)
		return
	}
	LogInfo(config, "Metrics: serving /metrics on :"+config.MetricsPort)

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsExposition(w, sampleMemory(r.Context(), duckdb))
	})
	// Explicit timeouts: the zero-value http.Server has none, letting slow or
	// half-open clients pin goroutines and buffers forever.
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			LogError(config, "Metrics server stopped:", err)
		}
	}()
}
