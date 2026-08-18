package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	duckDb "github.com/marcboeker/go-duckdb"
)

// TestSessionSettingsSurviveConnectionRecycling pins the connector init hook:
// USE public, SET timezone, and SET scalar_subquery_error_on_multiple_rows are
// session-scoped, and the sql.DB pool recycles connections (SetConnMaxLifetime).
// Applied once at boot they vanish on the first recycled connection — current
// schema reverts to main, timestamps flip to host-local, multi-row scalar
// subqueries start erroring. With a 1ms lifetime every checkout gets a brand-new
// connection, so each iteration exercises a fresh session.
func TestSessionSettingsSurviveConnectionRecycling(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()

	duckdb.db.SetConnMaxLifetime(time.Millisecond)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Millisecond) // let the pooled connection expire

		var schema string
		row := duckdb.db.QueryRowContext(ctx, "SELECT current_schema()")
		if err := row.Scan(&schema); err != nil {
			t.Fatalf("iteration %d: current_schema: %v", i, err)
		}
		if schema != "public" {
			t.Fatalf("iteration %d: current schema reverted to %q after recycling (USE public lost)", i, schema)
		}

		var timezone string
		row = duckdb.db.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = 'TimeZone'")
		if err := row.Scan(&timezone); err != nil {
			t.Fatalf("iteration %d: timezone lookup: %v", i, err)
		}
		if timezone != "UTC" {
			t.Fatalf("iteration %d: timezone reverted to %q after recycling (SET timezone lost)", i, timezone)
		}

		var value int
		row = duckdb.db.QueryRowContext(ctx, "SELECT (SELECT 1 FROM (VALUES (1), (2)) t(x))")
		if err := row.Scan(&value); err != nil {
			t.Fatalf("iteration %d: multi-row scalar subquery errored after recycling (SET scalar_subquery_error_on_multiple_rows lost): %v", i, err)
		}
	}
}

// TestSessionInitHookConvergesUnderConcurrentFreshConnections pins the
// USE/create/retry arbiter in duckdbSessionInitFn: on a fresh instance with no
// boot sequence, 16 connections race the very first CREATE SCHEMA — the class
// where CREATE ... IF NOT EXISTS alone loses write-write conflicts and, since a
// hook failure fails connection creation, a regression here is a permanent
// outage, not an error. The session list here omits SET timezone on purpose:
// its ICU autoload race is a separate instance-level hazard fixed by preloading
// ICU in the boot list, and including it would test that shield instead of the
// schema arbiter.
func TestSessionInitHookConvergesUnderConcurrentFreshConnections(t *testing.T) {
	ctx := context.Background()
	connector, err := duckDb.NewConnector("", duckdbSessionInitFn(ctx, []string{"SET scalar_subquery_error_on_multiple_rows=false"}))
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(ctx) // overlapping Conn calls force fresh physical connections
			if err != nil {
				errs <- fmt.Errorf("connection creation failed (hook error): %w", err)
				return
			}
			defer conn.Close()
			var schema string
			if err := conn.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
				errs <- err
				return
			}
			if schema != "public" {
				errs <- fmt.Errorf("expected schema public, got %q", schema)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent fresh connection: %v", err)
	}
}

// TestDuckdbMemorySizeRegexpMatchesEngine keeps the config-parse guard aligned
// with the engine's own memory-size parser, in the two directions that matter:
// (a) hard rule — anything the guard accepts the engine must accept, else a
// fail-soft SET silently no-ops and the OOM mitigation is absent; (b) every
// reasonable legal form must pass the guard, else legal config panics at boot.
// The guard MAY reject engine-tolerated junk (the engine parses "--1"): failing
// loud on pathological input is intent, not drift. If a DuckDB upgrade shifts
// the parser, this fails and the regexp follows.
func TestDuckdbMemorySizeRegexpMatchesEngine(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, false)
	defer duckdb.Close()
	ctx := context.Background()

	engineAccepts := func(value string) bool {
		_, err := duckdb.ExecContext(ctx, "SET allocator_flush_threshold='"+value+"'", nil)
		return err == nil
	}

	// (a) Everything the guard passes through must be engine-legal.
	for _, value := range []string{
		"64MB", "64M", "64 MB", " 64MB ", "1G", "1GiB", "1.5GB", "1kb", "0MB", "64B", "2KiB", "1TB", "1TiB", "-1",
		"1PiB", "1P", "64", "64XB", "80%", "MB", "1.5", "--1", "64 X B",
	} {
		if duckdbMemorySizeRegexp.MatchString(value) && !engineAccepts(value) {
			t.Errorf("guard accepts %q but the engine rejects it — the fail-soft SET would silently no-op; tighten duckdbMemorySizeRegexp", value)
		}
	}

	// (b) Canonical legal forms must pass the guard (and stay engine-legal).
	for _, value := range []string{"64MB", "64M", "1G", "1GiB", "1.5GB", "1kb", "2KiB", "1TB", "-1", "4GB", "2GB"} {
		if !duckdbMemorySizeRegexp.MatchString(value) {
			t.Errorf("guard rejects legal value %q — boot would panic on good config; loosen duckdbMemorySizeRegexp", value)
		}
		if !engineAccepts(value) {
			t.Errorf("engine no longer accepts %q — update the canonical list and the regexp together", value)
		}
	}
}

// setStatementRegexp parses SET statements including an optional GLOBAL/
// SESSION/LOCAL qualifier; the scope-guard test fails on any SET it cannot
// parse rather than silently skipping it.
var setStatementRegexp = regexp.MustCompile(`(?i)^SET\s+(?:(GLOBAL|SESSION|LOCAL)\s+)?(\w+)\s*=`)

// TestBootAndSessionQueryScopes guards the boot/session list split with the
// engine's own scope metadata: a session-scoped (LOCAL) SET placed in the boot
// list is applied to one pooled connection and silently vanishes on recycling —
// the exact bug the session hook exists to prevent — and the recycling test
// only asserts the settings it knows about. The inverse (a GLOBAL SET in the
// session list) is redundant per-connection work and belongs at boot.
func TestBootAndSessionQueryScopes(t *testing.T) {
	config := loadTestConfig()
	duckdb := NewDuckdb(config, true)
	defer duckdb.Close()
	ctx := context.Background()

	scopeOf := func(t *testing.T, settingName string) string {
		t.Helper()
		var scope string
		row := duckdb.db.QueryRowContext(ctx, "SELECT scope FROM duckdb_settings() WHERE lower(name) = lower('"+settingName+"')")
		if err := row.Scan(&scope); err != nil {
			t.Fatalf("could not determine scope of setting %q: %v", settingName, err)
		}
		return scope
	}

	// parseSet returns (qualifier, name, isSet); a SET the regexp cannot parse
	// fails the test — an unparseable form evading the guard is how a mis-scoped
	// SET would sneak back in.
	parseSet := func(t *testing.T, query string) (string, string, bool) {
		t.Helper()
		trimmed := strings.TrimSpace(query)
		if !strings.HasPrefix(strings.ToUpper(trimmed), "SET ") {
			return "", "", false
		}
		match := setStatementRegexp.FindStringSubmatch(trimmed)
		if match == nil {
			t.Fatalf("SET statement %q not parseable by the scope guard — broaden setStatementRegexp", query)
		}
		return strings.ToUpper(match[1]), match[2], true
	}

	for _, query := range DUCKDB_INIT_BOOT_QUERIES {
		qualifier, name, isSet := parseSet(t, query)
		if !isSet || qualifier == "GLOBAL" { // non-SETs (INSTALL/LOAD/…) and explicit SET GLOBAL are instance-wide
			continue
		}
		if qualifier == "SESSION" || qualifier == "LOCAL" {
			t.Errorf("boot query %q is explicitly session-scoped — move it to DUCKDB_SESSION_INIT_QUERIES", query)
			continue
		}
		if scope := scopeOf(t, name); scope != "GLOBAL" {
			t.Errorf("boot query %q sets a %s-scoped setting: it will apply to one pooled connection and vanish on recycling — move it to DUCKDB_SESSION_INIT_QUERIES", query, scope)
		}
	}
	for _, query := range DUCKDB_SESSION_INIT_QUERIES {
		qualifier, name, isSet := parseSet(t, query)
		if !isSet || qualifier == "SESSION" || qualifier == "LOCAL" {
			continue
		}
		if qualifier == "GLOBAL" {
			t.Errorf("session query %q is explicitly instance-wide — move it to the boot path", query)
			continue
		}
		if scope := scopeOf(t, name); scope != "LOCAL" {
			t.Errorf("session query %q sets a %s-scoped setting: it re-runs on every new connection for no reason — move it to the boot path", query, scope)
		}
	}
}
