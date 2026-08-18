package main

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"
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

var setStatementRegexp = regexp.MustCompile(`(?i)^SET\s+(\w+)\s*=`)

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

	for _, query := range DUCKDB_INIT_BOOT_QUERIES {
		match := setStatementRegexp.FindStringSubmatch(strings.TrimSpace(query))
		if match == nil {
			continue // not a SET statement (INSTALL/LOAD/…) — instance-wide by nature
		}
		if scope := scopeOf(t, match[1]); scope != "GLOBAL" {
			t.Errorf("boot query %q sets a %s-scoped setting: it will apply to one pooled connection and vanish on recycling — move it to DUCKDB_SESSION_INIT_QUERIES", query, scope)
		}
	}
	for _, query := range DUCKDB_SESSION_INIT_QUERIES {
		match := setStatementRegexp.FindStringSubmatch(strings.TrimSpace(query))
		if match == nil {
			continue
		}
		if scope := scopeOf(t, match[1]); scope != "LOCAL" {
			t.Errorf("session query %q sets a %s-scoped setting: it re-runs on every new connection for no reason — move it to the boot path", query, scope)
		}
	}
}
