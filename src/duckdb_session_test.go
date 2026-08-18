package main

import (
	"context"
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
