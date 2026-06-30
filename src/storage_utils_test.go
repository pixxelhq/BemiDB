package main

import (
	"strings"
	"testing"
)

func TestOverlappingRowsSql(t *testing.T) {
	s := &StorageUtils{}

	t.Run("uses the primary key columns when present", func(t *testing.T) {
		got := s.overlappingRowsSql([]string{"id", "name"}, []string{"id"})
		want := "SELECT 1 FROM existing_parquet WHERE EXISTS (SELECT 1 FROM new_parquet WHERE existing_parquet.id = new_parquet.id) LIMIT 1"
		if got != want {
			t.Errorf("PK case:\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("falls back to all columns with NULL-safe equality when there is no primary key", func(t *testing.T) {
		got := s.overlappingRowsSql([]string{"a", "b"}, nil)
		want := "SELECT 1 FROM existing_parquet WHERE EXISTS (SELECT 1 FROM new_parquet WHERE existing_parquet.a IS NOT DISTINCT FROM new_parquet.a AND existing_parquet.b IS NOT DISTINCT FROM new_parquet.b) LIMIT 1"
		if got != want {
			t.Errorf("no-PK case:\n got: %s\nwant: %s", got, want)
		}
		// Regression guard: must NOT emit the invalid bare JOIN that crashed DuckDB
		// ("syntax error at or near LIMIT") for keyless tables.
		if strings.Contains(got, "JOIN new_parquet LIMIT") {
			t.Errorf("no-PK case still emits invalid bare JOIN: %s", got)
		}
		// NULL-safe equality is required so rows with NULLs dedup instead of duplicating.
		if strings.Contains(got, "new_parquet.a = ") || strings.Contains(got, " = new_parquet.a") {
			t.Errorf("no-PK case must use NULL-safe equality, not '=': %s", got)
		}
	})

	// The overlap pre-check and the overwrite must condition on the same columns with
	// the same operator, otherwise resume could detect "no overlap" yet the overwrite
	// dedup differently, leaving duplicates or stale rows.
	t.Run("match conditions identical to selectNonOverlappingRowsSql", func(t *testing.T) {
		cols := []string{"a", "b", "c"}
		for _, pk := range [][]string{{"a"}, nil} {
			overlap := s.overlappingRowsSql(cols, pk)
			overwrite := s.selectNonOverlappingRowsSql(cols, pk)
			op := " = "
			keys := pk
			if len(pk) == 0 {
				op = " IS NOT DISTINCT FROM "
				keys = cols
			}
			for _, c := range keys {
				cond := "existing_parquet." + c + op + "new_parquet." + c
				if !strings.Contains(overlap, cond) {
					t.Errorf("pk=%v: overlap query missing condition %q: %s", pk, cond, overlap)
				}
				if !strings.Contains(overwrite, cond) {
					t.Errorf("pk=%v: overwrite query missing condition %q: %s", pk, cond, overwrite)
				}
			}
		}
	})
}
