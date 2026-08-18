package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestPreparedStatementClose(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	t.Run("nil receiver", func(t *testing.T) {
		var preparedStatement *PreparedStatement
		preparedStatement.Close() // must not panic
	})

	t.Run("empty statement (no driver statement)", func(t *testing.T) {
		preparedStatement := &PreparedStatement{Name: "empty"}
		preparedStatement.Close() // must not panic
	})

	t.Run("closes statement and is idempotent", func(t *testing.T) {
		statement, err := queryHandler.duckdb.PrepareContext(context.Background(), "SELECT 1")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		preparedStatement := &PreparedStatement{Statement: statement}
		preparedStatement.Close()
		if preparedStatement.Statement != nil {
			t.Errorf("expected Statement to be nil after Close")
		}
		preparedStatement.Close() // second Close must not panic

		// The driver-side handle must actually be released: using the original
		// statement now fails instead of silently working against a live plan.
		if _, err := statement.QueryContext(context.Background()); err == nil {
			t.Errorf("expected query on closed statement to fail")
		}
	})

	t.Run("closes open rows before the statement", func(t *testing.T) {
		statement, err := queryHandler.duckdb.PrepareContext(context.Background(), "SELECT 1")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		rows, err := statement.QueryContext(context.Background())
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		preparedStatement := &PreparedStatement{Statement: statement, Rows: rows}
		preparedStatement.Close() // must not panic or deadlock with rows open
		if preparedStatement.Rows != nil || preparedStatement.Statement != nil {
			t.Errorf("expected Rows and Statement to be nil after Close")
		}
	})
}

// After a wire-protocol Close releases the statement, Describe and Execute on it
// must return an error (like Postgres' "prepared statement does not exist"), not
// dereference the nil Statement and crash the process.
func TestDescribeAndExecuteAfterStatementClosed(t *testing.T) {
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	preparedStatement := &PreparedStatement{
		OriginalQuery: "SELECT 1",
		Query:         "SELECT 1",
		Bound:         true,
		Statement:     nil, // released by PreparedStatement.Close()
	}

	if _, _, err := queryHandler.HandleDescribeQuery(&pgproto3.Describe{ObjectType: 'P'}, preparedStatement); err == nil {
		t.Errorf("expected Describe on closed statement to error")
	}
	if _, err := queryHandler.HandleExecuteQuery(&pgproto3.Execute{}, preparedStatement); err == nil {
		t.Errorf("expected Execute on closed statement to error")
	}
}
