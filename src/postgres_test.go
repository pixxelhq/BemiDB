package main

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// startTestPostgresClient boots the real connection loop (Postgres.Run) on a
// loopback TCP socket — net.Pipe is unbuffered and deadlocks when the server
// replies while the client is still flushing — completes the startup handshake,
// and returns a ready frontend plus the channel closed when Run returns.
// Cleanup of the listener and client connection is registered on t.
func startTestPostgresClient(t *testing.T, queryHandler *QueryHandler) (*pgproto3.Frontend, chan struct{}) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	done := make(chan struct{})
	go func() {
		serverConn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		postgres := NewPostgres(queryHandler.config, &serverConn)
		postgres.Run(queryHandler) // must return normally, not panic
		serverConn.Close()
		close(done)
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { clientConn.Close() })
	// Bound every read/write: without a deadline, a server that wedges before
	// replying leaves frontend.Receive() blocked until go test's global timeout.
	if err := clientConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	frontend := pgproto3.NewFrontend(clientConn, clientConn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "bemidb", "database": "bemidb"},
	})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("startup flush: %v", err)
	}
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("startup receive: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return frontend, done
		}
	}
}

// receiveUntilReady drains replies until ReadyForQuery, reporting what was seen.
func receiveUntilReady(t *testing.T, frontend *pgproto3.Frontend, step string) (sawRows bool, sawError bool) {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("%s: receive error (server likely crashed): %v", step, err)
		}
		switch msg.(type) {
		case *pgproto3.DataRow:
			sawRows = true
		case *pgproto3.ErrorResponse:
			sawError = true
		case *pgproto3.ReadyForQuery:
			return sawRows, sawError
		}
	}
}

// terminateAndAwait ends the connection and fails the test if the server's
// connection loop does not return (wedged or panicked).
func terminateAndAwait(t *testing.T, frontend *pgproto3.Frontend, done chan struct{}) {
	t.Helper()
	frontend.Send(&pgproto3.Terminate{})
	_ = frontend.Flush()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after Terminate — connection loop wedged or panicked")
	}
}

// TestRunDoesNotPanicOnErrorBeforeSync drives the exact extended-protocol
// sequence that used to crash the process: a parameterized query whose Bind/
// Describe fails, followed by Sync — which must always respond. The server must
// return an ErrorResponse and stay alive.
func TestRunDoesNotPanicOnErrorBeforeSync(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	// Bind binds a non-numeric value to $1::int; the cast error surfaces at
	// Describe (which executes the query).
	frontend.Send(&pgproto3.Parse{Query: "SELECT $1::int"})
	frontend.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte("not_a_number")}, ParameterFormatCodes: []int16{0}})
	frontend.Send(&pgproto3.Describe{ObjectType: 'P'})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("extended flush: %v", err)
	}
	if _, sawError := receiveUntilReady(t, frontend, "bad parameter exchange"); !sawError {
		t.Errorf("expected an ErrorResponse for the bad parameter")
	}

	terminateAndAwait(t, frontend, done)
}

// TestExtendedQueryStatementLifecycle drives the paths where prepared statements
// are now released (they previously leaked their DuckDB plan on every exchange):
// consecutive exchanges (deferred Close between them must not break the
// connection), then wire-protocol Close of the statement followed by a Describe
// on it (must error, not crash), then a final full exchange proving the
// connection still serves.
func TestExtendedQueryStatementLifecycle(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	fullExchange := func(step string) {
		frontend.Send(&pgproto3.Parse{Query: "SELECT 1"})
		frontend.Send(&pgproto3.Bind{})
		frontend.Send(&pgproto3.Describe{ObjectType: 'P'})
		frontend.Send(&pgproto3.Execute{})
		frontend.Send(&pgproto3.Sync{})
		if err := frontend.Flush(); err != nil {
			t.Fatalf("%s: flush: %v", step, err)
		}
		sawRows, sawError := receiveUntilReady(t, frontend, step)
		if !sawRows || sawError {
			t.Fatalf("%s: expected data rows and no error, got rows=%v error=%v", step, sawRows, sawError)
		}
	}

	// Two consecutive successful exchanges: the deferred statement release after
	// the first must not affect the second.
	fullExchange("first exchange")
	fullExchange("second exchange")

	// Wire-protocol Close('S') releases the statement mid-exchange; the following
	// Describe must be answered with an error — previously this shape could only
	// crash or silently leak. The connection must survive to the next exchange.
	frontend.Send(&pgproto3.Parse{Query: "SELECT 1"})
	frontend.Send(&pgproto3.Close{ObjectType: 'S'})
	frontend.Send(&pgproto3.Describe{ObjectType: 'S'})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("close exchange: flush: %v", err)
	}
	// writeError sends its own ReadyForQuery (pre-existing behavior) and Sync sends
	// another — drain both so the next exchange starts on a clean stream.
	_, sawErrorOnError := receiveUntilReady(t, frontend, "close exchange (error reply)")
	_, sawErrorOnSync := receiveUntilReady(t, frontend, "close exchange (sync reply)")
	if !sawErrorOnError && !sawErrorOnSync {
		t.Errorf("expected an error for Describe after wire-protocol Close('S')")
	}

	fullExchange("exchange after wire Close")

	terminateAndAwait(t, frontend, done)
}

// TestWireClosePortalKeepsStatementBindable pins Postgres Close semantics:
// Close('P') releases only the portal — the statement must remain bindable and
// executable afterwards (JDBC cursor clients close portals mid-exchange and
// Bind again). A regression here surfaced as "prepared statement was already
// closed" on the second Execute.
func TestWireClosePortalKeepsStatementBindable(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	frontend.Send(&pgproto3.Parse{Query: "SELECT 1"})
	frontend.Send(&pgproto3.Bind{})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Close{ObjectType: 'P'})
	frontend.Send(&pgproto3.Bind{})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("portal close exchange: flush: %v", err)
	}
	sawRows, sawError := receiveUntilReady(t, frontend, "portal close exchange")
	if !sawRows || sawError {
		t.Fatalf("expected rebind after Close('P') to succeed, got rows=%v error=%v", sawRows, sawError)
	}

	terminateAndAwait(t, frontend, done)
}
