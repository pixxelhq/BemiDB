package main

import (
	"net"
	"reflect"
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

// expectReplies drains replies until ReadyForQuery and fails unless their types
// match expected, in order.
func expectReplies(t *testing.T, frontend *pgproto3.Frontend, step string, expected []pgproto3.BackendMessage) {
	t.Helper()
	var got, want []string
	for _, msg := range expected {
		want = append(want, reflect.TypeOf(msg).Elem().Name())
	}
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("%s: receive error after %v (server likely crashed): %v", step, got, err)
		}
		got = append(got, reflect.TypeOf(msg).Elem().Name())
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: expected replies %v, got %v", step, want, got)
	}
}

// expectFullExchange runs Parse->Bind->Describe->Execute->Sync for SELECT 1 and
// fails unless it returns rows without an error: the connection is alive and its
// replies are in step (a leftover ReadyForQuery would end this exchange early).
func expectFullExchange(t *testing.T, frontend *pgproto3.Frontend, step string) {
	t.Helper()
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
// connection), then wire-protocol Close of the statement followed by Execute
// on it (must error, not crash), then a final full exchange proving the
// connection still serves.
func TestExtendedQueryStatementLifecycle(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	// Two consecutive successful exchanges: the deferred statement release after
	// the first must not affect the second.
	expectFullExchange(t, frontend, "first exchange")
	expectFullExchange(t, frontend, "second exchange")

	// Wire-protocol Close('S') releases the statement mid-exchange; the following
	// Execute must be answered with an error — previously this shape could only
	// crash or silently leak. The connection must survive to the next exchange.
	frontend.Send(&pgproto3.Parse{Query: "SELECT 1"})
	frontend.Send(&pgproto3.Close{ObjectType: 'S'})
	frontend.Send(&pgproto3.Describe{ObjectType: 'S'})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("close exchange: flush: %v", err)
	}
	if _, sawError := receiveUntilReady(t, frontend, "close exchange"); !sawError {
		t.Errorf("expected an error for Execute after wire-protocol Close('S')")
	}

	expectFullExchange(t, frontend, "exchange after wire Close")

	terminateAndAwait(t, frontend, done)
}

// TestExtendedQueryErrorRepliesKeepConnectionInSync pins the extended-protocol
// error rule for an error at each step: one ErrorResponse, then every message up
// to Sync is skipped, and Sync sends the only ReadyForQuery. A second
// ReadyForQuery is read by clients (e.g., pgjdbc) as the reply to their next
// query, and closing the connection on a Parse error drops it. The full exchange
// after each failing batch catches both.
func TestExtendedQueryErrorRepliesKeepConnectionInSync(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	badParameter := [][]byte{[]byte("not_a_number")}
	errorAfterBind := []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}, &pgproto3.ErrorResponse{}, &pgproto3.ReadyForQuery{}}
	failingBatches := []struct {
		step     string
		messages []pgproto3.FrontendMessage
		replies  []pgproto3.BackendMessage
	}{
		{
			step:     "Parse error",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELEC 1"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Close{ObjectType: 'S'}, &pgproto3.Sync{}},
			replies:  []pgproto3.BackendMessage{&pgproto3.ErrorResponse{}, &pgproto3.ReadyForQuery{}},
		},
		{
			step:     "Bind error",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT 1"}, &pgproto3.Bind{PreparedStatement: "unknown"}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}},
			replies:  []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.ErrorResponse{}, &pgproto3.ReadyForQuery{}},
		},
		{
			step:     "planning error at Describe",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT * FROM non_existent_table"}, &pgproto3.Bind{}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}},
			replies:  errorAfterBind,
		},
		{
			step:     "execution error at Describe",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT $1::int"}, &pgproto3.Bind{Parameters: badParameter}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{}},
			replies:  errorAfterBind,
		},
		{
			step:     "planning error at Execute",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT * FROM non_existent_table"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}},
			replies:  errorAfterBind,
		},
		{
			step:     "execution error at Execute",
			messages: []pgproto3.FrontendMessage{&pgproto3.Parse{Query: "SELECT $1::int"}, &pgproto3.Bind{Parameters: badParameter}, &pgproto3.Execute{}, &pgproto3.Sync{}},
			replies:  errorAfterBind,
		},
	}
	for _, batch := range failingBatches {
		for _, message := range batch.messages {
			frontend.Send(message)
		}
		if err := frontend.Flush(); err != nil {
			t.Fatalf("%s: flush: %v", batch.step, err)
		}
		expectReplies(t, frontend, batch.step, batch.replies)

		expectFullExchange(t, frontend, "exchange after "+batch.step)
	}

	terminateAndAwait(t, frontend, done)
}

// TestTerminateAfterErrorBeforeSync pins that Terminate still ends the connection
// while the server skips messages after an error, as in Postgres.
func TestTerminateAfterErrorBeforeSync(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	frontend.Send(&pgproto3.Parse{Query: "SELEC 1"})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	msg, err := frontend.Receive()
	if _, ok := msg.(*pgproto3.ErrorResponse); err != nil || !ok {
		t.Fatalf("expected an ErrorResponse for the Parse error, got %T (%v)", msg, err)
	}

	terminateAndAwait(t, frontend, done)
}

// TestDescribeOnlyRoundTrip drives the shape pgjdbc uses for getParameterMetaData():
// Parse->Describe(S)->Sync with no Bind or Execute. Nothing is planned until the
// statement runs, so even SQL that can't be planned (an unknown table) gets no
// error here, and the connection stays ready for the real run.
func TestDescribeOnlyRoundTrip(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	frontend.Send(&pgproto3.Parse{Query: "SELECT id FROM non_existent_table WHERE id = $1"})
	frontend.Send(&pgproto3.Describe{ObjectType: 'S'})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("describe-only flush: %v", err)
	}
	expectReplies(t, frontend, "describe-only round trip", []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.NoData{}, &pgproto3.ReadyForQuery{}})

	expectFullExchange(t, frontend, "exchange after describe-only round trip")

	terminateAndAwait(t, frontend, done)
}

// TestWireCloseUnknownNameIsNoOp pins the name check: pgjdbc statement-cache
// evictions interleave Close('S', "old_name") into the current exchange —
// closing a name that isn't the current statement must be a CloseComplete
// no-op, not destroy the statement the client is actively using.
func TestWireCloseUnknownNameIsNoOp(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()
	frontend, done := startTestPostgresClient(t, queryHandler)

	frontend.Send(&pgproto3.Parse{Name: "S_2", Query: "SELECT 1"})
	frontend.Send(&pgproto3.Close{ObjectType: 'S', Name: "S_1"}) // evicting an older statement
	frontend.Send(&pgproto3.Bind{PreparedStatement: "S_2"})
	frontend.Send(&pgproto3.Describe{ObjectType: 'P'})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	sawRows, sawError := receiveUntilReady(t, frontend, "close unknown name")
	if !sawRows || sawError {
		t.Fatalf("expected S_2 to survive Close('S', \"S_1\"), got rows=%v error=%v", sawRows, sawError)
	}

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
