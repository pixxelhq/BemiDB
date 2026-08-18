package main

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestRunDoesNotPanicOnErrorBeforeSync drives the exact extended-protocol
// sequence that used to crash the process: a parameterized query whose Bind/
// Describe fails leaves preparedStatement nil, and the following Sync — which
// must always respond — previously dereferenced it (SIGSEGV in the per-connection
// goroutine, killing the whole server for every client). The server must instead
// return an ErrorResponse and stay alive.
func TestRunDoesNotPanicOnErrorBeforeSync(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	// A real loopback socket (not net.Pipe): net.Pipe is unbuffered, so the server
	// writing a reply while the client is still flushing later messages deadlocks.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

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
	defer clientConn.Close()
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
			break
		}
	}

	// Bind binds a non-numeric value to $1::int; the cast error surfaces at
	// Describe (which executes the query), leaving preparedStatement nil.
	frontend.Send(&pgproto3.Parse{Query: "SELECT $1::int"})
	frontend.Send(&pgproto3.Bind{Parameters: [][]byte{[]byte("not_a_number")}, ParameterFormatCodes: []int16{0}})
	frontend.Send(&pgproto3.Describe{ObjectType: 'P'})
	frontend.Send(&pgproto3.Execute{})
	frontend.Send(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		t.Fatalf("extended flush: %v", err)
	}

	sawError := false
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("expected ErrorResponse then ReadyForQuery, got receive error (server likely crashed): %v", err)
		}
		if _, ok := msg.(*pgproto3.ErrorResponse); ok {
			sawError = true
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if !sawError {
		t.Errorf("expected an ErrorResponse for the bad parameter")
	}

	// The connection loop must still be alive and serving: a Terminate ends it
	// cleanly and Run returns. (A regression panic would crash the whole test
	// process outright; the timeout below guards the wedged-but-alive case.)
	frontend.Send(&pgproto3.Terminate{})
	_ = frontend.Flush()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after Terminate — connection loop wedged or panicked")
	}
}

// TestExtendedQueryStatementLifecycle drives the paths where prepared statements
// are now released (they previously leaked their DuckDB plan on every exchange):
// two full exchanges back-to-back (deferred Close between them must not break the
// connection), then a wire-protocol Close followed by a Describe on the released
// statement (must error, not crash), then a final full exchange proving the
// connection still serves.
func TestExtendedQueryStatementLifecycle(t *testing.T) {
	createTestTables(t)
	queryHandler := initQueryHandler()
	defer queryHandler.duckdb.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		serverConn, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(done)
			return
		}
		postgres := NewPostgres(queryHandler.config, &serverConn)
		postgres.Run(queryHandler)
		serverConn.Close()
		close(done)
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()
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
			break
		}
	}

	// receiveUntilReady drains one exchange's replies, reporting what was seen.
	receiveUntilReady := func(step string) (sawRows bool, sawError bool) {
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

	fullExchange := func(step string) {
		frontend.Send(&pgproto3.Parse{Query: "SELECT 1"})
		frontend.Send(&pgproto3.Bind{})
		frontend.Send(&pgproto3.Describe{ObjectType: 'P'})
		frontend.Send(&pgproto3.Execute{})
		frontend.Send(&pgproto3.Sync{})
		if err := frontend.Flush(); err != nil {
			t.Fatalf("%s: flush: %v", step, err)
		}
		sawRows, sawError := receiveUntilReady(step)
		if !sawRows || sawError {
			t.Fatalf("%s: expected data rows and no error, got rows=%v error=%v", step, sawRows, sawError)
		}
	}

	// Two consecutive successful exchanges: the deferred statement release after
	// the first must not affect the second.
	fullExchange("first exchange")
	fullExchange("second exchange")

	// Wire-protocol Close releases the statement mid-exchange; the following
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
	_, sawErrorOnError := receiveUntilReady("close exchange (error reply)")
	_, sawErrorOnSync := receiveUntilReady("close exchange (sync reply)")
	if !sawErrorOnError && !sawErrorOnSync {
		t.Errorf("expected an error for Describe after wire-protocol Close")
	}

	fullExchange("exchange after wire Close")

	frontend.Send(&pgproto3.Terminate{})
	_ = frontend.Flush()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after Terminate — connection loop wedged or panicked")
	}
}
