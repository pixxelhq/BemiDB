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
	// cleanly and Run returns (a crashed goroutine would never close done).
	frontend.Send(&pgproto3.Terminate{})
	_ = frontend.Flush()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run did not return after Terminate — connection loop wedged or panicked")
	}
}
