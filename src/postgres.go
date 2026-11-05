package main

import (
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_VERSION        = "17.0"
	PG_ENCODING       = "UTF8"
	PG_TX_STATUS_IDLE = 'I'

	SYSTEM_AUTH_USER = "bemidb"
)

type Postgres struct {
	backend *pgproto3.Backend
	conn    *net.Conn
	config  *Config
}

func NewPostgres(config *Config, conn *net.Conn) *Postgres {
	return &Postgres{
		conn:    conn,
		backend: pgproto3.NewBackend(*conn, *conn),
		config:  config,
	}
}

func NewTcpListener(config *Config) net.Listener {
	parsedIp := net.ParseIP(config.Host)
	if parsedIp == nil {
		PrintErrorAndExit(config, "Invalid host: "+config.Host+".")
	}

	var network, host string
	if parsedIp.To4() == nil {
		network = "tcp6"
		host = "[" + config.Host + "]"
	} else {
		network = "tcp4"
		host = config.Host
	}

	tcpListener, err := net.Listen(network, host+":"+config.Port)
	PanicIfError(config, err)
	return tcpListener
}

func AcceptConnection(config *Config, listener net.Listener) net.Conn {
	conn, err := listener.Accept()
	PanicIfError(config, err)
	return conn
}

func (postgres *Postgres) Run(queryHandler *QueryHandler) {
	err := postgres.handleStartup()
	if err != nil {
		LogError(postgres.config, "Error handling startup:", err)
		return // Terminate connection
	}

	for {
		message, err := postgres.backend.Receive()
		if err != nil {
			return // Terminate connection
		}

		switch message := message.(type) {
		case *pgproto3.Query:
			postgres.handleSimpleQuery(queryHandler, message)
		case *pgproto3.Parse:
			err = postgres.handleExtendedQuery(queryHandler, message)
			if err != nil {
				return // Terminate connection
			}
		case *pgproto3.Terminate:
			LogDebug(postgres.config, "Client terminated connection")
			return
		default:
			LogError(postgres.config, "Received message other than Query from client:", message)
			return // Terminate connection
		}
	}
}

func (postgres *Postgres) Close() error {
	return (*postgres.conn).Close()
}

func (postgres *Postgres) handleSimpleQuery(queryHandler *QueryHandler, queryMessage *pgproto3.Query) {
	LogDebug(postgres.config, "Received query:", queryMessage.String)
	messages, err := queryHandler.HandleSimpleQuery(queryMessage.String)
	if err != nil {
		postgres.writeError(err)
		return
	}
	messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
	postgres.writeMessages(messages...)
}

func (postgres *Postgres) handleExtendedQuery(queryHandler *QueryHandler, parseMessage *pgproto3.Parse) error {
	LogDebug(postgres.config, "Parsing query", parseMessage.Query)
	messages, preparedStatement, err := queryHandler.HandleParseQuery(parseMessage)
	var previousErr error
	if err != nil {
		// Skip to Sync like any other error: returning would hand the rest of the
		// batch (Bind, Sync, ...) to Run, which drops the connection.
		postgres.writeErrorResponse(err)
		previousErr = err
	}
	postgres.writeMessages(messages...)

	// Release the statement's DuckDB-side plan on every exit from this exchange —
	// the Sync return, a dead connection, or a panic. Statements were previously
	// never closed anywhere (database/sql requires Stmt.Close), leaking each bound
	// plan into DuckDB's C++ memory for the life of the pooled connection.
	defer func() { preparedStatement.Close() }()

	for {
		message, err := postgres.backend.Receive()
		if err != nil {
			return err
		}

		// After an error, skip everything until Sync, as Postgres does (Terminate still
		// ends the connection). This also keeps the cases below from using the nil
		// statement that a Parse error leaves.
		_, isSync := message.(*pgproto3.Sync)
		_, isTerminate := message.(*pgproto3.Terminate)
		if previousErr != nil && !isSync && !isTerminate {
			continue
		}

		switch message := message.(type) {
		case *pgproto3.Bind:
			LogDebug(postgres.config, "Binding query", message.PreparedStatement)
			// Handlers return the input statement even on error, so ownership (and
			// the deferred Close) is never lost to a nil overwrite.
			messages, preparedStatement, err = queryHandler.HandleBindQuery(message, preparedStatement)
			if err != nil {
				postgres.writeErrorResponse(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Describe:
			LogDebug(postgres.config, "Describing query", message.Name, "("+string(message.ObjectType)+")")
			var messages []pgproto3.Message
			messages, preparedStatement, err = queryHandler.HandleDescribeQuery(message, preparedStatement)
			if err != nil {
				postgres.writeErrorResponse(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Execute:
			LogDebug(postgres.config, "Executing query", message.Portal)
			messages, err := queryHandler.HandleExecuteQuery(message, preparedStatement)
			if err != nil {
				postgres.writeErrorResponse(err)
				previousErr = err
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Close:
			LogDebug(postgres.config, "Closing", string(message.ObjectType), message.Name)
			// Postgres semantics: Close('S') releases the statement; Close('P')
			// releases only the portal — the statement must stay bindable (JDBC
			// cursor clients close portals mid-exchange and Bind again). Closing a
			// name that isn't the current one is a no-op answered with
			// CloseComplete, like Postgres closing an unknown statement — pgjdbc
			// statement-cache evictions interleave closes of OLD statements into
			// the current exchange. Statement release is idempotent with the
			// deferred Close; a later Execute, or Describe after Bind, on a closed
			// statement gets an error from prepare(), not a crash.
			if message.ObjectType == 'S' {
				if message.Name == preparedStatement.Name {
					preparedStatement.Close()
				}
			} else if message.Name == preparedStatement.Portal && preparedStatement.Rows != nil {
				preparedStatement.Rows.Close()
				preparedStatement.Rows = nil
			}
			postgres.writeMessages(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			LogDebug(postgres.config, "Syncing query")
			// Sync must always respond, even after an error. Release rows left open
			// by a Describe never followed by Execute: this Sync may not exit the
			// exchange (psycopg sends Parse->Sync->Bind->...), so rows can't wait
			// for the deferred Close. Handlers return the statement even on error,
			// so it's nil here only after a Parse error.
			if preparedStatement != nil && preparedStatement.Rows != nil {
				preparedStatement.Rows.Close()
				preparedStatement.Rows = nil
			}
			postgres.writeMessages(
				&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
			)

			// If there was an error or Parse->Bind->Sync (...) or Parse->Describe->Sync (e.g., Metabase)
			// it means that sync is the last message in the extended query protocol, we can exit handleExtendedQuery
			if previousErr != nil || preparedStatement.Bound || preparedStatement.Described {
				return nil
			}
			// Otherwise, wait for Bind/Describe/Execute/Sync.
			// For example, psycopg sends Parse->[extra Sync]->Bind->Describe->Execute->Sync
		case *pgproto3.Terminate:
			LogDebug(postgres.config, "Client terminated connection")
			return errors.New("client terminated connection")
		}
	}
}

func (postgres *Postgres) writeMessages(messages ...pgproto3.Message) {
	var buf []byte
	for _, message := range messages {
		buf, _ = message.Encode(buf)
	}
	(*postgres.conn).Write(buf)
}

// writeError ends a simple query (or a failed startup) with an ErrorResponse and a ReadyForQuery.
func (postgres *Postgres) writeError(err error) {
	postgres.writeErrorResponse(err)
	postgres.writeMessages(&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
}

// writeErrorResponse sends an ErrorResponse without a ReadyForQuery. Extended query
// protocol: after an error the server skips messages until Sync, which sends the only
// ReadyForQuery. Clients such as pgjdbc stop reading at the first ReadyForQuery, so a
// second one would be read as the reply to their next query.
func (postgres *Postgres) writeErrorResponse(err error) {
	LogError(postgres.config, err.Error())

	postgres.writeMessages(
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Message:  err.Error(),
		},
	)
}

func (postgres *Postgres) handleStartup() error {
	startupMessage, err := postgres.backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}

	switch startupMessage := startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.Parameters
		LogDebug(postgres.config, "BemiDB: startup message", params)

		if params["database"] != postgres.config.Database {
			postgres.writeError(errors.New("database " + params["database"] + " does not exist"))
			return errors.New("database does not exist")
		}

		if postgres.config.User != "" && params["user"] != postgres.config.User && params["user"] != SYSTEM_AUTH_USER {
			postgres.writeError(errors.New("role \"" + params["user"] + "\" does not exist"))
			return errors.New("role does not exist")
		}

		postgres.writeMessages(
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "client_encoding", Value: PG_ENCODING},
			&pgproto3.ParameterStatus{Name: "server_version", Value: PG_VERSION},
			&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
		)
		return nil
	case *pgproto3.SSLRequest:
		_, err = (*postgres.conn).Write([]byte("N"))
		if err != nil {
			return err
		}
		postgres.handleStartup()
		return nil
	default:
		return errors.New("unknown startup message")
	}
}
