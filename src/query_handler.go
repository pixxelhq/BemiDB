package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	duckDb "github.com/marcboeker/go-duckdb"
)

const (
	FALLBACK_SQL_QUERY  = "SELECT 1"
	INSPECT_SQL_COMMENT = " --INSPECT"
)

type QueryHandler struct {
	duckdb        *Duckdb
	icebergReader *IcebergReader
	queryRemapper *QueryRemapper
	config        *Config
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type PreparedStatement struct {
	// Parse
	Name          string
	OriginalQuery string
	Query         string
	Statement     *sql.Stmt
	ParameterOIDs []uint32

	// Bind
	Bound     bool
	Variables []interface{}
	Portal    string

	// Describe
	Described bool

	// Describe/Execute
	Rows *sql.Rows
}

// Close releases the driver-side resources this statement pins. database/sql
// requires it ("The caller must call the statement's Close method when the
// statement is no longer needed"), and go-duckdb has no finalizer: an unclosed
// statement keeps its bound plan alive in DuckDB's C++ memory — invisible to
// both the Go heap and duckdb_memory() — for the life of its pooled connection.
// Rows close first so the driver doesn't defer the statement close behind an
// open result. Nil-safe and idempotent.
func (preparedStatement *PreparedStatement) Close() {
	if preparedStatement == nil {
		return
	}
	if preparedStatement.Rows != nil {
		preparedStatement.Rows.Close()
		preparedStatement.Rows = nil
	}
	if preparedStatement.Statement != nil {
		preparedStatement.Statement.Close()
		preparedStatement.Statement = nil
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullDecimal struct {
	Present bool
	Value   duckDb.Decimal
}

func (nullDecimal *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nullDecimal.Present = false
		return nil
	}

	nullDecimal.Present = true
	nullDecimal.Value = value.(duckDb.Decimal)
	return nil
}

func (nullDecimal NullDecimal) String() string {
	if nullDecimal.Present {
		return fmt.Sprintf("%v", nullDecimal.Value.Float64())
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullInterval struct {
	Present bool
	Value   duckDb.Interval
}

func (nullInterval *NullInterval) Scan(value interface{}) error {
	if value == nil {
		nullInterval.Present = false
		return nil
	}

	nullInterval.Present = true
	nullInterval.Value = value.(duckDb.Interval)
	return nil
}

func (nullInterval NullInterval) String() string {
	if nullInterval.Present {
		return fmt.Sprintf("%d months %d days %d microseconds", nullInterval.Value.Months, nullInterval.Value.Days, nullInterval.Value.Micros)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint32 struct {
	Present bool
	Value   uint32
}

func (nullUint32 *NullUint32) Scan(value interface{}) error {
	if value == nil {
		nullUint32.Present = false
		return nil
	}

	nullUint32.Present = true
	nullUint32.Value = value.(uint32)
	return nil
}

func (nullUint32 NullUint32) String() string {
	if nullUint32.Present {
		return fmt.Sprintf("%v", nullUint32.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint64 struct {
	Present bool
	Value   uint64
}

func (nullUint64 *NullUint64) Scan(value interface{}) error {
	if value == nil {
		nullUint64.Present = false
		return nil
	}

	nullUint64.Present = true
	nullUint64.Value = value.(uint64)
	return nil
}

func (nullUint64 NullUint64) String() string {
	if nullUint64.Present {
		return fmt.Sprintf("%v", nullUint64.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullBigInt struct {
	Present bool
	Value   *big.Int
}

func (nullBigInt *NullBigInt) Scan(value interface{}) error {
	if value == nil {
		nullBigInt.Present = false
		return nil
	}

	nullBigInt.Present = true
	nullBigInt.Value = value.(*big.Int)
	return nil
}

func (nullBigInt NullBigInt) String() string {
	if nullBigInt.Present {
		return fmt.Sprintf("%v", nullBigInt.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUuid struct {
	Present bool
	Value   []uint8
}

func (nullUuid *NullUuid) Scan(value interface{}) error {
	if value == nil {
		nullUuid.Present = false
		return nil
	}

	nullUuid.Present = true
	nullUuid.Value = value.([]uint8)
	return nil
}

func (nullUuid NullUuid) String() string {
	if nullUuid.Present {
		if len(nullUuid.Value) != 16 {
			// Not a valid UUID byte length — avoid slicing out of range
			return fmt.Sprintf("%x", nullUuid.Value)
		}
		uuidString := string(nullUuid.Value)
		return fmt.Sprintf("%x-%x-%x-%x-%x", uuidString[:4], uuidString[4:6], uuidString[6:8], uuidString[8:10], uuidString[10:])
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullArray struct {
	Present bool
	Value   []interface{}
}

func (nullArray *NullArray) Scan(value interface{}) error {
	if value == nil {
		nullArray.Present = false
		return nil
	}

	nullArray.Present = true
	nullArray.Value = value.([]interface{})
	return nil
}

func (nullArray NullArray) String() string {
	if nullArray.Present {
		var stringVals []string
		for _, v := range nullArray.Value {
			switch v.(type) {
			case nil:
				stringVals = append(stringVals, "NULL")
			case []uint8:
				stringVals = append(stringVals, fmt.Sprintf("%s", v))
			default:
				stringVals = append(stringVals, fmt.Sprintf("%v", v))
			}
		}
		buffer := &bytes.Buffer{}
		csvWriter := csv.NewWriter(buffer)
		err := csvWriter.Write(stringVals)
		if err != nil {
			return ""
		}
		csvWriter.Flush()
		return "{" + strings.TrimRight(buffer.String(), "\n") + "}"
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func NewQueryHandler(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *QueryHandler {
	queryHandler := &QueryHandler{
		duckdb:        duckdb,
		icebergReader: icebergReader,
		queryRemapper: NewQueryRemapper(config, icebergReader, duckdb),
		config:        config,
	}

	queryHandler.createSchemas()

	return queryHandler
}

func (queryHandler *QueryHandler) HandleSimpleQuery(originalQuery string) ([]pgproto3.Message, error) {
	queryStatements, originalQueryStatements, err := queryHandler.queryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, err
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	var queriesMessages []pgproto3.Message
	// One byte budget across ALL statements: they accumulate into a single buffer
	// below, so a per-statement budget would multiply the cap by the statement count.
	var resultBytes int64

	for i, queryStatement := range queryStatements {
		rows, err := queryHandler.duckdb.QueryContext(context.Background(), queryStatement)
		if err != nil {
			errorMessage := err.Error()
			if errorMessage == "Binder Error: UNNEST requires a single list as input" {
				// https://github.com/duckdb/duckdb/issues/11693
				LogWarn(queryHandler.config, "Couldn't handle query via DuckDB:", queryStatement+"\n"+err.Error())
				queriesMsgs, err := queryHandler.HandleSimpleQuery(FALLBACK_SQL_QUERY) // self-recursion
				if err != nil {
					return nil, err
				}
				queriesMessages = append(queriesMessages, queriesMsgs...)
				continue
			} else {
				return nil, err
			}
		}
		defer rows.Close()

		var queryMessages []pgproto3.Message
		descriptionMessages, err := queryHandler.rowsToDescriptionMessages(rows, originalQueryStatements[i])
		if err != nil {
			return nil, err
		}
		queryMessages = append(queryMessages, descriptionMessages...)
		if queryHandler.config.MaxResultMb > 0 {
			// Description messages accumulate into the same single-flush buffer as data
			// rows, so they count against the shared budget too.
			resultBytes += encodedMessagesBytes(descriptionMessages)
		}
		dataMessages, updatedResultBytes, err := queryHandler.rowsToDataMessages(rows, originalQueryStatements[i], resultBytes)
		if err != nil {
			return nil, err
		}
		resultBytes = updatedResultBytes
		queryMessages = append(queryMessages, dataMessages...)

		queriesMessages = append(queriesMessages, queryMessages...)
	}

	return queriesMessages, nil
}

func (queryHandler *QueryHandler) HandleParseQuery(message *pgproto3.Parse) ([]pgproto3.Message, *PreparedStatement, error) {
	ctx := context.Background()
	originalQuery := string(message.Query)
	queryStatements, _, err := queryHandler.queryRemapper.ParseAndRemapQuery(originalQuery)
	if err != nil {
		return nil, nil, err
	}
	if len(queryStatements) > 1 {
		return nil, nil, fmt.Errorf("multiple queries in a single parse message are not supported: %s", originalQuery)
	}

	preparedStatement := &PreparedStatement{
		Name:          message.Name,
		OriginalQuery: originalQuery,
		ParameterOIDs: message.ParameterOIDs,
	}
	if len(queryStatements) == 0 {
		return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
	}

	query := queryStatements[0]
	preparedStatement.Query = query
	statement, err := queryHandler.duckdb.PrepareContext(ctx, query)
	preparedStatement.Statement = statement
	if err != nil {
		return nil, nil, err
	}

	return []pgproto3.Message{&pgproto3.ParseComplete{}}, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleBindQuery(message *pgproto3.Bind, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	// Bind creates a new portal: results from a previous Bind/Describe of this
	// statement must not leak into it (a follow-up Execute would reuse them via
	// the Rows != nil branch). Error returns hand the statement back to the
	// caller, so anything still open stays reachable by the deferred Close.
	if preparedStatement.Rows != nil {
		preparedStatement.Rows.Close()
		preparedStatement.Rows = nil
	}

	if message.PreparedStatement != preparedStatement.Name {
		return nil, preparedStatement, fmt.Errorf("prepared statement mismatch, %s instead of %s: %s", message.PreparedStatement, preparedStatement.Name, preparedStatement.OriginalQuery)
	}

	var variables []interface{}
	paramFormatCodes := message.ParameterFormatCodes

	for i, param := range message.Parameters {
		if param == nil {
			continue
		}

		textFormat := true
		if len(paramFormatCodes) == 1 {
			textFormat = paramFormatCodes[0] == 0
		} else if len(paramFormatCodes) > 1 {
			textFormat = paramFormatCodes[i] == 0
		}

		if textFormat {
			val, err := castTextParam(string(param), preparedStatement.ParameterOIDs, i)
			if err != nil {
				return nil, preparedStatement, fmt.Errorf("failed to cast parameter %d: %w. Original query: %s", i, err, preparedStatement.OriginalQuery)
			}
			variables = append(variables, val)
		} else if len(param) == 4 {
			variables = append(variables, int32(binary.BigEndian.Uint32(param)))
		} else if len(param) == 8 {
			variables = append(variables, int64(binary.BigEndian.Uint64(param)))
		} else if len(param) == 16 {
			variables = append(variables, uuid.UUID(param).String())
		} else {
			return nil, preparedStatement, fmt.Errorf("unsupported parameter format: %v (length %d). Original query: %s", param, len(param), preparedStatement.OriginalQuery)
		}
	}

	LogDebug(queryHandler.config, "Bound variables:", variables)
	preparedStatement.Bound = true
	preparedStatement.Variables = variables
	preparedStatement.Portal = message.DestinationPortal

	messages := []pgproto3.Message{&pgproto3.BindComplete{}}

	return messages, preparedStatement, nil
}

var timestampParseFormats = []string{
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

func castTextParam(text string, parameterOIDs []uint32, index int) (interface{}, error) {
	if index >= len(parameterOIDs) {
		return text, nil
	}

	switch parameterOIDs[index] {
	case uint32(pgtype.TimestampOID), uint32(pgtype.TimestamptzOID):
		for _, format := range timestampParseFormats {
			if t, err := time.Parse(format, text); err == nil {
				return t, nil
			}
		}
		return text, nil
	case uint32(pgtype.DateOID):
		if t, err := time.Parse("2006-01-02", text); err == nil {
			return t, nil
		}
		return text, nil
	default:
		return text, nil
	}
}

func (queryHandler *QueryHandler) HandleDescribeQuery(message *pgproto3.Describe, preparedStatement *PreparedStatement) ([]pgproto3.Message, *PreparedStatement, error) {
	// A repeat Describe would otherwise leak the prior result handle until GC
	// (mirrors the reset in HandleBindQuery). Error returns hand the statement
	// back to the caller, so anything still open here stays reachable by the
	// caller's deferred Close.
	if preparedStatement.Rows != nil {
		preparedStatement.Rows.Close()
		preparedStatement.Rows = nil
	}

	switch message.ObjectType {
	case 'S': // Statement
		if message.Name != preparedStatement.Name {
			return nil, preparedStatement, fmt.Errorf("statement mismatch, %s instead of %s: %s", message.Name, preparedStatement.Name, preparedStatement.OriginalQuery)
		}
	case 'P': // Portal
		if message.Name != preparedStatement.Portal {
			return nil, preparedStatement, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Name, preparedStatement.Portal, preparedStatement.OriginalQuery)
		}
	}

	preparedStatement.Described = true
	if preparedStatement.Query == "" || !preparedStatement.Bound { // Empty query or Parse->[No Bind]->Describe
		return []pgproto3.Message{&pgproto3.NoData{}}, preparedStatement, nil
	}
	if preparedStatement.Statement == nil { // Statement released by a wire-protocol Close
		return nil, preparedStatement, fmt.Errorf("prepared statement was already closed: %s", preparedStatement.OriginalQuery)
	}

	rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
	if err != nil {
		return nil, preparedStatement, fmt.Errorf("couldn't execute statement: %w. Original query: %s", err, preparedStatement.OriginalQuery)
	}
	preparedStatement.Rows = rows

	messages, err := queryHandler.rowsToDescriptionMessages(preparedStatement.Rows, preparedStatement.OriginalQuery)
	if err != nil {
		return nil, preparedStatement, err
	}
	return messages, preparedStatement, nil
}

func (queryHandler *QueryHandler) HandleExecuteQuery(message *pgproto3.Execute, preparedStatement *PreparedStatement) ([]pgproto3.Message, error) {
	if message.Portal != preparedStatement.Portal {
		return nil, fmt.Errorf("portal mismatch, %s instead of %s: %s", message.Portal, preparedStatement.Portal, preparedStatement.OriginalQuery)
	}

	if preparedStatement.Query == "" {
		return []pgproto3.Message{&pgproto3.EmptyQueryResponse{}}, nil
	}

	if preparedStatement.Rows == nil { // Parse->[No Bind]->Describe->Execute or Parse->Bind->[No Describe]->Execute
		if preparedStatement.Statement == nil { // Statement released by a wire-protocol Close
			return nil, fmt.Errorf("prepared statement was already closed: %s", preparedStatement.OriginalQuery)
		}
		rows, err := preparedStatement.Statement.QueryContext(context.Background(), preparedStatement.Variables...)
		if err != nil {
			return nil, err
		}
		preparedStatement.Rows = rows
	}

	defer preparedStatement.Rows.Close()

	// Execute.MaxRows (sent by e.g. JDBC setMaxRows/setFetchSize) is deliberately
	// NOT honored: doing it correctly requires portal suspension that survives Sync,
	// which the one-exchange-per-Parse connection loop cannot support — emitting
	// PortalSuspended without that support kills cursor-mode clients on their resume.
	// Clients receive the full result and truncate client-side (pre-existing
	// behavior); oversized results are bounded server-side by --max-result-mb.
	messages, _, err := queryHandler.rowsToDataMessages(preparedStatement.Rows, preparedStatement.OriginalQuery, 0)
	return messages, err
}

func (queryHandler *QueryHandler) createSchemas() {
	ctx := context.Background()
	schemas, err := queryHandler.icebergReader.Schemas()
	PanicIfError(queryHandler.config, err)

	for _, schema := range schemas {
		_, err := queryHandler.duckdb.ExecContext(
			ctx,
			"CREATE SCHEMA IF NOT EXISTS \"$schema\"",
			map[string]string{"schema": schema},
		)
		PanicIfError(queryHandler.config, err)
	}
}

func (queryHandler *QueryHandler) rowsToDescriptionMessages(rows *sql.Rows, originalQuery string) ([]pgproto3.Message, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	var messages []pgproto3.Message

	rowDescription := queryHandler.generateRowDescription(cols)
	if rowDescription != nil {
		messages = append(messages, rowDescription)
	}

	return messages, nil
}

// encodedMessagesBytes returns the wire size of already-built messages, used to
// count RowDescriptions against the result budget. Called only for the few, small
// description messages per statement — not for data rows, which are measured
// inline in rowsToDataMessages.
func encodedMessagesBytes(messages []pgproto3.Message) int64 {
	var total int64
	for _, message := range messages {
		buf, err := message.Encode(nil)
		if err == nil {
			total += int64(len(buf))
		}
	}
	return total
}

// Estimated heap cost per buffered row beyond raw cell bytes: the DataRow struct,
// its [][]byte backing array (NULL cells included), one allocation per non-NULL
// cell, and the messages slice entry. Deliberate overestimates so the cap errs on
// the safe side; the true multiplier for narrow rows is what matters, not the
// exact constant.
const resultBytesPerRowOverhead = 64
const resultBytesPerCellOverhead = 48

// rowsToDataMessages buffers the whole result in memory before it is written to
// the connection, so it enforces the --max-result-mb byte cap (0 = disabled):
// this buffer is the process's largest unbounded allocation, and the pod's OOM
// killer is the only alternative enforcement. startResultBytes threads one shared
// budget across the statements of a simple-protocol query; the returned int64 is
// the updated total.
func (queryHandler *QueryHandler) rowsToDataMessages(rows *sql.Rows, originalQuery string, startResultBytes int64) ([]pgproto3.Message, int64, error) {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, startResultBytes, fmt.Errorf("couldn't get column types: %w. Original query: %s", err, originalQuery)
	}

	maxResultBytes := int64(queryHandler.config.MaxResultMb) * 1024 * 1024
	resultBytes := startResultBytes

	// Catch a result already over budget from prior statements' rows or from wide
	// RowDescriptions, even when this statement yields no rows (the per-row check
	// below would never run).
	if maxResultBytes > 0 && resultBytes > maxResultBytes {
		return nil, resultBytes, fmt.Errorf("result exceeded the %d MB limit (BEMIDB_MAX_RESULT_MB); add a LIMIT or select fewer columns. Original query: %s", queryHandler.config.MaxResultMb, originalQuery)
	}

	var messages []pgproto3.Message
	for rows.Next() {
		dataRow, err := queryHandler.generateDataRow(rows, cols)
		if err != nil {
			return nil, resultBytes, fmt.Errorf("couldn't get data row: %w. Original query: %s", err, originalQuery)
		}
		messages = append(messages, dataRow)

		if maxResultBytes > 0 {
			resultBytes += resultBytesPerRowOverhead + int64(len(dataRow.Values))*resultBytesPerCellOverhead
			for _, value := range dataRow.Values {
				resultBytes += int64(len(value))
			}
			if resultBytes > maxResultBytes {
				return nil, resultBytes, fmt.Errorf("result exceeded the %d MB limit (BEMIDB_MAX_RESULT_MB); add a LIMIT or select fewer columns. Original query: %s", queryHandler.config.MaxResultMb, originalQuery)
			}
		}
	}

	commandTag := FALLBACK_SQL_QUERY
	upperOriginalQueryStatement := strings.ToUpper(originalQuery)
	switch {
	case strings.HasPrefix(upperOriginalQueryStatement, "SET "):
		commandTag = "SET"
	case strings.HasPrefix(upperOriginalQueryStatement, "SHOW "):
		commandTag = "SHOW"
	case strings.HasPrefix(upperOriginalQueryStatement, "DISCARD ALL"):
		commandTag = "DISCARD ALL"
	case strings.HasPrefix(upperOriginalQueryStatement, "BEGIN"):
		commandTag = "BEGIN"
	}

	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
	return messages, resultBytes, nil
}

func (queryHandler *QueryHandler) generateRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	description := pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}}

	for _, col := range cols {
		typeIod := queryHandler.columnTypeOid(col)

		if col.Name() == "Success" && typeIod == pgtype.BoolOID && len(cols) == 1 {
			// Skip the "Success" DuckDB column returned from SET ... commands
			return nil
		}

		description.Fields = append(description.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          typeIod,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &description
}

// https://pkg.go.dev/github.com/jackc/pgx/v5/pgtype#pkg-constants
func (queryHandler *QueryHandler) columnTypeOid(col *sql.ColumnType) uint32 {
	switch col.DatabaseTypeName() {
	case "BOOLEAN":
		return pgtype.BoolOID
	case "BOOLEAN[]":
		return pgtype.BoolArrayOID
	case "TINYINT", "UTINYINT":
		return pgtype.Int2OID
	case "TINYINT[]", "UTINYINT[]":
		return pgtype.Int2ArrayOID
	case "SMALLINT":
		return pgtype.Int2OID
	case "SMALLINT[]":
		return pgtype.Int2ArrayOID
	case "USMALLINT":
		return pgtype.Int4OID
	case "USMALLINT[]":
		return pgtype.Int4ArrayOID
	case "INTEGER":
		return pgtype.Int4OID
	case "INTEGER[]":
		return pgtype.Int4ArrayOID
	case "UINTEGER":
		return pgtype.Int8OID
	case "UINTEGER[]":
		return pgtype.Int8ArrayOID
	case "BIGINT":
		if isSystemTableOidColumn(col.Name()) {
			return pgtype.OIDOID
		}
		return pgtype.Int8OID
	case "BIGINT[]":
		return pgtype.Int8ArrayOID
	case "UBIGINT":
		return pgtype.Int8OID
	case "UBIGINT[]":
		return pgtype.Int8ArrayOID
	case "HUGEINT":
		return pgtype.NumericOID
	case "HUGEINT[]":
		return pgtype.NumericArrayOID
	case "FLOAT":
		return pgtype.Float4OID
	case "FLOAT[]":
		return pgtype.Float4ArrayOID
	case "DOUBLE":
		return pgtype.Float8OID
	case "DOUBLE[]":
		return pgtype.Float8ArrayOID
	case "VARCHAR":
		return pgtype.TextOID
	case "VARCHAR[]":
		return pgtype.TextArrayOID
	case "TIME":
		return pgtype.TimeOID
	case "TIME[]":
		return pgtype.TimeArrayOID
	case "DATE":
		return pgtype.DateOID
	case "DATE[]":
		return pgtype.DateArrayOID
	case "TIMESTAMP":
		return pgtype.TimestampOID
	case "TIMESTAMP[]":
		return pgtype.TimestampArrayOID
	case "TIMESTAMPTZ":
		return pgtype.TimestamptzOID
	case "TIMESTAMPTZ[]":
		return pgtype.TimestamptzArrayOID
	case "BLOB":
		// Synced UUID columns come back from iceberg_scan as BLOB, so use text
		// (values serialize as uuid strings or \x-hex depending on content)
		return pgtype.TextOID
	case "BLOB[]":
		return pgtype.TextArrayOID
	case "UUID":
		return pgtype.UUIDOID
	case "UUID[]":
		return pgtype.UUIDArrayOID
	case "INTERVAL":
		return pgtype.IntervalOID
	case "INTERVAL[]":
		return pgtype.IntervalArrayOID
	default:
		if strings.HasPrefix(col.DatabaseTypeName(), "DECIMAL") {
			if strings.HasSuffix(col.DatabaseTypeName(), "[]") {
				return pgtype.NumericArrayOID
			} else {
				return pgtype.NumericOID
			}
		}

		// Unknown types (e.g., INET structs from autoloaded extensions) degrade to text
		// instead of panicking, which would kill the server for all connections
		LogWarn(queryHandler.config, "Unsupported serialized column type, falling back to text:", col.DatabaseTypeName())
		return pgtype.TextOID
	}
}

func isSystemTableOidColumn(colName string) bool {
	oidColumns := map[string]bool{
		"oid":          true,
		"tableoid":     true,
		"relnamespace": true,
		"relowner":     true,
		"relfilenode":  true,
		"did":          true,
		"objoid":       true,
		"classoid":     true,
	}

	return oidColumns[colName]
}

func (queryHandler *QueryHandler) generateDataRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	valuePtrs := make([]interface{}, len(cols))
	for i, col := range cols {
		switch col.ScanType().String() {
		case "int16":
			var value sql.NullInt16
			valuePtrs[i] = &value
		case "int32":
			var value sql.NullInt32
			valuePtrs[i] = &value
		case "int64":
			var value sql.NullInt64
			valuePtrs[i] = &value
		case "uint32": // xid
			var value NullUint32
			valuePtrs[i] = &value
		case "uint64": // xid8
			var value NullUint64
			valuePtrs[i] = &value
		case "float64", "float32":
			var value sql.NullFloat64
			valuePtrs[i] = &value
		case "string":
			var value sql.NullString
			valuePtrs[i] = &value
		case "[]uint8": // uuid or blob
			if col.DatabaseTypeName() == "UUID" {
				var value NullUuid
				valuePtrs[i] = &value
			} else {
				var value interface{}
				valuePtrs[i] = &value
			}
		case "bool":
			var value sql.NullBool
			valuePtrs[i] = &value
		case "time.Time":
			var value sql.NullTime
			valuePtrs[i] = &value
		case "*big.Int":
			var value NullBigInt
			valuePtrs[i] = &value
		case "duckdb.Decimal":
			var value NullDecimal
			valuePtrs[i] = &value
		case "duckdb.Interval":
			var value NullInterval
			valuePtrs[i] = &value
		case "[]interface {}":
			var value NullArray
			valuePtrs[i] = &value
		default:
			// Unknown scan types (e.g., int8/uint8/uint16 from TINYINT, structs from
			// extension types) scan generically and serialize via fmt instead of panicking
			var value interface{}
			valuePtrs[i] = &value
		}
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	var values [][]byte
	for i, valuePtr := range valuePtrs {
		switch value := valuePtr.(type) {
		case *sql.NullInt16:
			if value.Valid {
				values = append(values, []byte(IntToString(int(value.Int16))))
			} else {
				values = append(values, nil)
			}
		case *sql.NullInt32:
			if value.Valid {
				values = append(values, []byte(IntToString(int(value.Int32))))
			} else {
				values = append(values, nil)
			}
		case *sql.NullInt64:
			if value.Valid {
				values = append(values, []byte(IntToString(int(value.Int64))))
			} else {
				values = append(values, nil)
			}
		case *NullUint32:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullUint64:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *sql.NullFloat64:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Float64)))
			} else {
				values = append(values, nil)
			}
		case *sql.NullString:
			if value.Valid {
				values = append(values, []byte(value.String))
			} else {
				values = append(values, nil)
			}
		case *sql.NullBool:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Bool)[0:1]))
			} else {
				values = append(values, nil)
			}
		case *sql.NullTime:
			if value.Valid {
				switch cols[i].DatabaseTypeName() {
				case "DATE":
					values = append(values, []byte(value.Time.Format("2006-01-02")))
				case "TIME":
					values = append(values, []byte(value.Time.Format("15:04:05.999999")))
				case "TIMESTAMP":
					values = append(values, []byte(value.Time.Format("2006-01-02 15:04:05.999999")))
				case "TIMESTAMPTZ":
					values = append(values, []byte(value.Time.Format("2006-01-02 15:04:05.999999-07:00")))
				default:
					// Unknown time-like types (e.g., TIMETZ, TIMESTAMP_NS) degrade to a full
					// timestamp format instead of panicking
					LogWarn(queryHandler.config, "Unsupported scanned time type, using default format:", cols[i].DatabaseTypeName())
					values = append(values, []byte(value.Time.Format("2006-01-02 15:04:05.999999-07:00")))
				}
			} else {
				values = append(values, nil)
			}
		case *NullBigInt:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullDecimal:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullInterval:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullArray:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullUuid:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *string:
			values = append(values, []byte(*value))
		case *interface{}:
			if *value == nil {
				values = append(values, nil)
			} else if byteValue, ok := (*value).([]byte); ok {
				if len(byteValue) == 16 {
					// Synced UUID columns are stored as 16-byte blobs in Parquet
					values = append(values, []byte(NullUuid{Present: true, Value: byteValue}.String()))
				} else {
					// Other BLOB/bytea values use PostgreSQL's hex output format
					values = append(values, []byte("\\x"+hex.EncodeToString(byteValue)))
				}
			} else {
				values = append(values, []byte(fmt.Sprintf("%v", *value)))
			}
		default:
			Panic(queryHandler.config, "Unsupported scanned row type: "+cols[i].ScanType().Name())
		}
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
