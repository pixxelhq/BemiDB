package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	MAX_IN_MEMORY_BUFFER_SIZE = 128 * 1024 * 1024 // 128 MB (expands to ~160 MB memory usage)
	MAX_PG_ROWS_BATCH_SIZE    = 1 * 1024 * 1024   // 1 MB
	PING_PG_INTERVAL_SECONDS  = 24

	MAX_PARQUET_PAYLOAD_THRESHOLD = 2 * 1024 * 1024 * 1024 // 2 GB (compressed to ~256 MB Parquet)
)

type Syncer struct {
	config        *Config
	icebergWriter *IcebergWriter
	icebergReader *IcebergReader
	syncerTable   *SyncerTable
}

func NewSyncer(config *Config) *Syncer {
	if config.Pg.DatabaseUrl == "" {
		PrintErrorAndExit(config, "Missing PostgreSQL database URL.\n\n"+
			"See https://github.com/BemiHQ/BemiDB#sync-command-options for more information.",
		)
	}

	icebergWriter := NewIcebergWriter(config)
	icebergReader := NewIcebergReader(config)
	return &Syncer{
		config:        config,
		icebergWriter: icebergWriter,
		icebergReader: icebergReader,
		syncerTable:   NewSyncerTable(config),
	}
}

func (syncer *Syncer) SyncFromPostgres() {
	ctx := context.Background()
	if syncer.config.Pg.IncrementallyRefreshedTables == nil {
		syncer.sendAnonymousAnalytics("sync-start")
	} else {
		syncer.sendAnonymousAnalytics("sync-start-incremental")
	}

	databaseUrl := syncer.urlEncodePassword(syncer.config.Pg.DatabaseUrl)
	icebergSchemaTables, icebergSchemaTablesErr := syncer.icebergReader.SchemaTables()

	structureConn := syncer.newConnection(ctx, databaseUrl)
	snapshotID := syncer.exportSnapshot(ctx, structureConn)
	copyConn := syncer.newConnectionWithSnapshot(ctx, databaseUrl, snapshotID)

	type pendingTable struct {
		pgSchemaTable         PgSchemaTable
		internalTableMetadata InternalTableMetadata
		incrementalRefresh    bool
	}
	var pendingTables []pendingTable
	for _, schema := range syncer.listPgSchemas(structureConn) {
		for _, pgSchemaTable := range syncer.listPgSchemaTables(structureConn, schema) {
			if syncer.shouldSyncTable(pgSchemaTable) {
				var internalTableMetadata InternalTableMetadata
				syncedPreviously := icebergSchemaTablesErr == nil && icebergSchemaTables.Contains(pgSchemaTable.ToIcebergSchemaTable())
				if syncedPreviously {
					internalTableMetadata = syncer.readInternalTableMetadata(pgSchemaTable)
				}
				incrementalRefresh := syncer.config.Pg.IncrementallyRefreshedTables != nil && HasExactOrWildcardMatch(syncer.config.Pg.IncrementallyRefreshedTables, pgSchemaTable.ToConfigArg())
				pendingTables = append(pendingTables, pendingTable{pgSchemaTable, internalTableMetadata, incrementalRefresh})
			}
		}
	}

	syncedPgSchemaTables := []PgSchemaTable{}
	var failedTables []string
	for _, table := range pendingTables {
		if !syncer.isConnectionHealthy(ctx, structureConn) {
			LogWarn(syncer.config, "Structure connection lost, reconnecting...")
			structureConn.Close(ctx)
			structureConn = syncer.newConnection(ctx, databaseUrl)
			snapshotID = ""
		}
		if !syncer.isConnectionHealthy(ctx, copyConn) {
			LogWarn(syncer.config, "Copy connection lost, reconnecting...")
			copyConn.Close(ctx)
			copyConn = syncer.newConnectionWithSnapshot(ctx, databaseUrl, snapshotID)
		}

		newStructureConn, newSnapshotID, err := syncer.syncTableWithRetry(ctx, table.pgSchemaTable, structureConn, copyConn, databaseUrl, snapshotID, table.internalTableMetadata, table.incrementalRefresh)
		if newStructureConn != structureConn {
			structureConn.Close(ctx)
			structureConn = newStructureConn
		}
		snapshotID = newSnapshotID

		if err != nil {
			LogError(syncer.config, "Failed to sync table", table.pgSchemaTable.String()+":", err)
			failedTables = append(failedTables, table.pgSchemaTable.String())
			continue
		}
		LogInfo(syncer.config, "Finished writing to Iceberg\n")
		syncedPgSchemaTables = append(syncedPgSchemaTables, table.pgSchemaTable)
	}

	structureConn.Close(ctx)
	copyConn.Close(ctx)

	syncer.WriteInternalStartSqlFile(syncedPgSchemaTables)

	if !syncer.config.Pg.PreserveUnsynced {
		syncer.deleteOldIcebergSchemaTables(syncedPgSchemaTables)
	}

	if syncer.config.Pg.IncrementallyRefreshedTables == nil {
		syncer.sendAnonymousAnalytics("sync-finish")
	} else {
		syncer.sendAnonymousAnalytics("sync-finish-incremental")
	}

	if len(failedTables) > 0 {
		PanicIfError(syncer.config, fmt.Errorf("failed to sync %d table(s): %s", len(failedTables), strings.Join(failedTables, ", ")))
	}
}

const MAX_RECOVERY_CONFLICT_RETRIES = 3

// syncTableWithRetry wraps SyncPgTable with retry logic for hot standby
// recovery conflicts (SQLSTATE 40001), snapshot invalidation, and connection
// termination. Returns the (possibly reconnected) structureConn and snapshotID
// so the caller can use them for subsequent tables.
func (syncer *Syncer) syncTableWithRetry(ctx context.Context, pgSchemaTable PgSchemaTable, structureConn *pgx.Conn, copyConn *pgx.Conn, databaseUrl string, snapshotID string, internalTableMetadata InternalTableMetadata, incrementalRefresh bool) (*pgx.Conn, string, error) {
	currentCopyConn := copyConn
	currentStructureConn := structureConn
	currentSnapshotID := snapshotID

	for attempt := 0; attempt <= MAX_RECOVERY_CONFLICT_RETRIES; attempt++ {
		err := syncer.syncerTable.SyncPgTable(pgSchemaTable, currentStructureConn, currentCopyConn, internalTableMetadata, incrementalRefresh)
		if err == nil {
			return currentStructureConn, currentSnapshotID, nil
		}

		isRetryable := false
		errLower := strings.ToLower(err.Error())

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			isRetryable = true
		} else if strings.Contains(errLower, "conflict with recovery") {
			isRetryable = true
		}

		if strings.Contains(errLower, "snapshot") && (strings.Contains(errLower, "does not exist") || strings.Contains(errLower, "not found") || strings.Contains(errLower, "invalid")) {
			isRetryable = true
			currentSnapshotID = ""
		}

		if strings.Contains(errLower, "current transaction is aborted") {
			isRetryable = true
			currentSnapshotID = ""
		}

		// Connection terminated by standby WAL replay (FATAL) or network error
		if strings.Contains(errLower, "conn closed") || strings.Contains(errLower, "connection reset") ||
			strings.Contains(errLower, "broken pipe") || strings.Contains(errLower, "connection lost") ||
			strings.Contains(errLower, "structure connection lost") {
			isRetryable = true
			currentSnapshotID = ""
		}

		if isRetryable && attempt < MAX_RECOVERY_CONFLICT_RETRIES {
			LogWarn(syncer.config, "Recovery conflict syncing", pgSchemaTable.String()+",",
				"retrying (attempt", fmt.Sprintf("%d/%d)", attempt+2, MAX_RECOVERY_CONFLICT_RETRIES+1))

			if !syncer.isConnectionHealthy(ctx, currentStructureConn) {
				LogWarn(syncer.config, "Structure connection lost, reconnecting...")
				if currentStructureConn != structureConn {
					currentStructureConn.Close(ctx)
				}
				currentStructureConn = syncer.newConnection(ctx, databaseUrl)
				currentSnapshotID = ""
			}

			if currentCopyConn != copyConn {
				currentCopyConn.Close(ctx)
			}
			currentCopyConn = syncer.newConnectionWithSnapshot(ctx, databaseUrl, currentSnapshotID)
			continue
		}

		if currentCopyConn != copyConn {
			currentCopyConn.Close(ctx)
		}
		if currentStructureConn != structureConn {
			currentStructureConn.Close(ctx)
		}
		return structureConn, snapshotID, err
	}
	return currentStructureConn, currentSnapshotID, nil
}

func (syncer *Syncer) WriteInternalStartSqlFile(pgSchemaTables []PgSchemaTable) {
	childTablesByParentTable := make(map[string][]string)
	for _, pgSchemaTable := range pgSchemaTables {
		if pgSchemaTable.ParentPartitionedTable != "" {
			parent := pgSchemaTable.ParentPartitionedTableString()
			childTablesByParentTable[parent] = append(childTablesByParentTable[parent], pgSchemaTable.String())
		}
	}

	queryRemapper := NewQueryRemapper(syncer.config, syncer.icebergReader, nil)
	queries := []string{}

	for parent, children := range childTablesByParentTable {
		// CREATE OR REPLACE TABLE test_table AS
		//   SELECT * FROM iceberg_scan('/iceberg/public/test_table_q1/metadata/v1.metadata.json', skip_schema_inference = true)
		//   UNION ALL
		//   SELECT * FROM iceberg_scan('/iceberg/public/test_table_q2/metadata/v1.metadata.json', skip_schema_inference = true)

		subqueries := []string{}
		for _, child := range children {
			originalSubquery := fmt.Sprintf("SELECT * FROM %s", child)
			queryStatements, _, err := queryRemapper.ParseAndRemapQuery(originalSubquery)
			PanicIfError(syncer.config, err)
			subqueries = append(subqueries, queryStatements[0])
		}
		queries = append(queries, fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", parent, strings.Join(subqueries, " UNION ALL ")))
	}

	syncer.icebergWriter.WriteInternalStartSqlFile(queries)
}

// Example:
// - From postgres://username:pas$:wor^d@host:port/database
// - To postgres://username:pas%24%3Awor%5Ed@host:port/database
func (syncer *Syncer) urlEncodePassword(databaseUrl string) string {
	// No credentials
	if !strings.Contains(databaseUrl, "@") {
		return databaseUrl
	}

	password := strings.TrimPrefix(databaseUrl, "postgresql://")
	password = strings.TrimPrefix(password, "postgres://")
	passwordEndIndex := strings.LastIndex(password, "@")
	password = password[:passwordEndIndex]

	// Credentials without password
	if !strings.Contains(password, ":") {
		return databaseUrl
	}

	_, password, _ = strings.Cut(password, ":")
	decodedPassword, err := url.QueryUnescape(password)
	if err != nil {
		return databaseUrl
	}

	// Password is already encoded
	if decodedPassword != password {
		return databaseUrl
	}

	return strings.Replace(databaseUrl, ":"+password+"@", ":"+url.QueryEscape(password)+"@", 1)
}

func (syncer *Syncer) shouldSyncTable(pgSchemaTable PgSchemaTable) bool {
	if syncer.config.Pg.ExcludeTables != nil && HasExactOrWildcardMatch(syncer.config.Pg.ExcludeTables, pgSchemaTable.ToConfigArg()) {
		return false
	}

	if syncer.config.Pg.IncludeTables != nil {
		return HasExactOrWildcardMatch(syncer.config.Pg.IncludeTables, pgSchemaTable.ToConfigArg())
	}

	return true
}

func (syncer *Syncer) listPgSchemas(conn *pgx.Conn) []string {
	var schemas []string

	schemasRows, err := conn.Query(
		context.Background(),
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'pg_toast', 'information_schema')",
	)
	PanicIfError(syncer.config, err)
	defer schemasRows.Close()

	for schemasRows.Next() {
		var schema string
		err = schemasRows.Scan(&schema)
		PanicIfError(syncer.config, err)
		schemas = append(schemas, schema)
	}

	return schemas
}

func (syncer *Syncer) listPgSchemaTables(conn *pgx.Conn, schema string) []PgSchemaTable {
	var pgSchemaTables []PgSchemaTable

	tablesRows, err := conn.Query(
		context.Background(),
		`
		SELECT pg_class.relname AS table, COALESCE(parent.relname, '') AS parent_partitioned_table
		FROM pg_class
		JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		LEFT JOIN pg_inherits ON pg_inherits.inhrelid = pg_class.oid
		LEFT JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
		WHERE pg_namespace.nspname = $1 AND pg_class.relkind = 'r';
		`,
		schema,
	)
	PanicIfError(syncer.config, err)
	defer tablesRows.Close()

	for tablesRows.Next() {
		pgSchemaTable := PgSchemaTable{Schema: schema}
		err = tablesRows.Scan(&pgSchemaTable.Table, &pgSchemaTable.ParentPartitionedTable)
		PanicIfError(syncer.config, err)
		pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
	}

	return pgSchemaTables
}

func (syncer *Syncer) newConnection(ctx context.Context, databaseUrl string) *pgx.Conn {
	conn, err := pgx.Connect(ctx, databaseUrl)
	PanicIfError(syncer.config, err)

	_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "0A000" &&
			(strings.Contains(strings.ToLower(pgErr.Message), "hot standby") || strings.Contains(strings.ToLower(pgErr.Message), "serializable")) {
			// Hot standby does not support SERIALIZABLE isolation level; fall back to REPEATABLE READ
			LogWarn(syncer.config, "Hot standby detected, falling back to REPEATABLE READ isolation level")
			_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
		}
		PanicIfError(syncer.config, err)
	}

	return conn
}

func (syncer *Syncer) exportSnapshot(ctx context.Context, conn *pgx.Conn) string {
	var snapshotID string
	err := conn.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID)
	if err != nil {
		// pg_export_snapshot may not be available (e.g., some managed PG services)
		LogWarn(syncer.config, "Could not export snapshot:", err, "— copy connections will use independent snapshots")
		return ""
	}
	LogDebug(syncer.config, "Exported snapshot:", snapshotID)
	return snapshotID
}

func (syncer *Syncer) newConnectionWithSnapshot(ctx context.Context, databaseUrl string, snapshotID string) *pgx.Conn {
	if snapshotID == "" {
		return syncer.newConnection(ctx, databaseUrl)
	}

	// SET TRANSACTION SNAPSHOT requires REPEATABLE READ (not SERIALIZABLE DEFERRABLE)
	conn, err := pgx.Connect(ctx, databaseUrl)
	PanicIfError(syncer.config, err)

	_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
	PanicIfError(syncer.config, err)

	_, err = conn.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+snapshotID+"'")
	if err != nil {
		// Snapshot may have been invalidated (e.g., standby WAL replay cleaned it up).
		// The transaction is now in an error state — rollback and start a fresh one
		// without the snapshot so the connection remains usable.
		LogWarn(syncer.config, "Could not set transaction snapshot:", err, "— using independent snapshot")
		_, _ = conn.Exec(ctx, "ROLLBACK")
		_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
		PanicIfError(syncer.config, err)
	} else {
		LogDebug(syncer.config, "Set transaction snapshot:", snapshotID)
	}
	return conn
}

// isConnectionHealthy checks if a connection is alive AND its transaction is usable.
// Ping() only checks TCP liveness — an aborted transaction (SQLSTATE 25P02) passes
// Ping() but fails on any actual query.
func (syncer *Syncer) isConnectionHealthy(ctx context.Context, conn *pgx.Conn) bool {
	var result int
	err := conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	return err == nil
}

func (syncer *Syncer) readInternalTableMetadata(pgSchemaTable PgSchemaTable) InternalTableMetadata {
	internalTableMetadata, err := syncer.icebergReader.InternalTableMetadata(pgSchemaTable)
	PanicIfError(syncer.config, err)
	return internalTableMetadata
}

func (syncer *Syncer) deleteOldIcebergSchemaTables(pgSchemaTables []PgSchemaTable) {
	var prefixedPgSchemaTables []PgSchemaTable
	for _, pgSchemaTable := range pgSchemaTables {
		prefixedPgSchemaTables = append(
			prefixedPgSchemaTables,
			PgSchemaTable{Schema: syncer.config.Pg.SchemaPrefix + pgSchemaTable.Schema, Table: pgSchemaTable.Table},
		)
	}

	icebergSchemas, err := syncer.icebergReader.Schemas()
	PanicIfError(syncer.config, err)

	for _, icebergSchema := range icebergSchemas {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchema == pgSchemaTable.Schema {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchema, "...")
			err := syncer.icebergWriter.DeleteSchema(icebergSchema)
			PanicIfError(syncer.config, err)
		}
	}

	icebergSchemaTables, err := syncer.icebergReader.SchemaTables()
	PanicIfError(syncer.config, err)

	for _, icebergSchemaTable := range icebergSchemaTables.Values() {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchemaTable.String() == pgSchemaTable.String() {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchemaTable.String(), "...")
			err := syncer.icebergWriter.DeleteSchemaTable(icebergSchemaTable)
			PanicIfError(syncer.config, err)
		}
	}
}

type AnonymousAnalyticsData struct {
	Command string `json:"command"`
	OsName  string `json:"osName"`
	Version string `json:"version"`
	PgHost  string `json:"pgHost"`
}

func (syncer *Syncer) sendAnonymousAnalytics(command string) {
	if syncer.config.DisableAnonymousAnalytics {
		return
	}

	data := AnonymousAnalyticsData{
		Command: command,
		OsName:  runtime.GOOS + "-" + runtime.GOARCH,
		Version: VERSION,
		PgHost:  ParseDatabaseHost(syncer.config.Pg.DatabaseUrl),
	}
	if data.PgHost == "" || IsLocalHost(data.PgHost) {
		return
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("https://api.bemidb.com/api/analytics", "application/json", bytes.NewBuffer(jsonData))
}
