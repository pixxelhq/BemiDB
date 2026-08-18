package main

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	duckDb "github.com/marcboeker/go-duckdb"
)

const (
	DUCKDB_SCHEMA_MAIN                        = "main"
	REFRESH_IMPLICIT_AWS_CREDENTIALS_INTERVAL = 10 * time.Minute
)

// Instance-wide setup: extensions and catalog state live on the shared database
// instance, so these run once at boot on whichever connection serves it.
var DUCKDB_INIT_BOOT_QUERIES = []string{
	// Set up Iceberg
	"INSTALL iceberg",
	"LOAD iceberg",

	"INSTALL spatial",
	"LOAD spatial",

	// Set up schemas
	"SELECT oid FROM pg_catalog.pg_namespace",
}

// Session-scoped setup: SET timezone, SET scalar_subquery_error_on_multiple_rows,
// and USE apply to a single DuckDB connection, and the sql.DB pool creates and
// recycles connections over time (SetConnMaxLifetime). Applied once at boot they
// silently vanish on the first recycled connection — timezone flips to host-local,
// the current schema reverts to main, multi-row scalar subqueries start erroring —
// so they run in the driver's per-connection init hook instead. These SETs are
// deliberately fail-hard there: a session with the wrong timezone or subquery
// semantics is worse than a failed connection. USE public is handled separately
// in the hook (it needs the schema to exist; see connInitFn).
var DUCKDB_SESSION_INIT_QUERIES = []string{
	"SET scalar_subquery_error_on_multiple_rows=false",
	"SET timezone='UTC'",
}

type Duckdb struct {
	db                                    *sql.DB
	config                                *Config
	stopImplicitAwsCredentialsRefreshChan chan struct{}
}

// readDuckdbSetting returns the engine's current value for a setting, or
// "unavailable" — for confirmation logs that must report applied state, not
// configured intent.
func readDuckdbSetting(ctx context.Context, db *sql.DB, name string) string {
	var value string
	err := db.QueryRowContext(ctx, "SELECT value FROM duckdb_settings() WHERE name = '"+name+"'").Scan(&value)
	if err != nil {
		return "unavailable"
	}
	return value
}

func NewDuckdb(config *Config, withPgCompatibility bool) *Duckdb {
	ctx := context.Background()

	// The init hook runs on every connection the pool creates — the only way
	// session-scoped state survives connection recycling (see
	// DUCKDB_SESSION_INIT_QUERIES). Sync instances keep their current behavior:
	// they never ran session setup, so their hook stays nil.
	var connInitFn func(execer driver.ExecerContext) error
	if withPgCompatibility {
		connInitFn = func(execer driver.ExecerContext) error {
			for _, query := range DUCKDB_SESSION_INIT_QUERIES {
				if _, err := execer.ExecContext(ctx, query, nil); err != nil {
					return err
				}
			}
			// USE public needs the schema to exist, which on the very first
			// connection it doesn't. CREATE SCHEMA IF NOT EXISTS can still lose a
			// concurrent write-write race (proven with 16 parallel fresh
			// connections), so the create's error is ignored and the retried USE
			// is the arbiter. After boot this stays a single always-succeeding
			// statement per connection.
			if _, err := execer.ExecContext(ctx, "USE public", nil); err != nil {
				_, _ = execer.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS public", nil)
				if _, err := execer.ExecContext(ctx, "USE public", nil); err != nil {
					return err
				}
			}
			return nil
		}
	}
	connector, err := duckDb.NewConnector("", connInitFn)
	PanicIfError(config, err)
	db := sql.OpenDB(connector)

	// Recycle pooled DuckDB connections periodically. Measured on this engine:
	// jemalloc's retained (freed-but-held) memory pins to long-lived connections
	// and is only returned to the OS when they close. database/sql rotates a
	// connection strictly between uses — never mid-query, invisible to clients.
	if config.DuckDbConnMaxLifetimeMinutes > 0 {
		db.SetConnMaxLifetime(time.Duration(config.DuckDbConnMaxLifetimeMinutes) * time.Minute)
	}

	duckdb := &Duckdb{
		db:                                    db,
		config:                                config,
		stopImplicitAwsCredentialsRefreshChan: make(chan struct{}),
	}

	if withPgCompatibility {
		bootQueries := slices.Concat(
			// Set up DuckDB
			DUCKDB_INIT_BOOT_QUERIES,

			// Create pg-compatible functions
			CreatePgCatalogMacroQueries(config),
			CreateInformationSchemaMacroQueries(config),

			// Create pg-compatible tables and views
			CreatePgCatalogTableQueries(config),
			CreateInformationSchemaTableQueries(config),
		)

		// Pin a single pooled connection for the boot sequence. The pg-compat
		// macros/views below are created unqualified and must land in main, where
		// they have always lived (USE public used to run only after them; session
		// resolution finds them via the main fallback). The session hook has
		// already switched this connection to public, so flip it to main for
		// creation and hand it back on public, matching every other connection.
		conn, err := db.Conn(ctx)
		PanicIfError(config, err)
		_, err = conn.ExecContext(ctx, "USE main")
		PanicIfError(config, err)
		for _, query := range bootQueries {
			LogDebug(config, "Querying DuckDB:", query)
			_, err := conn.ExecContext(ctx, query)
			PanicIfError(config, err)
		}
		_, err = conn.ExecContext(ctx, "USE public")
		PanicIfError(config, err)
		PanicIfError(config, conn.Close())
	}

	// Return freed memory to the OS. DuckDB's bundled jemalloc retains freed
	// allocations indefinitely by default — the right trade for an embedded
	// notebook, but on a long-lived server the retained pages accumulate with
	// every heavy query until the kernel OOM-kills the pod (measured: a scan
	// workload held 1.2GiB at idle with the defaults, 73MiB with these two).
	// Background threads purge asynchronously; the lower flush threshold
	// (default 128MB) extends the post-task flush to mid-size queries.
	// Fail-soft: these are GLOBAL-scope optimizations, not correctness — a bad
	// value or an engine that drops the setting must not turn the OOM fix into
	// a boot crash. (No-ops on builds without jemalloc, e.g. macOS.)
	if config.DuckDbAllocatorBackgroundThreads {
		if _, err := duckdb.ExecContext(ctx, "SET allocator_background_threads=true", nil); err != nil {
			LogError(config, "DuckDB: failed to enable allocator background threads:", err)
		}
	}
	if config.DuckDbAllocatorFlushThreshold != "" {
		if _, err := duckdb.ExecContext(ctx, "SET allocator_flush_threshold='"+config.DuckDbAllocatorFlushThreshold+"'", nil); err != nil {
			LogError(config, "DuckDB: failed to set allocator_flush_threshold:", err)
		}
	}
	// Log the values the engine actually holds, not the configured intent — a
	// rejected SET above must not produce an INFO line claiming it was applied.
	LogInfo(config, "DuckDB: allocator background_threads:", readDuckdbSetting(ctx, db, "allocator_background_threads"),
		"flush_threshold:", readDuckdbSetting(ctx, db, "allocator_flush_threshold"))

	// Point DuckDB at a temp directory so larger-than-memory operations (joins,
	// aggregates, sorts, and the sync merge) spill to disk instead of erroring.
	if config.DuckDbTempDirectory != "" {
		_, err = duckdb.ExecContext(ctx, "SET temp_directory='"+config.DuckDbTempDirectory+"'", nil)
		PanicIfError(config, err)
	}

	// Bound DuckDB memory. In-memory DuckDB defaults to ~80% of host RAM, which in a
	// container ignores the cgroup limit and gets OOM-killed. With a limit set (below the
	// container limit) DuckDB stays bounded and spills to temp_directory.
	if config.DuckDbMemoryLimit != "" {
		_, err = duckdb.ExecContext(ctx, "SET memory_limit='"+config.DuckDbMemoryLimit+"'", nil)
		PanicIfError(config, err)
		LogInfo(config, "DuckDB: memory_limit set to", config.DuckDbMemoryLimit, "(temp_directory:", config.DuckDbTempDirectory+")")
	}

	// Cap DuckDB parallelism. Threads default to all host cores — the node's, not the
	// container's, since cgroup CPU limits are ignored — and peak scan memory grows with
	// every worker holding its own decompressed row-group chunks.
	if config.DuckDbThreads > 0 {
		_, err = duckdb.ExecContext(ctx, "SET threads="+IntToString(config.DuckDbThreads), nil)
		PanicIfError(config, err)
		LogInfo(config, "DuckDB: threads set to", config.DuckDbThreads)
	}

	if config.EnableCache {
		_, err = duckdb.ExecContext(ctx, "SET enable_object_cache=true", nil)
		PanicIfError(config, err)
		LogInfo(config, "DuckDB: Object cache enabled")
	}

	if config.EnableHttpConnectionCache {
		_, err = duckdb.ExecContext(ctx, "SET httpfs_connection_caching=true", nil)
		PanicIfError(config, err)
		LogInfo(config, "DuckDB: HTTPFS connection caching enabled")
	}

	switch config.StorageType {
	case STORAGE_TYPE_S3:
		if duckdb.config.Aws.AccessKeyId != "" && duckdb.config.Aws.SecretAccessKey != "" {
			duckdb.setExplicitAwsCredentials(ctx)
		} else {
			duckdb.setImplicitAwsCredentials(ctx)
			duckdb.autoRefreshImplicitAwsCredentials(ctx)
		}

		if IsLocalHost(config.Aws.S3Endpoint) {
			_, err = duckdb.ExecContext(ctx, "SET s3_use_ssl=false", nil)
			PanicIfError(config, err)
		}

		if config.Aws.S3Endpoint != "" && config.Aws.S3Endpoint != DEFAULT_AWS_S3_ENDPOINT {
			// Use endpoint/bucket/key (path, deprecated on AWS) instead of bucket.endpoint/key (vhost)
			_, err = duckdb.ExecContext(ctx, "SET s3_url_style='path'", nil)
			PanicIfError(config, err)
		}

		if config.LogLevel == LOG_LEVEL_TRACE {
			_, err = duckdb.ExecContext(ctx, "SET enable_http_logging=true", nil)
			PanicIfError(config, err)
		}
	}

	return duckdb
}

func (duckdb *Duckdb) ExecContext(ctx context.Context, query string, args map[string]string) (sql.Result, error) {
	LogDebug(duckdb.config, "Querying DuckDB:", query)
	return duckdb.db.ExecContext(ctx, replaceNamedStringArgs(query, args))
}

func (duckdb *Duckdb) QueryContext(ctx context.Context, query string) (*sql.Rows, error) {
	LogDebug(duckdb.config, "Querying DuckDB:", query)
	return duckdb.db.QueryContext(ctx, query)
}

func (duckdb *Duckdb) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	LogDebug(duckdb.config, "Preparing DuckDB statement:", query)
	return duckdb.db.PrepareContext(ctx, query)
}

func (duckdb *Duckdb) Close() {
	close(duckdb.stopImplicitAwsCredentialsRefreshChan)
	duckdb.db.Close()
}

func (duckdb *Duckdb) ExecTransactionContext(ctx context.Context, queries []string) error {
	tx, err := duckdb.db.Begin()
	LogDebug(duckdb.config, "Querying DuckDB: BEGIN")
	if err != nil {
		return err
	}

	for _, query := range queries {
		LogDebug(duckdb.config, "Querying DuckDB:", query)
		_, err := tx.ExecContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	LogDebug(duckdb.config, "Querying DuckDB: COMMIT")
	return tx.Commit()
}

func (duckdb *Duckdb) ExecFile(reader io.ReadCloser) {
	defer reader.Close()

	lines := []string{}
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	PanicIfError(duckdb.config, scanner.Err())

	ctx := context.Background()
	for _, sql := range lines {
		_, err := duckdb.ExecContext(ctx, sql, nil)
		PanicIfError(duckdb.config, err)
	}
}

func (duckdb *Duckdb) setExplicitAwsCredentials(ctx context.Context) {
	config := duckdb.config
	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, KEY_ID '$accessKeyId', SECRET '$secretAccessKey', REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := duckdb.ExecContext(ctx, query, map[string]string{
		"accessKeyId":     config.Aws.AccessKeyId,
		"secretAccessKey": config.Aws.SecretAccessKey,
		"region":          config.Aws.Region,
		"endpoint":        config.Aws.S3Endpoint,
		"s3Bucket":        "s3://" + config.Aws.S3Bucket,
	})
	PanicIfError(config, err)
}

func (duckdb *Duckdb) setImplicitAwsCredentials(ctx context.Context) {
	config := duckdb.config
	query := "CREATE OR REPLACE SECRET aws_s3_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION '$region', ENDPOINT '$endpoint', SCOPE '$s3Bucket')"
	_, err := duckdb.ExecContext(ctx, query, map[string]string{
		"region":   config.Aws.Region,
		"endpoint": config.Aws.S3Endpoint,
		"s3Bucket": "s3://" + config.Aws.S3Bucket,
	})
	PanicIfError(config, err)
}

func (duckdb *Duckdb) autoRefreshImplicitAwsCredentials(ctx context.Context) {
	ticker := time.NewTicker(REFRESH_IMPLICIT_AWS_CREDENTIALS_INTERVAL)
	go func() {
		for {
			select {
			case <-ticker.C:
				duckdb.setImplicitAwsCredentials(ctx)
			case <-duckdb.stopImplicitAwsCredentialsRefreshChan:
				ticker.Stop()
				return
			}
		}
	}()
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	re := regexp.MustCompile(`['";]`) // Escape single quotes, double quotes, and semicolons from args

	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, re.ReplaceAllString(value, ""))
	}
	return query
}
