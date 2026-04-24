package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type SyncerTable struct {
	config *Config
}

func NewSyncerTable(config *Config) *SyncerTable {
	return &SyncerTable{config: config}
}

func (syncer *SyncerTable) SyncPgTable(pgSchemaTable PgSchemaTable, structureConn *pgx.Conn, copyConn *pgx.Conn, existingInternalTableMetadata InternalTableMetadata, incrementalRefresh bool) error {
	continuedRefresh := existingInternalTableMetadata.MaxXmin != nil &&
		(incrementalRefresh || existingInternalTableMetadata.LastRefreshMode == RefreshModeFullInProgress)

	currentTxid, err := syncer.currentTxid(structureConn)
	if err != nil {
		return fmt.Errorf("structure connection lost: %w", err)
	}

	dynamicRowCountPerBatch, err := syncer.calculatedynamicRowCountPerBatch(pgSchemaTable, structureConn)
	if err != nil {
		return fmt.Errorf("structure connection lost: %w", err)
	}
	LogDebug(syncer.config, "Calculated row count per batch:", dynamicRowCountPerBatch, "Continued refresh:", continuedRefresh, "Incremental refresh:", incrementalRefresh)

	cappedBuffer := NewCappedBuffer(MAX_IN_MEMORY_BUFFER_SIZE, syncer.config)
	copyErrChan := make(chan error, 1)
	var waitGroup sync.WaitGroup

	waitGroup.Add(1)
	go func() {
		LogInfo(syncer.config, "Reading from Postgres:", pgSchemaTable.String()+"...")
		copySql := syncer.CopyFromPgTableSql(pgSchemaTable, existingInternalTableMetadata, currentTxid, continuedRefresh)
		copyErr := syncer.copyFromPgTable(copySql, copyConn, cappedBuffer, &waitGroup)
		if copyErr != nil {
			copyErrChan <- copyErr
		}
	}()

	stopPingChannel := make(chan struct{})
	waitGroup.Add(1)
	go func() {
		syncer.pingPg(structureConn, &stopPingChannel, &waitGroup)
	}()

	var lastTxid int64
	if existingInternalTableMetadata.LastRefreshMode == RefreshModeFullInProgress && existingInternalTableMetadata.LastTxid != 0 {
		lastTxid = existingInternalTableMetadata.LastTxid
	} else {
		lastTxid = currentTxid
	}

	csvReader := csv.NewReader(cappedBuffer)
	csvHeaders, err := csvReader.Read()
	if err != nil {
		close(stopPingChannel)
		select {
		case copyErr := <-copyErrChan:
			return copyErr
		default:
		}
		return fmt.Errorf("failed to read from copy stream: %w", err)
	}
	csvHeaders = csvHeaders[:len(csvHeaders)-1]

	pgSchemaColumns, err := syncer.pgTableSchemaColumns(structureConn, pgSchemaTable, csvHeaders)
	if err != nil {
		close(stopPingChannel)
		return fmt.Errorf("structure connection lost: %w", err)
	}

	icebergTableWriter := NewIcebergWriterTable(
		syncer.config,
		pgSchemaTable.ToIcebergSchemaTable(),
		pgSchemaColumns,
		dynamicRowCountPerBatch,
		MAX_PARQUET_PAYLOAD_THRESHOLD,
		continuedRefresh,
	)

	reachedEnd := false
	totalRowCount := 0
	var maxXmin uint32
	if existingInternalTableMetadata.MaxXmin != nil {
		maxXmin = *existingInternalTableMetadata.MaxXmin
	}

	LogInfo(syncer.config, "Writing to Iceberg...")
	icebergTableWriter.Write(func() ([][]string, InternalTableMetadata) {
		var newInternalTableMetadata InternalTableMetadata
		var rows [][]string

		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				reachedEnd = true
				break
			}
			if err != nil {
				PanicIfError(syncer.config, err)
			}

			maxXmin, err = StringToUint32(row[len(row)-1])
			PanicIfError(syncer.config, err)

			row = row[:len(row)-1] // Ignore the last column (xmin)
			rows = append(rows, row)

			if len(rows) >= dynamicRowCountPerBatch {
				break
			}
		}

		totalRowCount += len(rows)
		LogDebug(syncer.config, "Current total rows written to Parquet files:", totalRowCount, "...")
		runtime.GC() // To reduce Parquet Go memory leakage

		newInternalTableMetadata.LastSyncedAt = time.Now().Unix()
		newInternalTableMetadata.LastTxid = lastTxid
		if maxXmin != 0 {
			newInternalTableMetadata.MaxXmin = &maxXmin
		}
		if reachedEnd {
			if incrementalRefresh {
				newInternalTableMetadata.LastRefreshMode = RefreshModeIncremental
			} else {
				newInternalTableMetadata.LastRefreshMode = RefreshModeFull
			}
		} else {
			if incrementalRefresh {
				newInternalTableMetadata.LastRefreshMode = RefreshModeIncrementalInProgress
			} else {
				newInternalTableMetadata.LastRefreshMode = RefreshModeFullInProgress
			}
		}

		return rows, newInternalTableMetadata
	})

	close(stopPingChannel) // Stop the pingPg goroutine
	waitGroup.Wait()       // Wait for the Read goroutine to finish

	// Check for COPY errors (e.g., recovery conflict on hot standby)
	select {
	case copyErr := <-copyErrChan:
		return copyErr
	default:
		return nil
	}
}

func (syncer *SyncerTable) CopyFromPgTableSql(
	pgSchemaTable PgSchemaTable,
	existingInternalTableMetadata InternalTableMetadata,
	currentTxid int64,
	continuedRefresh bool,
) string {
	initialWraparoundTxid := PgWraparoundTxid(existingInternalTableMetadata.LastTxid)
	currentWraparoundTxid := PgWraparoundTxid(currentTxid)
	var previousMaxXmin int64
	if existingInternalTableMetadata.MaxXmin != nil {
		previousMaxXmin = int64(*existingInternalTableMetadata.MaxXmin)
	}

	if continuedRefresh {
		if previousMaxXmin <= currentWraparoundTxid {
			// When no wraparound occurred after an incremental or interrupted full sync
			//
			// [-----------------------|************************|************************|------------------------]
			// 0                 prev max xmin       init (wraparound) txid    curr (wraparound) txid           32^2
			//
			// [-----------------------|------------------------|************************|------------------------]
			// 0            init (wraparound) txid        prev max xmin        curr (wraparound) txid           32^2
			//
			// [-----------------------|************************|------------------------|------------------------]
			// 0                 prev max xmin        curr wraparound txid     init (wraparound) txid           32^2
			operator := ">"
			if existingInternalTableMetadata.IsInProgress() {
				operator = ">="
			}
			return "COPY (SELECT *, xmin::text::bigint AS xmin FROM " + pgSchemaTable.String() +
				" WHERE xmin::text::bigint " + operator + " " + existingInternalTableMetadata.MaxXminString() +
				" AND xmin::text::bigint <= " + Int64ToString(currentWraparoundTxid) +
				" ORDER BY xmin::text::bigint ASC)" +
				" TO STDOUT WITH CSV HEADER NULL '" + PG_NULL_STRING + "'"
		} else if IsPgWraparoundTxid(currentTxid) {
			// When a wraparound occurred after an incremental or interrupted full sync
			//
			// [***********************|------------------------|************************|************************]
			// 0             curr wraparound txid        prev max xmin        init (wraparound) txid            32^2
			//
			// [***********************|------------------------|------------------------|************************]
			// 0             curr wraparound txid     init (wraparound) txid      prev max xmin                 32^2
			//
			// [***********************|************************|------------------------|************************]
			// 0            init (wraparound) txid     curr wraparound txid       prev max xmin                 32^2
			operator := ">"
			if existingInternalTableMetadata.IsInProgress() {
				operator = ">="
			}
			return "COPY (SELECT *, xmin::text::bigint AS xmin FROM " + pgSchemaTable.String() +
				" WHERE xmin::text::bigint " + operator + " " + existingInternalTableMetadata.MaxXminString() +
				" OR xmin::text::bigint <= " + Int64ToString(currentWraparoundTxid) +
				" ORDER BY xmin::text::bigint <= " + Int64ToString(currentWraparoundTxid) + " ASC, xmin::text::bigint ASC)" + // Ordered by FALSE, then TRUE
				" TO STDOUT WITH CSV HEADER NULL '" + PG_NULL_STRING + "'"
		} else {
			Panic(syncer.config, "Unexpected case for the COPY SQL statement. Previous max xmin: "+
				Int64ToString(previousMaxXmin)+
				", initial wraparound txid: "+
				Int64ToString(initialWraparoundTxid)+
				", current wraparound txid: "+
				Int64ToString(currentWraparoundTxid))
			return ""
		}
	} else {
		// When a new full sync after a successful one or when missing the previous internal metadata (e.g., old BemiDB version)
		//
		// [**************************************************************************************************]
		// 0                                                                                           curr max xmin
		return "COPY (SELECT *, xmin::text::bigint AS xmin FROM " + pgSchemaTable.String() +
			" ORDER BY xmin::text::bigint ASC)" +
			" TO STDOUT WITH CSV HEADER NULL '" + PG_NULL_STRING + "'"
	}
}

func (syncer *SyncerTable) pgTableSchemaColumns(conn *pgx.Conn, pgSchemaTable PgSchemaTable, csvHeaders []string) ([]PgSchemaColumn, error) {
	if len(csvHeaders) == 0 {
		return nil, errors.New("couldn't read data from " + pgSchemaTable.String())
	}

	var pgSchemaColumns []PgSchemaColumn

	rows, err := conn.Query(
		context.Background(),
		`SELECT
			columns.column_name,
			columns.data_type,
			columns.udt_name,
			columns.is_nullable,
			columns.ordinal_position,
			COALESCE(columns.character_maximum_length, 0),
			COALESCE(columns.numeric_precision, 0),
			COALESCE(columns.numeric_scale, 0),
			COALESCE(columns.datetime_precision, 0),
			pg_namespace.nspname,
			CASE WHEN pk.constraint_name IS NOT NULL THEN true ELSE false END
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = columns.udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		LEFT JOIN (
			SELECT
				tc.constraint_name,
				kcu.column_name,
				kcu.table_schema,
				kcu.table_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
				AND tc.table_name = kcu.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
		) pk ON pk.column_name = columns.column_name AND pk.table_schema = columns.table_schema AND pk.table_name = columns.table_name
		WHERE columns.table_schema = $1 AND columns.table_name = $2
		ORDER BY array_position($3, columns.column_name)`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
		csvHeaders,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		pgSchemaColumn := NewPgSchemaColumn(syncer.config)
		err = rows.Scan(
			&pgSchemaColumn.ColumnName,
			&pgSchemaColumn.DataType,
			&pgSchemaColumn.UdtName,
			&pgSchemaColumn.IsNullable,
			&pgSchemaColumn.OrdinalPosition,
			&pgSchemaColumn.CharacterMaximumLength,
			&pgSchemaColumn.NumericPrecision,
			&pgSchemaColumn.NumericScale,
			&pgSchemaColumn.DatetimePrecision,
			&pgSchemaColumn.Namespace,
			&pgSchemaColumn.PartOfPrimaryKey,
		)
		if err != nil {
			return nil, err
		}
		pgSchemaColumns = append(pgSchemaColumns, *pgSchemaColumn)
	}

	return pgSchemaColumns, nil
}

func (syncer *SyncerTable) copyFromPgTable(copySql string, copyConn *pgx.Conn, cappedBuffer *CappedBuffer, waitGroup *sync.WaitGroup) error {
	LogDebug(syncer.config, copySql)
	result, err := copyConn.PgConn().CopyTo(context.Background(), cappedBuffer, copySql)
	cappedBuffer.Close()
	waitGroup.Done()
	if err != nil {
		return err
	}
	LogInfo(syncer.config, "Copied", result.RowsAffected(), "row(s)...")
	return nil
}

func (syncer *SyncerTable) currentTxid(conn *pgx.Conn) (int64, error) {
	var txid int64
	err := conn.QueryRow(context.Background(), `SELECT txid_snapshot_xmin(txid_current_snapshot())`).Scan(&txid)
	if err != nil {
		return 0, err
	}
	return txid, nil
}

func (syncer *SyncerTable) calculatedynamicRowCountPerBatch(pgSchemaTable PgSchemaTable, conn *pgx.Conn) (int, error) {
	var tableSize int64
	var rowCount int64

	err := conn.QueryRow(
		context.Background(),
		`
		SELECT
			pg_total_relation_size(c.oid) AS table_size,
			CASE
				WHEN c.reltuples >= 0 THEN c.reltuples::bigint
				ELSE (SELECT count(*) FROM `+pgSchemaTable.String()+`)
			END AS row_count
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
	).Scan(&tableSize, &rowCount)
	if err != nil {
		return 0, err
	}
	LogDebug(syncer.config, "Read table size:", tableSize, "Approximate row count:", rowCount)

	if tableSize == 0 || rowCount == 0 {
		return 1, nil
	}

	rowSize := tableSize / rowCount
	dynamicRowCountPerBatch := int(MAX_PG_ROWS_BATCH_SIZE / rowSize)
	if dynamicRowCountPerBatch == 0 {
		return 1, nil
	}

	return dynamicRowCountPerBatch, nil
}

func (syncer *SyncerTable) pingPg(conn *pgx.Conn, stopPingChannel *chan struct{}, waitGroup *sync.WaitGroup) {
	ticker := time.NewTicker(PING_PG_INTERVAL_SECONDS * time.Second)

	for {
		select {
		case <-*stopPingChannel:
			LogDebug(syncer.config, "Stopping the ping...")
			waitGroup.Done()
			ticker.Stop()
			return
		case <-ticker.C:
			LogDebug(syncer.config, "Pinging the database...")
			_, err := conn.Exec(context.Background(), "SELECT 1")
			if err != nil {
				LogWarn(syncer.config, "Ping failed (connection may have been terminated by recovery conflict):", err)
				waitGroup.Done()
				ticker.Stop()
				return
			}
		}
	}
}
