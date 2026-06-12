package main

import (
	"regexp"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	BEMIDB_FUNCTION_LAST_SYNCED_AT = "bemidb_last_synced_at"
)

var PG_CATALOG_MACRO_FUNCTION_NAMES = Set[string]{}
var PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES = Set[string]{}

func CreatePgCatalogMacroQueries(config *Config) []string {
	result := []string{
		// Functions
		"CREATE MACRO aclexplode(aclitem_array) AS json(aclitem_array)",
		"CREATE MACRO current_setting(setting_name) AS '', (setting_name, missing_ok) AS ''",
		"CREATE MACRO pg_backend_pid() AS 0",
		"CREATE MACRO pg_encoding_to_char(encoding_int) AS 'UTF8'",
		"CREATE MACRO pg_get_expr(pg_node_tree, relation_oid) AS pg_catalog.pg_get_expr(pg_node_tree, relation_oid), (pg_node_tree, relation_oid, pretty_bool) AS pg_catalog.pg_get_expr(pg_node_tree, relation_oid)",
		"CREATE MACRO pg_get_function_identity_arguments(func_oid) AS ''",
		"CREATE MACRO pg_get_indexdef(index_oid) AS '', (index_oid, column_int) AS '', (index_oid, column_int, pretty_bool) AS ''",
		"CREATE MACRO pg_get_partkeydef(table_oid) AS ''",
		"CREATE MACRO pg_get_userbyid(role_id) AS 'bemidb'",
		"CREATE MACRO pg_get_viewdef(view_oid) AS pg_catalog.pg_get_viewdef(view_oid), (view_oid, pretty_bool) AS pg_catalog.pg_get_viewdef(view_oid)",
		"CREATE MACRO pg_indexes_size(regclass) AS 0",
		"CREATE MACRO pg_is_in_recovery() AS false",
		"CREATE MACRO pg_table_size(regclass) AS 0",
		"CREATE MACRO pg_tablespace_location(tablespace_oid) AS ''",
		"CREATE MACRO pg_total_relation_size(regclass) AS 0",
		"CREATE MACRO quote_ident(text) AS '\"' || text || '\"'",
		"CREATE MACRO row_to_json(record) AS to_json(record), (record, pretty_bool) AS to_json(record)",
		"CREATE MACRO set_config(setting_name, new_value, is_local) AS new_value",
		"CREATE MACRO version() AS 'PostgreSQL " + PG_VERSION + ", compiled by BemiDB'",
		"CREATE MACRO pg_get_statisticsobjdef_columns(oid) AS NULL",
		"CREATE MACRO pg_relation_is_publishable(val) AS NULL",
		`CREATE MACRO jsonb_extract_path_text(from_json, path_elems) AS
			CASE typeof(path_elems) LIKE '%[]'
			WHEN true THEN json_extract_path_text(from_json, path_elems)[1]::varchar
			ELSE json_extract_path_text(from_json, path_elems)::varchar
		END`,
		`CREATE MACRO json_build_object(k1, v1) AS json_object(k1, v1),
			(k1, v1, k2, v2) AS json_object(k1, v1, k2, v2),
			(k1, v1, k2, v2, k3, v3) AS json_object(k1, v1, k2, v2, k3, v3),
			(k1, v1, k2, v2, k3, v3, k4, v4) AS json_object(k1, v1, k2, v2, k3, v3, k4, v4)`,
		`CREATE MACRO array_upper(arr, dimension) AS
			CASE dimension
			WHEN 1 THEN len(arr)
			ELSE NULL
		END`,
		`CREATE MACRO array_lower(arr, dimension) AS
			CASE dimension
			WHEN 1 THEN CASE WHEN len(arr) > 0 THEN 1 ELSE NULL END
			ELSE NULL
		END`,

		// PostgreSQL compatibility aliases (functions missing in DuckDB)
		"CREATE MACRO char_length(s) AS length(s)",
		"CREATE MACRO character_length(s) AS length(s)",
		"CREATE MACRO btrim(s) AS trim(s), (s, chars) AS trim(s, chars)",
		"CREATE MACRO every(x) AS bool_and(x)",
		"CREATE MACRO json_typeof(j) AS json_type(j)",
		"CREATE MACRO jsonb_typeof(j) AS json_type(j)",
		"CREATE MACRO json_object_keys(j) AS json_keys(j)",
		"CREATE MACRO jsonb_object_keys(j) AS json_keys(j)",
		"CREATE MACRO json_agg(x) AS json_group_array(x)",
		"CREATE MACRO jsonb_agg(x) AS json_group_array(x)",
		"CREATE MACRO json_object_agg(k, v) AS json_group_object(k, v)",
		"CREATE MACRO jsonb_object_agg(k, v) AS json_group_object(k, v)",
		"CREATE MACRO div(a, b) AS trunc(a / b)",
		"CREATE MACRO clock_timestamp() AS now()",
		"CREATE MACRO timeofday() AS CAST(now() AS varchar)",
		`CREATE MACRO quote_literal(s) AS '''' || replace(s::varchar, '''', '''''') || ''''`,
		`CREATE MACRO quote_nullable(s) AS CASE WHEN s IS NULL THEN 'NULL' ELSE '''' || replace(s::varchar, '''', '''''') || '''' END`,
		"CREATE MACRO array_remove(arr, elem) AS list_filter(arr, x -> x IS DISTINCT FROM elem)",
		"CREATE MACRO array_replace(arr, from_elem, to_elem) AS list_transform(arr, x -> CASE WHEN x IS NOT DISTINCT FROM from_elem THEN to_elem ELSE x END)",
		"CREATE MACRO array_positions(arr, elem) AS list_filter(generate_series(1, len(arr)), i -> arr[i] IS NOT DISTINCT FROM elem)",
		// BemiDB-synced arrays are always one-dimensional
		"CREATE MACRO array_ndims(arr) AS CASE WHEN arr IS NULL THEN NULL ELSE 1 END",
		`CREATE MACRO array_dims(arr) AS '[1:' || len(arr)::varchar || ']'`,
		// Capitalizes space-separated words (PG also splits on other non-alpha chars)
		"CREATE MACRO initcap(s) AS array_to_string(list_transform(string_split(s, ' '), w -> upper(w[1]) || lower(w[2:])), ' ')",

		// Table functions
		"CREATE MACRO pg_is_in_recovery() AS TABLE SELECT false AS pg_is_in_recovery",
		`CREATE MACRO pg_show_all_settings() AS TABLE SELECT
			name,
			value AS setting,
			NULL::text AS unit,
			'Settings' AS category,
			description AS short_desc,
			NULL::text AS extra_desc,
			'user' AS context,
			input_type AS vartype,
			'default' AS source,
			NULL::int4 AS min_val,
			NULL::int4 AS max_val,
			NULL::text[] AS enumvals,
			value AS boot_val,
			value AS reset_val,
			NULL::text AS sourcefile,
			NULL::int4 AS sourceline,
			FALSE AS pending_restart
		FROM duckdb_settings()`,
		`CREATE MACRO pg_get_keywords() AS TABLE SELECT
			keyword_name AS word,
			'U' AS catcode,
			TRUE AS barelabel,
			keyword_category AS catdesc,
			'can be bare label' AS baredesc
		FROM duckdb_keywords()`,
	}
	PG_CATALOG_MACRO_FUNCTION_NAMES = extractMacroNames(result)
	return result
}

func CreateInformationSchemaMacroQueries(config *Config) []string {
	result := []string{
		"CREATE MACRO _pg_expandarray(arr) AS STRUCT_PACK(x := unnest(arr), n := unnest(generate_series(1, array_length(arr))))",
	}
	PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES = extractMacroNames(result)
	return result
}

var BUILTIN_DUCKDB_PG_FUNCTION_NAMES = NewSet([]string{
	"array_to_string",
	"generate_series",
})

type QueryRemapperFunction struct {
	parserFunction *ParserFunction
	icebergReader  *IcebergReader
	config         *Config
}

func NewQueryRemapperFunction(config *Config, icebergReader *IcebergReader) *QueryRemapperFunction {
	return &QueryRemapperFunction{
		parserFunction: NewParserFunction(config),
		icebergReader:  icebergReader,
		config:         config,
	}
}

func (remapper *QueryRemapperFunction) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	return remapper.parserFunction.SchemaFunction(functionCall)
}

// FUNCTION(...) -> ANOTHER_FUNCTION(...)
func (remapper *QueryRemapperFunction) RemapFunctionCall(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	schemaFunction := remapper.SchemaFunction(functionCall)

	// Pre-defined macro functions
	switch schemaFunction.Schema {

	// pg_catalog.func() -> main.func()
	case PG_SCHEMA_PG_CATALOG, "":
		if PG_CATALOG_MACRO_FUNCTION_NAMES.Contains(schemaFunction.Function) || BUILTIN_DUCKDB_PG_FUNCTION_NAMES.Contains(schemaFunction.Function) {
			remapper.parserFunction.RemapSchemaToMain(functionCall)
			return schemaFunction
		}

	// information_schema.func() -> main.func()
	case PG_SCHEMA_INFORMATION_SCHEMA:
		if PG_INFORMATION_SCHEMA_MACRO_FUNCTION_NAMES.Contains(schemaFunction.Function) {
			remapper.parserFunction.RemapSchemaToMain(functionCall)
			return schemaFunction
		}
	}

	switch {

	// format('%s %1$s', str) -> printf('%1$s %1$s', str)
	case schemaFunction.Function == PG_FUNCTION_FORMAT:
		remapper.parserFunction.RemapFormatToPrintf(functionCall)
		return schemaFunction

	// encode(sha256(...), 'hex') -> sha256(...), encode(x, 'hex') -> lower(hex(x))
	case schemaFunction.Function == PG_FUNCTION_ENCODE:
		remapper.parserFunction.RemapEncode(functionCall)
		return schemaFunction

	// decode(x, 'hex') -> unhex(x), decode(x, 'base64') -> from_base64(x)
	case schemaFunction.Function == PG_FUNCTION_DECODE:
		remapper.parserFunction.RemapDecode(functionCall)
		return schemaFunction

	// cardinality(array) -> len(array) (DuckDB's cardinality only works on MAPs)
	case schemaFunction.Function == PG_FUNCTION_CARDINALITY:
		remapper.parserFunction.RemapToFunction(functionCall, "len")
		return schemaFunction

	// bemidb_last_synced_at('schema.table') -> to_timestamp(internalTableMetadata.LastSyncedAt)
	case schemaFunction.Function == BEMIDB_FUNCTION_LAST_SYNCED_AT:
		schemaTableName := remapper.parserFunction.FirstArgumentToString(functionCall)
		schemaTableParts := strings.Split(schemaTableName, ".")
		var pgSchemaTable PgSchemaTable
		if len(schemaTableParts) == 2 {
			pgSchemaTable.Schema = schemaTableParts[0]
			pgSchemaTable.Table = schemaTableParts[1]
		} else {
			pgSchemaTable.Schema = PG_SCHEMA_PUBLIC
			pgSchemaTable.Table = schemaTableParts[0]
		}

		internalTableMetadata, err := remapper.icebergReader.InternalTableMetadata(pgSchemaTable)

		if err != nil {
			LogError(remapper.config, "Failed to get internal table metadata for %s: %v", pgSchemaTable, err)
			remapper.parserFunction.RemapToTimestamp(functionCall, 0)
		} else {
			remapper.parserFunction.RemapToTimestamp(functionCall, internalTableMetadata.LastSyncedAt)
		}

		return schemaFunction
	}

	return nil
}

func (remapper *QueryRemapperFunction) RemapNestedFunctionCalls(functionCall *pgQuery.FuncCall) {
	nestedFunctionCalls := remapper.parserFunction.NestedFunctionCalls(functionCall)
	if len(nestedFunctionCalls) == 0 {
		return
	}

	for _, nestedFunctionCall := range nestedFunctionCalls {
		if nestedFunctionCall == nil {
			continue
		}

		schemaFunction := remapper.RemapFunctionCall(nestedFunctionCall)
		if schemaFunction != nil {
			continue
		}

		remapper.RemapNestedFunctionCalls(nestedFunctionCall) // self-recursion
	}
}

func extractMacroNames(macros []string) Set[string] {
	names := make(Set[string])
	re := regexp.MustCompile(`CREATE MACRO (\w+)\(`)

	for _, macro := range macros {
		matches := re.FindStringSubmatch(macro)
		if len(matches) > 1 {
			names.Add(matches[1])
		}
	}

	return names
}
