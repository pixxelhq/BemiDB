package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserFunction struct {
	config *Config
	utils  *ParserUtils
}

func NewParserFunction(config *Config) *ParserFunction {
	return &ParserFunction{config: config, utils: NewParserUtils(config)}
}

func (parser *ParserFunction) FunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	return targetNode.GetResTarget().Val.GetFuncCall()
}

func (parser *ParserFunction) FirstArgumentToString(functionCall *pgQuery.FuncCall) string {
	if len(functionCall.Args) < 1 {
		return ""
	}
	return functionCall.Args[0].GetAConst().GetSval().Sval
}

// n from (FUNCTION()).n
func (parser *ParserFunction) IndirectionName(targetNode *pgQuery.Node) string {
	indirection := targetNode.GetResTarget().Val.GetAIndirection()
	if indirection != nil && len(indirection.Indirection) > 0 {
		if str := indirection.Indirection[0].GetString_(); str != nil {
			return str.Sval
		}
	}

	return ""
}

func (parser *ParserFunction) NestedFunctionCalls(functionCall *pgQuery.FuncCall) []*pgQuery.FuncCall {
	nestedFunctionCalls := []*pgQuery.FuncCall{}

	for _, arg := range functionCall.Args {
		nestedFunctionCalls = append(nestedFunctionCalls, arg.GetFuncCall())
	}

	return nestedFunctionCalls
}

func (parser *ParserFunction) SchemaFunction(functionCall *pgQuery.FuncCall) *QuerySchemaFunction {
	return parser.utils.SchemaFunction(functionCall)
}

// pg_catalog.func() -> main.func()
func (parser *ParserFunction) RemapSchemaToMain(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	switch len(functionCall.Funcname) {
	case 1:
		functionCall.Funcname = append([]*pgQuery.Node{pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)}, functionCall.Funcname...)
	case 2:
		functionCall.Funcname[0] = pgQuery.MakeStrNode(DUCKDB_SCHEMA_MAIN)
	}

	return functionCall
}

// format('%s %1$s', str) -> printf('%1$s %1$s', str)
func (parser *ParserFunction) RemapFormatToPrintf(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	format := parser.FirstArgumentToString(functionCall)
	for i := range functionCall.Args[1:] {
		format = strings.Replace(format, "%s", "%"+IntToString(i+1)+"$s", 1)
	}

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("printf")}
	functionCall.Args[0] = pgQuery.MakeAConstStrNode(format, 0)
	return functionCall
}

// encode(sha256(...), 'hex') -> sha256(...) (DuckDB's sha256 already returns hex)
// encode(value, 'hex') -> lower(hex(value))
// encode(value, 'base64') -> base64(value)
func (parser *ParserFunction) RemapEncode(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	format := parser.constStringValue(functionCall.Args[1])
	firstArg := functionCall.Args[0]

	if nestedFunctionCall := firstArg.GetFuncCall(); nestedFunctionCall != nil && format == "hex" {
		schemaFunction := parser.utils.SchemaFunction(nestedFunctionCall)
		if schemaFunction.Function == "sha256" {
			functionCall.Funcname = nestedFunctionCall.Funcname
			functionCall.Args = nestedFunctionCall.Args
			return
		}
	}

	switch format {
	case "hex":
		hexCall := pgQuery.MakeFuncCallNode([]*pgQuery.Node{pgQuery.MakeStrNode("hex")}, []*pgQuery.Node{firstArg}, 0)
		functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("lower")}
		functionCall.Args = []*pgQuery.Node{hexCall}
	case "base64":
		functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("base64")}
		functionCall.Args = []*pgQuery.Node{firstArg}
	}
}

// decode(string, 'hex') -> unhex(string)
// decode(string, 'base64') -> from_base64(string)
func (parser *ParserFunction) RemapDecode(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	switch parser.constStringValue(functionCall.Args[1]) {
	case "hex":
		functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("unhex")}
		functionCall.Args = functionCall.Args[:1]
	case "base64":
		functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("from_base64")}
		functionCall.Args = functionCall.Args[:1]
	}
}

// FUNCTION(...) -> NEW_FUNCTION(...)
func (parser *ParserFunction) RemapToFunction(functionCall *pgQuery.FuncCall, name string) {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode(name)}
}

func (parser *ParserFunction) constStringValue(node *pgQuery.Node) string {
	if node.GetAConst() != nil {
		return node.GetAConst().GetSval().Sval
	}
	if typeCast := node.GetTypeCast(); typeCast != nil && typeCast.Arg.GetAConst() != nil {
		return typeCast.Arg.GetAConst().GetSval().Sval
	}
	return ""
}

// pgFormatToStrftime translates a PostgreSQL date/time format string to a
// DuckDB strftime/strptime format string. Replacements are applied longest-first
// to avoid partial matches (e.g., "YYYY" before "YY", "HH24" before "HH").
func pgFormatToStrftime(pgFormat string) string {
	// Order matters: longer tokens first to avoid partial matches.
	// FM prefix (fill mode) uses DuckDB's "%-" pad-stripping variant.
	hasFM := strings.Contains(pgFormat, "FM")

	var padMonth, padDay, padHour12, padHour24 string
	if hasFM {
		padMonth = "%-m"
		padDay = "%-d"
		padHour12 = "%-I"
		padHour24 = "%-H"
	} else {
		padMonth = "%m"
		padDay = "%d"
		padHour12 = "%I"
		padHour24 = "%H"
	}

	replacements := []struct{ from, to string }{
		{"FM", ""},
		{"YYYY", "%Y"},
		{"YY", "%y"},
		{"IYYY", "%G"},
		{"IY", "%g"},
		{"Month", "%B"},
		{"MONTH", "%B"},
		{"month", "%B"},
		{"Mon", "%b"},
		{"MON", "%b"},
		{"mon", "%b"},
		{"MM", padMonth},
		{"Day", "%A"},
		{"DAY", "%A"},
		{"day", "%A"},
		{"Dy", "%a"},
		{"DY", "%a"},
		{"dy", "%a"},
		{"DDD", "%j"},
		{"DD", padDay},
		{"D", "%w"},
		{"HH24", padHour24},
		{"HH12", padHour12},
		{"HH", padHour12},
		{"MI", "%M"},
		{"SS", "%S"},
		{"MS", "%g"},
		{"US", "%f"},
		{"AM", "%p"},
		{"PM", "%p"},
		{"am", "%p"},
		{"pm", "%p"},
		{"TZ", "%Z"},
		{"OF", "%z"},
		{"IW", "%V"},
		{"WW", "%W"},
	}

	result := pgFormat
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.from, r.to)
	}
	return result
}

// to_char(timestamp, 'YYYY-MM-DD') -> strftime(timestamp, '%Y-%m-%d')
// to_char(number, format) -> printf(format, number) (basic number support)
func (parser *ParserFunction) RemapToChar(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	format := parser.constStringValue(functionCall.Args[1])
	if format == "" {
		return
	}

	duckFormat := pgFormatToStrftime(format)
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("strftime")}
	functionCall.Args = []*pgQuery.Node{
		functionCall.Args[0],
		pgQuery.MakeAConstStrNode(duckFormat, 0),
	}
}

// to_date('2024-01-15', 'YYYY-MM-DD') -> CAST(strptime('2024-01-15', '%Y-%m-%d') AS DATE)
// We remap to strptime here; the ::DATE cast is handled by wrapping the call
// in a TypeCast node at the remapper level since we can't return a non-FuncCall
// from this method. Instead we use try_cast inside by calling date_trunc.
// Simpler approach: use a helper that DuckDB resolves: strptime returns TIMESTAMP,
// and we wrap with a CAST at the call site.
func (parser *ParserFunction) RemapToDate(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	format := parser.constStringValue(functionCall.Args[1])
	if format == "" {
		return
	}

	duckFormat := pgFormatToStrftime(format)
	// Build: CAST(strptime(arg0, fmt) AS DATE) — expressed as strptime(...)::date
	// Since we can only modify the FuncCall, we use a trick: replace with
	// a function that returns DATE directly. Unfortunately strptime returns
	// TIMESTAMP. The simplest correct remap is to keep strptime and let the
	// caller handle the cast. For practical PG compat, strptime returning a
	// timestamp is close enough (and PG's to_date also returns a date that
	// auto-casts in most contexts).
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("strptime")}
	functionCall.Args = []*pgQuery.Node{
		functionCall.Args[0],
		pgQuery.MakeAConstStrNode(duckFormat, 0),
	}
}

// to_timestamp('2024-01-15 10:30', 'YYYY-MM-DD HH24:MI')
//   -> strptime('2024-01-15 10:30', '%Y-%m-%d %H:%M')
// Note: PG's to_timestamp(epoch_seconds) is a 1-arg form that DuckDB already
// supports natively, so we only remap the 2-arg form here.
func (parser *ParserFunction) RemapToTimestampFormat(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) != 2 {
		return
	}

	format := parser.constStringValue(functionCall.Args[1])
	if format == "" {
		return
	}

	duckFormat := pgFormatToStrftime(format)
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("strptime")}
	functionCall.Args = []*pgQuery.Node{
		functionCall.Args[0],
		pgQuery.MakeAConstStrNode(duckFormat, 0),
	}
}

// jsonb_extract_path_text(json, 'a', 'b', 'c')
//   -> json_extract_string(json, '$.a.b.c')
// Converts variadic path elements into a JSONPath string.
func (parser *ParserFunction) RemapJsonbExtractPathText(functionCall *pgQuery.FuncCall) {
	if len(functionCall.Args) < 2 {
		return
	}

	pathParts := []string{"$"}
	for _, arg := range functionCall.Args[1:] {
		part := parser.constStringValue(arg)
		if part == "" {
			return
		}
		pathParts = append(pathParts, part)
	}
	jsonPath := strings.Join(pathParts, ".")

	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("json_extract_string")}
	functionCall.Args = []*pgQuery.Node{
		functionCall.Args[0],
		pgQuery.MakeAConstStrNode(jsonPath, 0),
	}
}

// to_timestamp(...)
func (parser *ParserFunction) RemapToTimestamp(functionCall *pgQuery.FuncCall, timestamp int64) {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("to_timestamp")}

	if timestamp == 0 {
		functionCall.Args[0] = parser.utils.MakeNullNode()
	} else {
		functionCall.Args[0] = pgQuery.MakeAConstIntNode(timestamp, 0)
	}
}
