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

// to_timestamp(...)
func (parser *ParserFunction) RemapToTimestamp(functionCall *pgQuery.FuncCall, timestamp int64) {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("to_timestamp")}

	if timestamp == 0 {
		functionCall.Args[0] = parser.utils.MakeNullNode()
	} else {
		functionCall.Args[0] = pgQuery.MakeAConstIntNode(timestamp, 0)
	}
}
