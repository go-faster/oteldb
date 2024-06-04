package chsql

import (
	"fmt"
	"strconv"

	"golang.org/x/exp/constraints"
)

type exprType uint8

const (
	exprIdent    exprType = iota + 1 // columns, tables, views names
	exprLiteral                      // numbers, strings, etc.
	exprUnaryOp                      // `NOT`, etc.
	exprBinaryOp                     // `+`,`-`,`IN`, etc.
	exprFunction                     // functions
	exprTuple
)

type expr struct {
	typ  exprType
	tok  string
	args []expr
}

func (expr) tableExpr() {}

// Ident returns identifier.
func Ident(tok string) expr {
	return expr{typ: exprIdent, tok: tok}
}

// Value returns literal.
func Value[V litValue](v V) expr {
	switch v := any(v).(type) {
	case string:
		return String(v)
	case bool:
		return Bool(v)
	case int:
		return Integer(v)
	case int8:
		return Integer(v)
	case int16:
		return Integer(v)
	case int32:
		return Integer(v)
	case int64:
		return Integer(v)
	case uint:
		return Integer(v)
	case uint8:
		return Integer(v)
	case uint16:
		return Integer(v)
	case uint32:
		return Integer(v)
	case uint64:
		return Integer(v)
	case float32:
		return Float(v)
	case float64:
		return Float(v)
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}
}

type litValue interface {
	string |
		bool |
		int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		float32 | float64
}

// String returns string literal.
func String(v string) expr {
	return expr{
		typ: exprLiteral,
		tok: singleQuoted(v),
	}
}

// Integer returns integer literal.
func Integer[I constraints.Integer](v I) expr {
	return expr{
		typ: exprLiteral,
		// FIXME(tdakkota): suboptimal
		tok: fmt.Sprintf("%d", v),
	}
}

// Float returns float literal.
func Float[I constraints.Float](v I) expr {
	size := 64
	if _, ok := any(v).(float32); ok {
		size = 32
	}
	return expr{
		typ: exprLiteral,
		tok: strconv.FormatFloat(float64(v), 'f', -1, size),
	}
}

// Bool returns bool literal.
func Bool(v bool) expr {
	return expr{
		typ: exprLiteral,
		tok: strconv.FormatBool(v),
	}
}

// Tuple returns tuple of given expressions.
func Tuple(args ...expr) expr {
	return expr{
		typ:  exprTuple,
		args: args,
	}
}

// TupleValues returns tuple of given values.
func TupleValues[V litValue](vals ...V) expr {
	args := make([]expr, len(vals))
	for i, val := range vals {
		args[i] = Value(val)
	}
	return Tuple(args...)
}
