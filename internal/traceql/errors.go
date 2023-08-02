package traceql

import (
	"fmt"
	"text/scanner"
)

// SyntaxError is a syntax error.
type SyntaxError struct {
	Msg string
	Pos scanner.Position
}

// Error implements error.
func (e *SyntaxError) Error() string {
	return fmt.Sprintf("at %s: %s", e.Pos, e.Msg)
}

// TypeError is a type checking error.
type TypeError struct {
	Msg string
	Pos scanner.Position
}

// Error implements error.
func (e *TypeError) Error() string {
	return fmt.Sprintf("at %s: %s", e.Pos, e.Msg)
}
