// Package logparser parses logs.
package logparser

// Parser parses logs.
type Parser interface {
	Parse(data []byte) (*Line, error)
	Detect(line string) bool
}
