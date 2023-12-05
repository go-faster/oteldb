// Package logparser parses logs.
package logparser

// Parser parses logs.
type Parser interface {
	// Parse parses data and returns a line.
	//
	// TODO: refactor to `Parse(data []byte, target *logstorage.Record) error`
	Parse(data []byte) (*Line, error)
	// Detect whether data is parsable.
	//
	// TODO: refactor to `Detect(data []byte) bool`
	Detect(line string) bool
	String() string
}
