package logqlengine

import (
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LineFilter is a line matching Processor.
type LineFilter struct {
	matcher StringMatcher
}

func buildLineFilter(stage *logql.LineFilter) (Processor, error) {
	if stage.IP {
		return nil, &UnsupportedError{Msg: "ip line filter is not supported yet"}
	}

	matcher, err := buildStringMatcher(stage.Op, stage.Value, stage.Re, false)
	if err != nil {
		return nil, err
	}

	return &LineFilter{
		matcher: matcher,
	}, nil
}

// Process implements Processor.
func (lf *LineFilter) Process(_ otelstorage.Timestamp, line string, _ LabelSet) (_ string, keep bool) {
	keep = lf.matcher.Match(line)
	return line, keep
}
