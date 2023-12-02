package logqlengine

import (
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlpattern"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// PatternExtractor is a Pattern label extractor.
type PatternExtractor struct {
	pattern logqlpattern.Pattern
}

func buildPatternExtractor(stage *logql.PatternLabelParser) (Processor, error) {
	compiled, err := logqlpattern.Parse(stage.Pattern)
	if err != nil {
		return nil, errors.Wrapf(err, "parse pattern %q", stage.Pattern)
	}

	return &PatternExtractor{
		pattern: compiled,
	}, nil
}

// Process implements Processor.
func (e *PatternExtractor) Process(_ otelstorage.Timestamp, line string, set LabelSet) (string, bool) {
	logqlpattern.Match(e.pattern, line, func(l logql.Label, s string) {
		set.Set(l, pcommon.NewValueStr(s))
	})
	return line, true
}
