package logqlengine

import (
	"regexp"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// RegexpExtractor is a Regexp label extractor.
type RegexpExtractor struct {
	Regexp  *regexp.Regexp
	Mapping map[int]logql.Label
}

func buildRegexpExtractor(stage *logql.RegexpLabelParser) (Processor, error) {
	return &RegexpExtractor{
		Regexp:  stage.Regexp,
		Mapping: stage.Mapping,
	}, nil
}

// Process implements Processor.
func (e *RegexpExtractor) Process(_ otelstorage.Timestamp, line string, set LabelSet) (string, bool) {
	for i, match := range e.Regexp.FindStringSubmatch(line) {
		label, ok := e.Mapping[i]
		if !ok {
			continue
		}
		set.Add(label, pcommon.NewValueStr(match))
	}
	return line, true
}
