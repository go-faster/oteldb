package logqlengine

import (
	"strings"

	"github.com/go-logfmt/logfmt"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LogfmtExtractor is a Logfmt label extractor.
type LogfmtExtractor struct {
	Labels map[logql.Label]struct{}
}

func buildLogfmtExtractor(stage *logql.LogfmtExpressionParser) (Processor, error) {
	if len(stage.Exprs) > 0 {
		return nil, &UnsupportedError{Msg: "extraction expressions are not supported yet"}
	}

	e := &LogfmtExtractor{}
	if labels := stage.Labels; len(labels) > 0 {
		e.Labels = make(map[logql.Label]struct{}, len(labels))
		for _, label := range labels {
			e.Labels[label] = struct{}{}
		}
	}
	return e, nil
}

// Process implements Processor.
func (e *LogfmtExtractor) Process(_ otelstorage.Timestamp, line string, set LabelSet) (string, bool) {
	var err error
	if len(e.Labels) == 0 {
		err = e.extractAll(line, set)
	} else {
		err = e.extractSome(line, set)
	}
	if err != nil {
		set.SetError(err)
	}
	return line, true
}

func (e *LogfmtExtractor) extractSome(line string, set LabelSet) error {
	// TODO(tdakkota): re-use decoder somehow.
	d := logfmt.NewDecoder(strings.NewReader(line))

	for d.ScanRecord() {
		for d.ScanKeyval() {
			if _, ok := e.Labels[logql.Label(d.Key())]; ok {
				// TODO(tdakkota): try string interning
				// TODO(tdakkota): probably, we can just use label name string
				// 	instead of allocating a new string every time
				set.Add(logql.Label(d.Key()), pcommon.NewValueStr(string(d.Value())))
			}
		}
	}

	return d.Err()
}

func (e *LogfmtExtractor) extractAll(line string, set LabelSet) error {
	// TODO(tdakkota): re-use decoder somehow.
	d := logfmt.NewDecoder(strings.NewReader(line))

	for d.ScanRecord() {
		for d.ScanKeyval() {
			// TODO(tdakkota): try string interning
			set.Add(logql.Label(d.Key()), pcommon.NewValueStr(string(d.Value())))
		}
	}

	return d.Err()
}
