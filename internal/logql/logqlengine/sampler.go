package logqlengine

import (
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
)

// sampleExtractor extracts samples from log records.
type sampleExtractor interface {
	Extract(e entry) (float64, bool)
}

func buildSampleExtractor(expr *logql.RangeAggregationExpr) (sampleExtractor, error) {
	qrange := expr.Range
	if expr.Grouping != nil {
		return nil, &UnsupportedError{Msg: "grouping is not supported yet"}
	}
	switch expr.Op {
	case logql.RangeOpCount, logql.RangeOpRate, logql.RangeOpAbsent:
		return &lineCounterExtractor{}, nil
	case logql.RangeOpBytes, logql.RangeOpBytesRate:
		return &bytesCounterExtractor{}, nil
	case logql.RangeOpRateCounter,
		logql.RangeOpAvg,
		logql.RangeOpSum,
		logql.RangeOpMin,
		logql.RangeOpMax,
		logql.RangeOpStdvar,
		logql.RangeOpStddev,
		logql.RangeOpQuantile,
		logql.RangeOpFirst,
		logql.RangeOpLast:
		unwrap := qrange.Unwrap
		if unwrap == nil {
			return nil, errors.Errorf("operation %q require unwrap expression", expr.Op)
		}

		le := &labelsExtractor{
			label: unwrap.Label,
		}

		switch unwrap.Op {
		case "":
			le.converter = func(s string) (float64, error) {
				return strconv.ParseFloat(s, 64)
			}
		case "bytes":
			le.converter = func(s string) (float64, error) {
				v, err := humanize.ParseBytes(s)
				if err != nil {
					return 0, err
				}
				return float64(v), nil
			}
		case "duration", "duration_seconds":
			le.converter = func(s string) (float64, error) {
				d, err := time.ParseDuration(s)
				if err != nil {
					return 0, err
				}
				return d.Seconds(), nil
			}
		default:
			return nil, errors.Errorf("unknown conversion operation %q", unwrap.Op)
		}

		switch len(unwrap.Filters) {
		case 0:
			le.postfilter = NopProcessor
		case 1:
			proc, err := buildLabelMatcher(unwrap.Filters[0])
			if err != nil {
				return nil, err
			}
			le.postfilter = proc
		default:
			procs := make([]Processor, len(unwrap.Filters))
			var err error
			for i, f := range unwrap.Filters {
				procs[i], err = buildLabelMatcher(f)
				if err != nil {
					return nil, err
				}
			}
			le.postfilter = &Pipeline{Stages: procs}
		}

		return le, nil
	default:
		return nil, errors.Errorf("unknown range operation %q", expr.Op)
	}
}

type lineCounterExtractor struct{}

func (*lineCounterExtractor) Extract(entry) (float64, bool) {
	return 1., true
}

type bytesCounterExtractor struct{}

func (*bytesCounterExtractor) Extract(e entry) (float64, bool) {
	return float64(len(e.line)), true
}

type labelsExtractor struct {
	label      logql.Label
	converter  func(string) (float64, error)
	postfilter Processor
}

func (l *labelsExtractor) Extract(e entry) (p float64, _ bool) {
	v, ok := e.set.GetString(l.label)
	if !ok {
		return p, false
	}

	p, _ = l.converter(v)
	// TODO(tdakkota): save error

	_, ok = l.postfilter.Process(e.ts, e.line, e.set)
	return p, ok
}
