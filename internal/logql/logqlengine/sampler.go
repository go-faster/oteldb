package logqlengine

import (
	"context"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func (e *Engine) sampleSelector(ctx context.Context, params EvalParams) logqlmetric.SampleSelector {
	return func(expr *logql.RangeAggregationExpr, start, end time.Time) (_ iterators.Iterator[logqlmetric.SampledEntry], rerr error) {
		qrange := expr.Range

		iter, err := e.selectLogs(ctx, qrange.Sel, qrange.Pipeline, selectLogsParams{
			Start:     otelstorage.NewTimestampFromTime(start),
			End:       otelstorage.NewTimestampFromTime(end),
			Instant:   params.IsInstant(),
			Direction: params.Direction,
			// Do not limit sample queries.
			Limit: -1,
		})
		if err != nil {
			return nil, errors.Wrap(err, "select logs")
		}
		defer func() {
			if rerr != nil {
				_ = iter.Close()
			}
		}()

		return newSampleIterator(iter, expr)
	}
}

type sampleIterator struct {
	iter    iterators.Iterator[entry]
	sampler sampleExtractor

	// grouping parameters.
	by      map[string]struct{}
	without map[string]struct{}
}

func newSampleIterator(iter iterators.Iterator[entry], expr *logql.RangeAggregationExpr) (*sampleIterator, error) {
	sampler, err := buildSampleExtractor(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build sample extractor")
	}

	var (
		by      []logql.Label
		without []logql.Label
	)
	if g := expr.Grouping; g != nil {
		if g.Without {
			without = g.Labels
		} else {
			by = g.Labels
		}
	}

	return &sampleIterator{
		iter:    iter,
		sampler: sampler,
		by:      buildSet(nil, by...),
		without: buildSet(nil, without...),
	}, nil
}

func (i *sampleIterator) Next(s *logqlmetric.SampledEntry) bool {
	var e entry
	for {
		if !i.iter.Next(&e) {
			return false
		}

		v, ok := i.sampler.Extract(e)
		if !ok {
			continue
		}

		s.Timestamp = e.ts
		s.Sample = v
		s.Set = newAggregatedLabels(e.set, i.by, i.without)
		return true
	}
}

func (i *sampleIterator) Err() error {
	return i.iter.Err()
}

func (i *sampleIterator) Close() error {
	return i.iter.Close()
}

// sampleExtractor extracts samples from log records.
type sampleExtractor interface {
	Extract(e entry) (float64, bool)
}

func buildSampleExtractor(expr *logql.RangeAggregationExpr) (sampleExtractor, error) {
	qrange := expr.Range
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
			le.converter = convertBytes
		case "duration", "duration_seconds":
			le.converter = convertDuration
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

func convertBytes(s string) (float64, error) {
	v, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, err
	}
	return float64(v), nil
}

func convertDuration(s string) (float64, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
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
