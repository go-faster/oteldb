package logqlengine

import (
	"context"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func (e *Engine) sampleSelector(ctx context.Context, params EvalParams) logqlmetric.SampleSelector {
	return func(expr *logql.RangeAggregationExpr, start, end time.Time) (_ iterators.Iterator[logqlmetric.SampledEntry], rerr error) {
		qrange := expr.Range

		iter, err := e.selectLogs(ctx, qrange.Sel, qrange.Pipeline, selectLogsParams{
			Start:   otelstorage.NewTimestampFromTime(start),
			End:     otelstorage.NewTimestampFromTime(end),
			Instant: params.IsInstant(),
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

func buildSet[K ~string](r map[string]struct{}, input ...K) map[string]struct{} {
	if len(input) == 0 {
		return r
	}

	if r == nil {
		r = make(map[string]struct{}, len(input))
	}
	for _, k := range input {
		r[string(k)] = struct{}{}
	}
	return r
}

type labelEntry struct {
	name  string
	value string
}

type aggregatedLabels struct {
	entries []labelEntry
	without map[string]struct{}
	by      map[string]struct{}
}

func newAggregatedLabels(set LabelSet, by, without map[string]struct{}) *aggregatedLabels {
	labels := make([]labelEntry, 0, len(set.labels))
	set.Range(func(l logql.Label, v pcommon.Value) {
		labels = append(labels, labelEntry{
			name:  string(l),
			value: v.AsString(),
		})
	})

	return &aggregatedLabels{
		entries: labels,
		without: without,
		by:      by,
	}
}

// By returns new set of labels containing only given list of labels.
func (a *aggregatedLabels) By(labels ...logql.Label) logqlmetric.AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: a.without,
		by:      buildSet(maps.Clone(a.by), labels...),
	}
	return sub
}

// Without returns new set of labels without given list of labels.
func (a *aggregatedLabels) Without(labels ...logql.Label) logqlmetric.AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: maps.Clone(a.without),
		by:      buildSet(maps.Clone(a.without), labels...),
	}
	return sub
}

// Key computes grouping key from set of labels.
func (a *aggregatedLabels) Key() logqlmetric.GroupingKey {
	h := xxhash.New()
	a.forEach(func(k, v string) {
		_, _ = h.WriteString(k)
		_, _ = h.WriteString(v)
	})
	return h.Sum64()
}

// AsLokiAPI returns API structure for label set.
func (a *aggregatedLabels) AsLokiAPI() (r lokiapi.LabelSet) {
	r = lokiapi.LabelSet{}
	a.forEach(func(k, v string) {
		r[k] = v
	})
	return r
}

func (a *aggregatedLabels) forEach(cb func(k, v string)) {
	for _, e := range a.entries {
		if _, ok := a.without[e.name]; ok {
			continue
		}
		if len(a.by) > 0 {
			if _, ok := a.by[e.name]; !ok {
				continue
			}
		}
		cb(e.name, e.value)
	}
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
