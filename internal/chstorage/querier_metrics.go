package chstorage

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var _ storage.Queryable = (*Querier)(nil)

// Querier returns a new Querier on the storage.
func (q *Querier) Querier(mint, maxt int64) (storage.Querier, error) {
	var minTime, maxTime time.Time
	if mint > 0 {
		minTime = time.UnixMilli(mint)
	}
	if maxt > 0 {
		maxTime = time.UnixMilli(maxt)
	}
	return &promQuerier{
		mint: minTime,
		maxt: maxTime,

		ch:     q.ch,
		tables: q.tables,
		tracer: q.tracer,
	}, nil
}

type promQuerier struct {
	mint time.Time
	maxt time.Time

	ch     chClient
	tables Tables

	tracer trace.Tracer
}

var _ storage.Querier = (*promQuerier)(nil)

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifetime of the querier.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (p *promQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) (result []string, _ annotations.Annotations, rerr error) {
	table := p.tables.Labels

	ctx, span := p.tracer.Start(ctx, "LabelValues",
		trace.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.String("chstorage.label_to_query", name),
			attribute.Int("chstorage.label_matchers", len(matchers)),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, "SELECT DISTINCT value FROM %#q WHERE name = %s\n", table, singleQuoted(name))
	if err := addLabelMatchers(&query, matchers); err != nil {
		return nil, nil, err
	}

	var column proto.ColStr
	if err := p.ch.Do(ctx, ch.Query{
		Body: query.String(),
		Result: proto.Results{
			{Name: "value", Data: &column},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < column.Rows(); i++ {
				result = append(result, column.Row(i))
			}
			return nil
		},
	}); err != nil {
		return nil, nil, errors.Wrap(err, "do query")
	}
	return result, nil, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
// If matchers are specified the returned result set is reduced
// to label names of metrics matching the matchers.
func (p *promQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) (result []string, _ annotations.Annotations, rerr error) {
	table := p.tables.Labels

	ctx, span := p.tracer.Start(ctx, "LabelNames",
		trace.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.Int("chstorage.label_matchers", len(matchers)),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, "SELECT DISTINCT name FROM %#q WHERE true\n", table)
	if err := addLabelMatchers(&query, matchers); err != nil {
		return nil, nil, err
	}

	column := new(proto.ColStr).LowCardinality()
	if err := p.ch.Do(ctx, ch.Query{
		Body: query.String(),
		Result: proto.Results{
			{Name: "name", Data: column},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < column.Rows(); i++ {
				result = append(result, column.Row(i))
			}
			return nil
		},
	}); err != nil {
		return nil, nil, errors.Wrap(err, "do query")
	}
	return result, nil, nil
}

func addLabelMatchers(query *strings.Builder, matchers []*labels.Matcher) error {
	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			query.WriteString("AND ")
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			query.WriteString("AND NOT ")
		default:
			return errors.Errorf("unexpected type %q", m.Type)
		}

		// Note: predicate negated above.
		switch m.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			fmt.Fprintf(query, "name = %s\n", singleQuoted(m.Value))
		case labels.MatchRegexp, labels.MatchNotRegexp:
			fmt.Fprintf(query, "name REGEXP %s\n", singleQuoted(m.Value))
		default:
			return errors.Errorf("unexpected type %q", m.Type)
		}
	}
	return nil
}

// Close releases the resources of the Querier.
func (p *promQuerier) Close() error {
	return nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select, but it's up to implementation how this is used if used at all.
func (p *promQuerier) Select(ctx context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ss, err := p.selectSeries(ctx, hints, matchers...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return ss
}

type seriesKey struct {
	attributes string
	resource   string
}

func (p *promQuerier) selectSeries(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) (_ storage.SeriesSet, rerr error) {
	table := p.tables.Points
	var (
		start = p.mint
		end   = p.maxt
	)
	if hints != nil {
		if t := time.UnixMilli(hints.Start); t.After(start) {
			start = t
		}
		if t := time.UnixMilli(hints.End); t.Before(end) {
			end = t
		}
	}

	ctx, span := p.tracer.Start(ctx, "SelectSeries",
		trace.WithAttributes(
			attribute.Int64("chstorage.start_range", start.UnixNano()),
			attribute.Int64("chstorage.end_range", end.UnixNano()),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var query strings.Builder
	fmt.Fprintf(&query, "SELECT * FROM %#[1]q WHERE true\n", table)
	if !start.IsZero() {
		fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(ts) >= %d\n", start.UnixNano())
	}
	if !end.IsZero() {
		fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(ts) <= %d\n", end.UnixNano())
	}
	for _, m := range matchers {
		query.WriteString("\t(")

		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			query.WriteString("AND ")
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			query.WriteString("AND NOT ")
		default:
			return nil, errors.Errorf("unexpected type %q", m.Type)
		}

		{
			selectors := []string{
				"name",
			}
			if m.Name != "__name__" {
				selectors = []string{
					fmt.Sprintf("JSONExtractString(attributes, %s)", singleQuoted(m.Name)),
					fmt.Sprintf("JSONExtractString(resource, %s)", singleQuoted(m.Name)),
				}
			}
			query.WriteString("\t\t(")
			for i, sel := range selectors {
				if i != 0 {
					query.WriteString("\t\tOR")
				}
				// Note: predicate negated above.
				switch m.Type {
				case labels.MatchEqual, labels.MatchNotEqual:
					fmt.Fprintf(&query, "%s = %s\n", sel, singleQuoted(m.Value))
				case labels.MatchRegexp, labels.MatchNotRegexp:
					fmt.Fprintf(&query, "%s REGEXP %s\n", sel, singleQuoted(m.Value))
				default:
					return nil, errors.Errorf("unexpected type %q", m.Type)
				}
			}
			query.WriteString("\t)")
		}

		query.WriteString(")\n")
	}
	query.WriteString("ORDER BY ts")

	return p.doQuery(ctx, query.String())
}

func (p *promQuerier) doQuery(ctx context.Context, query string) (storage.SeriesSet, error) {
	type seriesWithLabels struct {
		series *series
		labels map[string]string
	}

	var (
		set = map[seriesKey]seriesWithLabels{}
		c   = newMetricColumns()
	)
	if err := p.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.ts.Rows(); i++ {
				value := c.value.Row(i)
				ts := c.ts.Row(i)
				attributes := c.attributes.Row(i)
				resource := c.resource.Row(i)

				key := seriesKey{
					attributes: attributes,
					resource:   resource,
				}
				s, ok := set[key]
				if !ok {
					s = seriesWithLabels{
						series: &series{},
						labels: map[string]string{},
					}
					set[key] = s
				}

				s.series.values = append(s.series.values, value)
				s.series.ts = append(s.series.ts, ts.UnixMilli())
				if err := parseLabels(resource, s.labels); err != nil {
					return errors.Wrap(err, "parse resource")
				}
				if err := parseLabels(attributes, s.labels); err != nil {
					return errors.Wrap(err, "parse attributes")
				}
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "do query")
	}

	var (
		result = make([]*series, 0, len(set))
		lb     labels.ScratchBuilder
	)
	for _, s := range set {
		lb.Reset()
		for key, value := range s.labels {
			lb.Add(key, value)
		}
		lb.Sort()
		s.series.labels = lb.Labels()
		result = append(result, s.series)
	}

	return &seriesSet{
		set: result,
	}, nil
}

type seriesSet struct {
	set []*series
	n   int
}

func newSeriesSet(set []*series) *seriesSet {
	return &seriesSet{
		set: set,
		n:   -1,
	}
}

var _ storage.SeriesSet = (*seriesSet)(nil)

func (s *seriesSet) Next() bool {
	if s.n+1 >= len(s.set) {
		return false
	}
	s.n++
	return true
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s *seriesSet) At() storage.Series {
	return s.set[s.n]
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (s *seriesSet) Err() error {
	return nil
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (s *seriesSet) Warnings() annotations.Annotations {
	return nil
}

type series struct {
	labels labels.Labels
	values []float64
	ts     []int64
}

var _ storage.Series = (*series)(nil)

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s *series) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s *series) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return newPointIterator(s.values, s.ts)
}

type pointIterator struct {
	values []float64
	ts     []int64
	n      int
}

var _ chunkenc.Iterator = (*pointIterator)(nil)

func newPointIterator(values []float64, ts []int64) *pointIterator {
	return &pointIterator{
		values: values,
		ts:     ts,
		n:      -1,
	}
}

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (p *pointIterator) Next() chunkenc.ValueType {
	if p.n+1 >= len(p.values) {
		return chunkenc.ValNone
	}
	p.n++
	return chunkenc.ValFloat
}

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after which the iterator is
// exhausted.
func (p *pointIterator) Seek(seek int64) chunkenc.ValueType {
	// Find the closest value.
	idx, _ := slices.BinarySearch(p.ts, seek)
	if idx >= len(p.ts) {
		p.n = len(p.ts)
		return chunkenc.ValNone
	}
	p.n = idx - 1
	return chunkenc.ValFloat
}

// At returns the current timestamp/value pair if the value is a float.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) At() (t int64, v float64) {
	t = p.AtT()
	v = p.values[p.n]
	return t, v
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behavior is unspecified.
func (p *pointIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behavior is unspecified.
func (p *pointIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) AtT() int64 {
	return p.ts[p.n]
}

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (p *pointIterator) Err() error {
	return nil
}
