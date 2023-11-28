package chstorage

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
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
	name       string
	attributes string
	resource   string
	bucketKey  [2]string
}

func (p *promQuerier) selectSeries(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) (_ storage.SeriesSet, rerr error) {
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
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	buildQuery := func(table string) (string, error) {
		var query strings.Builder
		fmt.Fprintf(&query, "SELECT * FROM %#[1]q WHERE true\n", table)
		if !start.IsZero() {
			fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(timestamp) >= %d\n", start.UnixNano())
		}
		if !end.IsZero() {
			fmt.Fprintf(&query, "\tAND toUnixTimestamp64Nano(timestamp) <= %d\n", end.UnixNano())
		}
		for _, m := range matchers {
			switch m.Type {
			case labels.MatchEqual, labels.MatchRegexp:
				query.WriteString("AND ")
			case labels.MatchNotEqual, labels.MatchNotRegexp:
				query.WriteString("AND NOT ")
			default:
				return "", errors.Errorf("unexpected type %q", m.Type)
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
				query.WriteString("(\n")
				for i, sel := range selectors {
					if i != 0 {
						query.WriteString("\tOR ")
					}
					// Note: predicate negated above.
					switch m.Type {
					case labels.MatchEqual, labels.MatchNotEqual:
						fmt.Fprintf(&query, "%s = %s\n", sel, singleQuoted(m.Value))
					case labels.MatchRegexp, labels.MatchNotRegexp:
						fmt.Fprintf(&query, "%s REGEXP %s\n", sel, singleQuoted(m.Value))
					default:
						return "", errors.Errorf("unexpected type %q", m.Type)
					}
				}
				query.WriteString(")")
			}
			query.WriteString("\n")
		}
		query.WriteString("ORDER BY timestamp")
		return query.String(), nil
	}

	var (
		points        []storage.Series
		histSeries    []storage.Series
		expHistSeries []storage.Series
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx

		query, err := buildQuery(p.tables.Points)
		if err != nil {
			return err
		}

		result, err := p.queryPoints(ctx, query)
		if err != nil {
			return errors.Wrap(err, "query points")
		}
		points = result
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		query, err := buildQuery(p.tables.Histograms)
		if err != nil {
			return err
		}

		result, err := p.queryHistograms(ctx, query)
		if err != nil {
			return errors.Wrap(err, "query histograms")
		}
		histSeries = result
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx

		query, err := buildQuery(p.tables.ExpHistograms)
		if err != nil {
			return err
		}

		result, err := p.queryExpHistograms(ctx, query)
		if err != nil {
			return errors.Wrap(err, "query exponential histograms")
		}
		expHistSeries = result
		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	points = append(points, histSeries...)
	points = append(points, expHistSeries...)
	return newSeriesSet(points), nil
}

func (p *promQuerier) queryPoints(ctx context.Context, query string) ([]storage.Series, error) {
	type seriesWithLabels struct {
		series *series[pointData]
		labels map[string]string
	}

	var (
		set = map[seriesKey]seriesWithLabels{}
		c   = newPointColumns()
	)
	if err := p.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				name := c.name.Row(i)
				value := c.value.Row(i)
				timestamp := c.timestamp.Row(i)
				attributes := c.attributes.Row(i)
				resource := c.resource.Row(i)

				key := seriesKey{
					name:       name,
					attributes: attributes,
					resource:   resource,
				}
				s, ok := set[key]
				if !ok {
					s = seriesWithLabels{
						series: &series[pointData]{},
						labels: map[string]string{},
					}
					set[key] = s
				}

				s.series.data.values = append(s.series.data.values, value)
				s.series.ts = append(s.series.ts, timestamp.UnixMilli())

				s.labels["__name__"] = name
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
		result = make([]storage.Series, 0, len(set))
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

	return result, nil
}

func (p *promQuerier) queryHistograms(ctx context.Context, query string) ([]storage.Series, error) {
	type seriesWithLabels struct {
		series *series[pointData]
		labels map[string]string
	}
	type histogramSample struct {
		timestamp                  int64
		rawAttributes, rawResource string
		attributes, resource       map[string]string
		flags                      pmetric.DataPointFlags
	}

	var (
		set       = map[seriesKey]seriesWithLabels{}
		addSample = func(
			name string,
			val float64,
			sample histogramSample,
			bucketKey [2]string,
		) {
			key := seriesKey{
				name:       name,
				attributes: sample.rawAttributes,
				resource:   sample.rawResource,
				bucketKey:  bucketKey,
			}
			s, ok := set[key]
			if !ok {
				s = seriesWithLabels{
					series: &series[pointData]{},
					labels: map[string]string{},
				}
				set[key] = s
			}

			if sample.flags.NoRecordedValue() {
				val = math.Float64frombits(value.StaleNaN)
			}
			s.series.data.values = append(s.series.data.values, val)
			s.series.ts = append(s.series.ts, sample.timestamp)

			s.labels["__name__"] = name
			maps.Copy(s.labels, sample.attributes)
			maps.Copy(s.labels, sample.resource)
			if key := bucketKey[0]; key != "" {
				s.labels[key] = bucketKey[1]
			}
		}
		c = newHistogramColumns()
	)
	if err := p.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				name := c.name.Row(i)
				timestamp := c.timestamp.Row(i)
				count := c.count.Row(i)
				sum := c.sum.Row(i)
				_min := c.min.Row(i)
				_max := c.max.Row(i)
				bucketCounts := c.bucketCounts.Row(i)
				explicitBounds := c.explicitBounds.Row(i)
				flags := pmetric.DataPointFlags(c.flags.Row(i))
				rawAttributes := c.attributes.Row(i)
				rawResource := c.resource.Row(i)

				var (
					resource   = map[string]string{}
					attributes = map[string]string{}
				)
				if err := parseLabels(rawResource, resource); err != nil {
					return errors.Wrap(err, "parse resource")
				}
				if err := parseLabels(rawAttributes, attributes); err != nil {
					return errors.Wrap(err, "parse attributes")
				}
				sample := histogramSample{
					timestamp:     timestamp.UnixMilli(),
					rawAttributes: rawAttributes,
					rawResource:   rawResource,
					attributes:    attributes,
					resource:      resource,
					flags:         flags,
				}

				if sum.Set {
					addSample(name+"_sum", sum.Value, sample, [2]string{})
				}
				if _min.Set {
					addSample(name+"_min", _min.Value, sample, [2]string{})
				}
				if _max.Set {
					addSample(name+"_max", _max.Value, sample, [2]string{})
				}
				addSample(name+"_count", float64(count), sample, [2]string{})

				var cumCount uint64
				for i := 0; i < min(len(bucketCounts), len(explicitBounds)); i++ {
					bound := explicitBounds[i]
					cumCount += bucketCounts[i]

					// Generate series with "_bucket" suffix and "le" label.
					addSample("_bucket", float64(cumCount), sample, [2]string{
						"le",
						strconv.FormatFloat(bound, 'f', -1, 64),
					})
				}

				{
					// Generate series with "_bucket" suffix and "le" label.
					addSample("_bucket", float64(count), sample, [2]string{
						"le",
						"+Inf",
					})
				}
			}
			return nil
		},
	}); err != nil {
		return nil, errors.Wrap(err, "do query")
	}

	var (
		result = make([]storage.Series, 0, len(set))
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

	return result, nil
}

func (p *promQuerier) queryExpHistograms(ctx context.Context, query string) ([]storage.Series, error) {
	type seriesWithLabels struct {
		series *series[expHistData]
		labels map[string]string
	}

	var (
		set = map[seriesKey]seriesWithLabels{}
		c   = newExpHistogramColumns()
	)
	if err := p.ch.Do(ctx, ch.Query{
		Body:   query,
		Result: c.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < c.timestamp.Rows(); i++ {
				name := c.name.Row(i)
				timestamp := c.timestamp.Row(i)
				count := c.count.Row(i)
				sum := c.sum.Row(i)
				min := c.min.Row(i)
				max := c.max.Row(i)
				scale := c.scale.Row(i)
				zerocount := c.zerocount.Row(i)
				positiveOffset := c.positiveOffset.Row(i)
				positiveBucketCounts := c.positiveBucketCounts.Row(i)
				negativeOffset := c.negativeOffset.Row(i)
				negativeBucketCounts := c.negativeBucketCounts.Row(i)
				attributes := c.attributes.Row(i)
				resource := c.resource.Row(i)

				key := seriesKey{
					name:       name,
					attributes: attributes,
					resource:   resource,
				}
				s, ok := set[key]
				if !ok {
					s = seriesWithLabels{
						series: &series[expHistData]{},
						labels: map[string]string{},
					}
					set[key] = s
				}

				s.series.data.count = append(s.series.data.count, count)
				s.series.data.sum = append(s.series.data.sum, sum)
				s.series.data.min = append(s.series.data.min, min)
				s.series.data.max = append(s.series.data.max, max)
				s.series.data.scale = append(s.series.data.scale, scale)
				s.series.data.zerocount = append(s.series.data.zerocount, zerocount)
				s.series.data.positiveOffset = append(s.series.data.positiveOffset, positiveOffset)
				s.series.data.positiveBucketCounts = append(s.series.data.positiveBucketCounts, positiveBucketCounts)
				s.series.data.negativeOffset = append(s.series.data.negativeOffset, negativeOffset)
				s.series.data.negativeBucketCounts = append(s.series.data.negativeBucketCounts, negativeBucketCounts)
				s.series.ts = append(s.series.ts, timestamp.UnixMilli())

				s.labels["__name__"] = name
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
		result = make([]storage.Series, 0, len(set))
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

	return result, nil
}

type seriesSet[S storage.Series] struct {
	set []S
	n   int
}

func newSeriesSet[S storage.Series](set []S) *seriesSet[S] {
	return &seriesSet[S]{
		set: set,
		n:   -1,
	}
}

var _ storage.SeriesSet = (*seriesSet[storage.Series])(nil)

func (s *seriesSet[S]) Next() bool {
	if s.n+1 >= len(s.set) {
		return false
	}
	s.n++
	return true
}

// At returns full series. Returned series should be iterable even after Next is called.
func (s *seriesSet[S]) At() storage.Series {
	return s.set[s.n]
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (s *seriesSet[S]) Err() error {
	return nil
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (s *seriesSet[S]) Warnings() annotations.Annotations {
	return nil
}

type seriesData interface {
	Iterator(ts []int64) chunkenc.Iterator
}

type series[Data seriesData] struct {
	labels labels.Labels
	data   Data
	ts     []int64
}

var _ storage.Series = (*series[pointData])(nil)

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s *series[Data]) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (s *series[Data]) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return s.data.Iterator(s.ts)
}
