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
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/otelstorage"
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

		ch:              q.ch,
		tables:          q.tables,
		tracer:          q.tracer,
		getLabelMapping: q.getMetricsLabelMapping,
	}, nil
}

type promQuerier struct {
	mint time.Time
	maxt time.Time

	ch              chClient
	tables          Tables
	getLabelMapping func(context.Context, []string) (map[string]string, error)

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
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
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

	if name == labels.MetricName {
		// Map label names.
		for i := range result {
			result[i] = otelstorage.KeyToLabel(result[i])
		}
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
		Logger: zctx.From(ctx).Named("ch"),
		Body:   query.String(),
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

func (q *Querier) getMetricsLabelMapping(ctx context.Context, input []string) (_ map[string]string, rerr error) {
	ctx, span := q.tracer.Start(ctx, "getMetricsLabelMapping",
		trace.WithAttributes(
			attribute.Int("chstorage.labels_count", len(input)),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	out := make(map[string]string, len(input))
	var (
		name = new(proto.ColStr).LowCardinality()
		key  = new(proto.ColStr).LowCardinality()
	)
	var inputData proto.ColStr
	for _, label := range input {
		inputData.Append(label)
	}
	if err := q.ch.Do(ctx, ch.Query{
		Result: proto.Results{
			{Name: "name", Data: name},
			{Name: "key", Data: key},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			l := min(name.Rows(), key.Rows())
			for i := 0; i < l; i++ {
				out[name.Row(i)] = key.Row(i)
			}
			return nil
		},
		ExternalTable: "labels",
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},
		Body: fmt.Sprintf(`SELECT name, key FROM %[1]s WHERE name IN labels`, q.tables.Labels),
	}); err != nil {
		return nil, errors.Wrap(err, "select")
	}

	return out, nil
}

// Close releases the resources of the Querier.
func (p *promQuerier) Close() error {
	return nil
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimizing select, but it's up to implementation how this is used if used at all.
func (p *promQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ss, err := p.selectSeries(ctx, sortSeries, hints, matchers...)
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

func (p *promQuerier) selectSeries(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (_ storage.SeriesSet, rerr error) {
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

	var queryLabels []string
	for _, m := range matchers {
		queryLabels = append(queryLabels, m.Name)
	}
	mapping, err := p.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	// TODO(tdakkota): optimize query by func hint (e.g. func "series").
	buildQuery := func(table string) (string, error) {
		var query strings.Builder
		columns := "_bug"
		switch table {
		case p.tables.Points:
			columns = newPointColumns().Columns().All()
		case p.tables.ExpHistograms:
			columns = newExpHistogramColumns().Columns().All()
		default:
			return "", errors.Errorf("unexpected table %q", table)
		}
		fmt.Fprintf(&query, "SELECT %[1]s FROM %#[2]q WHERE true\n", columns, table)
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
				if name := m.Name; name != labels.MetricName {
					if mapped, ok := mapping[name]; ok {
						name = mapped
					}
					selectors = []string{
						fmt.Sprintf("JSONExtractString(attributes, %s)", singleQuoted(name)),
						fmt.Sprintf("JSONExtractString(resource, %s)", singleQuoted(name)),
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
		summarySeries []storage.Series
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
	points = append(points, summarySeries...)
	if sortSeries {
		slices.SortFunc(points, func(a, b storage.Series) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}
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
		Logger: zctx.From(ctx).Named("ch"),
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

				s.labels[labels.MetricName] = otelstorage.KeyToLabel(name)
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
		s.series.labels = buildPromLabels(&lb, s.labels)
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
		Logger: zctx.From(ctx).Named("ch"),
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

				s.labels[labels.MetricName] = otelstorage.KeyToLabel(name)
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
		s.series.labels = buildPromLabels(&lb, s.labels)
		result = append(result, s.series)
	}

	return result, nil
}

func buildPromLabels(lb *labels.ScratchBuilder, set map[string]string) labels.Labels {
	lb.Reset()
	for key, value := range set {
		key = otelstorage.KeyToLabel(key)
		lb.Add(key, value)
	}
	lb.Sort()
	return lb.Labels()
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
