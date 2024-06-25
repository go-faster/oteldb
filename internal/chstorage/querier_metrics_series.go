package chstorage

import (
	"context"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/xattribute"
)

// OnlySeries selects only labels from series.
func (p *promQuerier) OnlySeries(ctx context.Context, sortSeries bool, startMs, endMs int64, matcherSets ...[]*labels.Matcher) storage.SeriesSet {
	ss, err := p.selectOnlySeries(ctx, sortSeries, startMs, endMs, matcherSets...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return ss
}

var _ metricstorage.OptimizedSeriesQuerier = (*promQuerier)(nil)

// OnlySeries selects only labels from series.
func (p *promQuerier) selectOnlySeries(
	ctx context.Context,
	sortSeries bool,
	startMs, endMs int64,
	matcherSets ...[]*labels.Matcher,
) (_ storage.SeriesSet, rerr error) {
	var start, end time.Time
	if ms := startMs; ms != promapi.MinTime.UnixMilli() {
		start = p.getStart(time.UnixMilli(ms))
	}
	if ms := endMs; ms != promapi.MaxTime.UnixMilli() {
		end = p.getEnd(time.UnixMilli(ms))
	}

	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.selectOnlySeries",
		trace.WithAttributes(
			attribute.Bool("promql.sort_series", sortSeries),
			attribute.Int64("promql.hints.start", startMs),
			attribute.Int64("promql.hints.end", endMs),

			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	var queryLabels []string
	for _, set := range matcherSets {
		for _, m := range set {
			queryLabels = append(queryLabels, m.Name)
		}
	}
	mapping, err := p.getLabelMapping(ctx, queryLabels)
	if err != nil {
		return nil, errors.Wrap(err, "get label mapping")
	}

	query := func(ctx context.Context, table string) (result []onlyLabelsSeries, _ error) {
		series := proto.ColMap[string, string]{
			Keys:   new(proto.ColStr),
			Values: new(proto.ColStr),
		}
		query, err := p.buildSeriesQuery(
			table,
			chsql.ResultColumn{
				Name: "series",
				Expr: chsql.MapConcat(
					chsql.Map(chsql.String("__name__"), chsql.Ident("name_normalized")),
					attrStringMap(colAttrs),
					attrStringMap(colResource),
					attrStringMap(colScope),
				),
				Data: &series,
			},
			start, end,
			matcherSets,
			mapping,
		)
		if err != nil {
			return nil, err
		}

		var (
			dedup = map[string]string{}
			lb    labels.ScratchBuilder
		)
		if err := p.do(ctx, selectQuery{
			Query: query,
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < series.Rows(); i++ {
					clear(dedup)
					forEachColMap(&series, i, func(k, v string) {
						dedup[otelstorage.KeyToLabel(k)] = v
					})

					lb.Reset()
					for k, v := range dedup {
						lb.Add(k, v)
					}
					lb.Sort()
					result = append(result, onlyLabelsSeries{
						labels: lb.Labels(),
					})
				}
				return nil
			},

			Type:   "QueryOnlySeries",
			Signal: "metrics",
			Table:  table,
		}); err != nil {
			return nil, err
		}
		span.AddEvent("series_fetched", trace.WithAttributes(
			attribute.String("chstorage.table", table),
			attribute.Int("chstorage.total_series", len(result)),
		))

		return result, nil
	}

	var (
		pointsSeries  []onlyLabelsSeries
		expHistSeries []onlyLabelsSeries
	)
	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.Points

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrap(err, "query points")
		}
		pointsSeries = result
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		table := p.tables.ExpHistograms

		result, err := query(ctx, table)
		if err != nil {
			return errors.Wrap(err, "query exponential histogram")
		}
		expHistSeries = result

		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	pointsSeries = append(pointsSeries, expHistSeries...)
	if sortSeries {
		slices.SortFunc(pointsSeries, func(a, b onlyLabelsSeries) int {
			return labels.Compare(a.Labels(), b.Labels())
		})
	}
	return newSeriesSet(pointsSeries), nil
}

func (p *promQuerier) buildSeriesQuery(
	table string, column chsql.ResultColumn,
	start, end time.Time,
	matcherSets [][]*labels.Matcher,
	mapping map[string]string,
) (*chsql.SelectQuery, error) {
	query := chsql.Select(table, column).
		Distinct(true).
		Where(chsql.InTimeRange("timestamp", start, end))

	sets := make([]chsql.Expr, 0, len(matcherSets))
	for _, set := range matcherSets {
		matchers := make([]chsql.Expr, 0, len(set))
		for _, m := range set {
			selectors := []chsql.Expr{
				chsql.Ident("name_normalized"),
			}
			if name := m.Name; name != labels.MetricName {
				if mapped, ok := mapping[name]; ok {
					name = mapped
				}
				selectors = []chsql.Expr{
					attrSelector(colAttrs, name),
					attrSelector(colScope, name),
					attrSelector(colResource, name),
				}
			}

			matcher, err := promQLLabelMatcher(selectors, m.Type, m.Value)
			if err != nil {
				return query, err
			}
			matchers = append(matchers, matcher)
		}
		sets = append(sets, chsql.JoinAnd(matchers...))
	}

	return query.
		Where(chsql.JoinOr(sets...)).
		Order(chsql.Ident("timestamp"), chsql.Asc), nil
}

type onlyLabelsSeries struct {
	labels labels.Labels
}

var _ storage.Series = onlyLabelsSeries{}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (s onlyLabelsSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns an iterator of the data of the series.
// The iterator passed as argument is for re-use, if not nil.
// Depending on implementation, the iterator can
// be re-used or a new iterator can be allocated.
func (onlyLabelsSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}
