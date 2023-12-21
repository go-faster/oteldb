package chstorage

import (
	"cmp"
	"context"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// ConsumeMetrics inserts given metrics.
func (i *Inserter) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	b := newMetricBatch()
	if err := i.mapMetrics(b, metrics); err != nil {
		return errors.Wrap(err, "map metrics")
	}
	if err := b.Insert(ctx, i.tables, i.ch); err != nil {
		return errors.Wrap(err, "send batch")
	}
	return nil
}

type metricsBatch struct {
	points        *pointColumns
	histograms    *histogramColumns
	expHistograms *expHistogramColumns
	summaries     *summaryColumns
	exemplars     *exemplarColumns
	labels        map[[2]string]struct{}
}

func newMetricBatch() *metricsBatch {
	return &metricsBatch{
		points:        newPointColumns(),
		histograms:    newHistogramColumns(),
		expHistograms: newExpHistogramColumns(),
		summaries:     newSummaryColumns(),
		exemplars:     newExemplarColumns(),
		labels:        map[[2]string]struct{}{},
	}
}

func (b *metricsBatch) Insert(ctx context.Context, tables Tables, client *chpool.Pool) error {
	labelColumns := newLabelsColumns()
	for pair := range b.labels {
		key := pair[0]
		labelColumns.name.Append(otelstorage.KeyToLabel(key))
		labelColumns.key.Append(key)
		labelColumns.value.Append(pair[1])
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	type columns interface {
		Input() proto.Input
	}
	for _, table := range []struct {
		name    string
		columns columns
	}{
		{tables.Points, b.points},
		{tables.Histograms, b.histograms},
		{tables.ExpHistograms, b.expHistograms},
		{tables.Summaries, b.summaries},
		{tables.Exemplars, b.exemplars},
		{tables.Labels, labelColumns},
	} {
		table := table
		grp.Go(func() error {
			ctx := grpCtx

			input := table.columns.Input()
			if err := client.Do(ctx, ch.Query{
				Logger: zctx.From(ctx).Named("ch"),
				Body:   input.Into(table.name),
				Input:  input,
			}); err != nil {
				return errors.Wrapf(err, "insert %q", table.name)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "insert")
	}

	return nil
}

func (b *metricsBatch) addPoints(name string, res pcommon.Map, slice pmetric.NumberDataPointSlice) error {
	c := b.points

	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := point.Attributes()

		var val float64
		switch typ := point.ValueType(); typ {
		case pmetric.NumberDataPointValueTypeEmpty:
			// Just ignore it.
			continue
		case pmetric.NumberDataPointValueTypeInt:
			// TODO(tdakkota): check for overflow
			val = float64(point.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			val = point.DoubleValue()
		default:
			return errors.Errorf("unexpected metric %q value type: %v", name, typ)
		}

		b.addName(name)
		b.addLabels(attrs)

		if err := b.addExemplars(
			exemplarSeries{
				Name:       name,
				Timestamp:  ts,
				Attributes: attrs,
				Resource:   res,
			},
			point.Exemplars(),
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.mapping.Append(proto.Enum8(noMapping))
		c.value.Append(val)
		c.flags.Append(uint8(flags))
		c.attributes.Append(encodeAttributes(attrs))
		c.resource.Append(encodeAttributes(res))
	}
	return nil
}

func (b *metricsBatch) addHistogramPoints(name string, res pcommon.Map, slice pmetric.HistogramDataPointSlice) error {
	c := b.histograms
	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := point.Attributes()
		count := point.Count()
		sum := proto.Nullable[float64]{
			Set:   point.HasSum(),
			Value: point.Sum(),
		}
		_min := proto.Nullable[float64]{
			Set:   point.HasMin(),
			Value: point.Min(),
		}
		_max := proto.Nullable[float64]{
			Set:   point.HasMax(),
			Value: point.Max(),
		}
		bucketCounts := point.BucketCounts().AsRaw()
		explicitBounds := point.ExplicitBounds().AsRaw()

		b.addName(name)
		b.addLabels(attrs)
		// Save original histogram.
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.count.Append(count)
		c.sum.Append(sum)
		c.min.Append(_min)
		c.max.Append(_max)
		c.bucketCounts.Append(bucketCounts)
		c.explicitBounds.Append(explicitBounds)
		c.flags.Append(uint32(flags))
		c.attributes.Append(encodeAttributes(attrs))
		c.resource.Append(encodeAttributes(res))

		// Map histogram as set of series for Prometheus compatibility.
		series := mappedSeries{
			ts:    ts,
			flags: flags,
			attrs: attrs,
			res:   res,
		}
		if sum.Set {
			b.addMappedSample(
				series,
				name+"_sum",
				histogramSum,
				sum.Value,
				[2]string{},
			)
		}
		if _min.Set {
			b.addMappedSample(
				series,
				name+"_min",
				histogramMin,
				_min.Value,
				[2]string{},
			)
		}
		if _max.Set {
			b.addMappedSample(
				series,
				name+"_max",
				histogramMax,
				_max.Value,
				[2]string{},
			)
		}
		b.addMappedSample(
			series,
			name+"_count",
			histogramCount,
			float64(count),
			[2]string{},
		)

		var (
			cumCount     uint64
			bucketName   = name + "_bucket"
			bucketBounds = make([]histogramBucketBounds, 0, len(bucketCounts))
		)
		for i := 0; i < min(len(bucketCounts), len(explicitBounds)); i++ {
			bound := explicitBounds[i]
			cumCount += bucketCounts[i]

			key := [2]string{
				"le",
				strconv.FormatFloat(bound, 'f', -1, 64),
			}
			bucketBounds = append(bucketBounds, histogramBucketBounds{
				bound:     bound,
				bucketKey: key,
			})

			// Generate series with "_bucket" suffix and "le" label.
			b.addMappedSample(
				series,
				bucketName,
				histogramBucket,
				float64(cumCount),
				key,
			)
		}
		// Generate series with "_bucket" suffix and "le" label.
		{
			key := [2]string{
				"le",
				"+Inf",
			}
			bucketBounds = append(bucketBounds, histogramBucketBounds{
				bound:     math.Inf(1),
				bucketKey: key,
			})

			b.addMappedSample(
				series,
				bucketName,
				histogramBucket,
				float64(cumCount),
				key,
			)
		}

		if err := b.addHistogramExemplars(
			exemplarSeries{
				// Note: we're using the "_bucket" name, not the original.
				Name:       bucketName,
				Timestamp:  ts,
				Attributes: attrs,
				Resource:   res,
			},
			point.Exemplars(),
			bucketBounds,
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
	}
	return nil
}

func (b *metricsBatch) addHistogramExemplars(
	p exemplarSeries,
	exemplars pmetric.ExemplarSlice,
	bounds []histogramBucketBounds,
) error {
	slices.SortFunc(bounds, func(a, b histogramBucketBounds) int {
		return cmp.Compare(a.bound, b.bound)
	})
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		for _, bound := range bounds {
			var val float64
			switch typ := e.ValueType(); typ {
			case pmetric.ExemplarValueTypeEmpty:
				// Just ignore it.
				return nil
			case pmetric.ExemplarValueTypeInt:
				// TODO(tdakkota): check for overflow
				val = float64(e.IntValue())
			case pmetric.ExemplarValueTypeDouble:
				val = e.DoubleValue()
			default:
				return errors.Errorf("unexpected exemplar value type: %v", typ)
			}

			if val <= bound.bound {
				if err := b.addExemplar(p, e, bound.bucketKey); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type histogramBucketBounds struct {
	bound     float64
	bucketKey [2]string
}

func (b *metricsBatch) addExpHistogramPoints(name string, res pcommon.Map, slice pmetric.ExponentialHistogramDataPointSlice) error {
	var (
		c          = b.expHistograms
		mapBuckets = func(b pmetric.ExponentialHistogramDataPointBuckets) (offset int32, counts []uint64) {
			offset = b.Offset()
			counts = b.BucketCounts().AsRaw()
			return offset, counts
		}
	)
	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := point.Attributes()
		count := point.Count()
		sum := proto.Nullable[float64]{
			Set:   point.HasSum(),
			Value: point.Sum(),
		}
		min := proto.Nullable[float64]{
			Set:   point.HasMin(),
			Value: point.Min(),
		}
		max := proto.Nullable[float64]{
			Set:   point.HasMax(),
			Value: point.Max(),
		}
		scale := point.Scale()
		zerocount := point.ZeroCount()

		positiveOffset, positiveBucketCounts := mapBuckets(point.Positive())
		negativeOffset, negativeBucketCounts := mapBuckets(point.Negative())

		b.addName(name)
		b.addLabels(attrs)
		if err := b.addExemplars(
			exemplarSeries{
				Name:       name,
				Timestamp:  ts,
				Attributes: attrs,
				Resource:   res,
			},
			point.Exemplars(),
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.count.Append(count)
		c.sum.Append(sum)
		c.min.Append(min)
		c.max.Append(max)
		c.scale.Append(scale)
		c.zerocount.Append(zerocount)
		c.positiveOffset.Append(positiveOffset)
		c.positiveBucketCounts.Append(positiveBucketCounts)
		c.negativeOffset.Append(negativeOffset)
		c.negativeBucketCounts.Append(negativeBucketCounts)
		c.flags.Append(uint32(flags))
		c.attributes.Append(encodeAttributes(attrs))
		c.resource.Append(encodeAttributes(res))
	}
	return nil
}

func (b *metricsBatch) addSummaryPoints(name string, res pcommon.Map, slice pmetric.SummaryDataPointSlice) error {
	c := b.summaries
	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := point.Attributes()
		count := point.Count()
		sum := point.Sum()
		var (
			qv = point.QuantileValues()

			quantiles = make([]float64, qv.Len())
			values    = make([]float64, qv.Len())
		)
		for i := 0; i < qv.Len(); i++ {
			p := qv.At(i)

			quantiles[i] = p.Quantile()
			values[i] = p.Value()
		}

		b.addName(name)
		b.addLabels(attrs)
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.count.Append(count)
		c.sum.Append(sum)
		c.quantiles.Append(quantiles)
		c.values.Append(values)
		c.flags.Append(uint32(flags))
		c.attributes.Append(encodeAttributes(attrs))
		c.resource.Append(encodeAttributes(res))

		series := mappedSeries{
			ts:    ts,
			flags: flags,
			attrs: attrs,
			res:   res,
		}
		b.addMappedSample(series, name+"_count", summaryCount, float64(count), [2]string{})
		b.addMappedSample(series, name+"_sum", summarySum, sum, [2]string{})

		for i := 0; i < min(len(quantiles), len(values)); i++ {
			quantile := quantiles[i]
			value := values[i]

			// Generate series with "quantile" label.
			b.addMappedSample(series, name, summaryQuantile, value, [2]string{
				"quantile",
				strconv.FormatFloat(quantile, 'f', -1, 64),
			})
		}
	}
	return nil
}

type mappedSeries struct {
	ts         time.Time
	flags      pmetric.DataPointFlags
	attrs, res pcommon.Map
}

func (b *metricsBatch) addMappedSample(
	series mappedSeries,
	name string,
	mapping metricMapping,
	val float64,
	bucketKey [2]string,
) {
	c := b.points
	c.name.Append(name)
	c.timestamp.Append(series.ts)
	c.mapping.Append(proto.Enum8(mapping))
	c.value.Append(val)
	c.flags.Append(uint8(series.flags))
	c.attributes.Append(encodeAttributes(series.attrs, bucketKey))
	c.resource.Append(encodeAttributes(series.res, bucketKey))
}

type exemplarSeries struct {
	Name       string
	Timestamp  time.Time
	Attributes pcommon.Map
	Resource   pcommon.Map
}

func (b *metricsBatch) addExemplars(p exemplarSeries, exemplars pmetric.ExemplarSlice) error {
	for i := 0; i < exemplars.Len(); i++ {
		if err := b.addExemplar(p, exemplars.At(i), [2]string{}); err != nil {
			return err
		}
	}
	return nil
}

func (b *metricsBatch) addExemplar(p exemplarSeries, e pmetric.Exemplar, bucketKey [2]string) error {
	c := b.exemplars

	var val float64
	switch typ := e.ValueType(); typ {
	case pmetric.ExemplarValueTypeEmpty:
		// Just ignore it.
		return nil
	case pmetric.ExemplarValueTypeInt:
		// TODO(tdakkota): check for overflow
		val = float64(e.IntValue())
	case pmetric.ExemplarValueTypeDouble:
		val = e.DoubleValue()
	default:
		return errors.Errorf("unexpected exemplar value type: %v", typ)
	}

	c.name.Append(p.Name)
	c.timestamp.Append(p.Timestamp)

	c.filteredAttributes.Append(encodeAttributes(e.FilteredAttributes()))
	c.exemplarTimestamp.Append(e.Timestamp().AsTime())
	c.value.Append(val)
	c.spanID.Append(e.SpanID())
	c.traceID.Append(e.TraceID())

	c.attributes.Append(encodeAttributes(p.Attributes, bucketKey))
	c.resource.Append(encodeAttributes(p.Resource))
	return nil
}

func (b *metricsBatch) addName(name string) {
	b.labels[[2]string{labels.MetricName, name}] = struct{}{}
}

func (b *metricsBatch) addLabels(m pcommon.Map) {
	m.Range(func(key string, value pcommon.Value) bool {
		pair := [2]string{
			key,
			// FIXME(tdakkota): annoying allocations
			value.AsString(),
		}
		b.labels[pair] = struct{}{}
		return true
	})
}

func (i *Inserter) mapMetrics(b *metricsBatch, metrics pmetric.Metrics) error {
	resMetrics := metrics.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		resMetric := resMetrics.At(i)
		resAttrs := resMetric.Resource().Attributes()
		b.addLabels(resAttrs)

		scopeMetrics := resMetric.ScopeMetrics()
		for i := 0; i < scopeMetrics.Len(); i++ {
			scopeLog := scopeMetrics.At(i)

			records := scopeLog.Metrics()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				name := record.Name()

				switch typ := record.Type(); typ {
				case pmetric.MetricTypeGauge:
					gauge := record.Gauge()
					if err := b.addPoints(name, resAttrs, gauge.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeSum:
					sum := record.Sum()
					if err := b.addPoints(name, resAttrs, sum.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeHistogram:
					hist := record.Histogram()
					if err := b.addHistogramPoints(name, resAttrs, hist.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeExponentialHistogram:
					hist := record.ExponentialHistogram()
					if err := b.addExpHistogramPoints(name, resAttrs, hist.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeSummary:
					summary := record.Summary()
					if err := b.addSummaryPoints(name, resAttrs, summary.DataPoints()); err != nil {
						return err
					}
				default:
					return errors.Errorf("unexpected metric %q type %v", name, typ)
				}
			}
		}
	}

	return nil
}
