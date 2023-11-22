package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/sync/errgroup"
)

// ConsumeMetrics inserts given metrics.
func (i *Inserter) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	var (
		points = newMetricColumns()

		labels        = newLabelsColumns()
		collectLabels = func(m pcommon.Map) {
			m.Range(func(k string, v pcommon.Value) bool {
				labels.name.Append(k)
				// FIXME(tdakkota): annoying allocations
				labels.value.Append(v.AsString())
				return true
			})
		}
	)

	if err := i.mapMetrics(points, metrics, collectLabels); err != nil {
		return errors.Wrap(err, "map metrics")
	}

	{
		grp, grpCtx := errgroup.WithContext(ctx)

		grp.Go(func() error {
			ctx := grpCtx

			input := points.Input()
			if err := i.ch.Do(ctx, ch.Query{
				Body:  input.Into(i.tables.Points),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert points")
			}
			return nil
		})
		grp.Go(func() error {
			ctx := grpCtx

			input := labels.Input()
			if err := i.ch.Do(ctx, ch.Query{
				Body:  input.Into(i.tables.Labels),
				Input: input,
			}); err != nil {
				return errors.Wrap(err, "insert labels")
			}
			return nil
		})
		if err := grp.Wait(); err != nil {
			return err
		}
	}

	return nil
}

func (i *Inserter) mapMetrics(c *metricColumns, metrics pmetric.Metrics, collectLabels func(attrs pcommon.Map)) error {
	var (
		addPoints = func(
			name string,
			res pcommon.Map,
			slices pmetric.NumberDataPointSlice,
		) error {
			for i := 0; i < slices.Len(); i++ {
				point := slices.At(i)
				ts := point.Timestamp().AsTime()
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

				collectLabels(attrs)
				c.name.Append(name)
				c.timestamp.Append(ts)
				c.value.Append(val)
				c.attributes.Append(encodeAttributes(attrs))
				c.resource.Append(encodeAttributes(res))
			}
			return nil
		}

		resMetrics = metrics.ResourceMetrics()
	)
	for i := 0; i < resMetrics.Len(); i++ {
		resMetric := resMetrics.At(i)
		resAttrs := resMetric.Resource().Attributes()
		collectLabels(resAttrs)

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
					if err := addPoints(name, resAttrs, gauge.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeSum:
					sum := record.Sum()
					if err := addPoints(name, resAttrs, sum.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary:
					// FIXME(tdakkota): ignore for now.
				default:
					return errors.Errorf("unexpected metric %q type %v", name, typ)
				}
			}
		}
	}

	return nil
}
