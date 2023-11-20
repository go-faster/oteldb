package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// ConsumeMetrics inserts given metrics.
func (i *Inserter) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	c := newMetricColumns()
	if err := i.mapMetrics(c, metrics); err != nil {
		return errors.Wrap(err, "map metrics")
	}

	input := c.Input()
	if err := i.ch.Do(ctx, ch.Query{
		Body:  input.Into(i.tables.Points),
		Input: input,
	}); err != nil {
		return errors.Wrap(err, "insert")
	}

	return nil
}

func (i *Inserter) mapMetrics(c *metricColumns, metrics pmetric.Metrics) error {
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
				case pmetric.NumberDataPointValueTypeInt:
					// TODO(tdakkota): check for overflow
					val = float64(point.IntValue())
				case pmetric.NumberDataPointValueTypeDouble:
					val = point.DoubleValue()
				default:
					return errors.Errorf("unexpected metric %q value type: %v", name, typ)
				}

				c.name.Append(name)
				c.ts.Append(ts)
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
