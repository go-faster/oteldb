package metricsharding

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Consumer consumes given metrics and inserts them using given Inserter.
type Consumer struct {
	yc yt.Client

	shardOpts ShardingOptions
}

// NewConsumer creates new Consumer.
func NewConsumer(yc yt.Client, shardOpts ShardingOptions) *Consumer {
	shardOpts.SetDefaults()
	return &Consumer{
		yc:        yc,
		shardOpts: shardOpts,
	}
}

// ConsumeMetrics implements otelreceiver.Consumer.
func (c *Consumer) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	batches, err := c.mapMetrics(ctx, metrics)
	if err != nil {
		return err
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	for id, batch := range batches {
		id, batch := id, batch
		grp.Go(func() error {
			ctx := grpCtx
			return c.insertBatch(ctx, id, batch)
		})
	}
	return grp.Wait()
}

func (c *Consumer) insertBatch(ctx context.Context, id TenantID, batch *InsertBatch) error {
	activePath := c.shardOpts.TenantPath(id)

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		return insertDynamicSlice(ctx, c.yc, activePath.Child("points"), batch.Points)
	})
	batch.Attributes.Each(func(at time.Time, v []metricstorage.Attributes) {
		table := activePath.Child(at.Format(timeBlockLayout))
		grp.Go(func() error {
			ctx := grpCtx
			return insertDynamicSlice(ctx, c.yc, table, v)
		})
	})
	batch.Resource.Each(func(at time.Time, v []metricstorage.Resource) {
		table := activePath.Child(at.Format(timeBlockLayout))
		grp.Go(func() error {
			ctx := grpCtx
			return insertDynamicSlice(ctx, c.yc, table, v)
		})
	})

	return grp.Wait()
}

func insertDynamicSlice[T interface {
	YTSchema() schema.Schema
}](
	ctx context.Context,
	yc yt.Client,
	table ypath.Path,
	data []T,
) (rerr error) {
	var zero T
	if err := migrate.EnsureTables(ctx, yc,
		map[ypath.Path]migrate.Table{
			table: {
				Schema: zero.YTSchema(),
			},
		},
		migrate.OnConflictTryAlter(ctx, yc),
	); err != nil {
		return errors.Wrap(err, "create table")
	}

	bw := yc.NewRowBatchWriter()
	defer func() {
		if rerr != nil {
			_ = bw.Rollback()
		}
	}()

	for _, e := range data {
		if err := bw.Write(e); err != nil {
			return errors.Wrapf(err, "write %T", e)
		}
	}

	if err := bw.Commit(); err != nil {
		return errors.Wrap(err, "commit")
	}
	var (
		update        = true
		insertOptions = &yt.InsertRowsOptions{
			Update: &update,
		}
	)
	return yc.InsertRowBatch(ctx, table, bw.Batch(), insertOptions)
}

func (c *Consumer) mapMetrics(ctx context.Context, metrics pmetric.Metrics) (batches map[TenantID]*InsertBatch, _ error) {
	var (
		lg = zctx.From(ctx)

		getTenantBatch = func(id TenantID) *InsertBatch {
			if batches == nil {
				batches = map[int64]*InsertBatch{}
			}
			b, ok := batches[id]
			if !ok {
				b = &InsertBatch{
					Resource: ResourceInsert{
						Delta: c.shardOpts.AttributeDelta,
					},
					Attributes: AttributesInsert{
						Delta: c.shardOpts.AttributeDelta,
					},
				}
				batches[id] = b
			}
			return b
		}

		addPoints = func(
			name string,
			res metricstorage.Resource,
			slices pmetric.NumberDataPointSlice,
		) error {
			for i := 0; i < slices.Len(); i++ {
				point := slices.At(i)
				ts := point.Timestamp()
				attrs := point.Attributes()

				id, ok := c.shardOpts.ExtractTenant(res.Attrs.AsMap(), attrs)
				if !ok {
					lg.Warn("Can't extract tenant",
						zap.String("metric_name", name),
					)
					continue
				}
				b := getTenantBatch(id)

				attrHash := otelstorage.AttrHash(attrs)
				b.Attributes.Add(ts.AsTime(), AttributesKey{name, attrHash}, metricstorage.Attributes{
					Metric: name,
					Hash:   attrHash,
					Attrs:  otelstorage.Attrs(attrs),
				})
				b.Resource.Add(ts.AsTime(), res.Hash, res)

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

				b.Points = append(b.Points, metricstorage.Point{
					Metric:        name,
					ResourceHash:  res.Hash,
					AttributeHash: attrHash,
					Timestamp:     point.Timestamp(),
					Point:         val,
				})
			}
			return nil
		}

		resMetrics = metrics.ResourceMetrics()
	)
	for i := 0; i < resMetrics.Len(); i++ {
		resMetric := resMetrics.At(i)

		resAttrs := resMetric.Resource().Attributes()
		res := metricstorage.Resource{
			Hash:  otelstorage.AttrHash(resAttrs),
			Attrs: otelstorage.Attrs(resAttrs),
		}

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
					if err := addPoints(name, res, gauge.DataPoints()); err != nil {
						return nil, err
					}
				case pmetric.MetricTypeSum:
					sum := record.Sum()
					if err := addPoints(name, res, sum.DataPoints()); err != nil {
						return nil, err
					}
				case pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary:
					// FIXME(tdakkota): ignore for now.
				default:
					return nil, errors.Errorf("unexpected metric %q type %v", name, typ)
				}
			}
		}
	}
	return batches, nil
}

type (
	// ResourceInsert is a shorthand for resource attributes insert batch.
	ResourceInsert = TimeMap[otelstorage.Hash, metricstorage.Resource]
	// AttributesInsert is a shorthand for attributes insert batch.
	AttributesInsert = TimeMap[AttributesKey, metricstorage.Attributes]

	// InsertBatch is a metrics insert batch.
	InsertBatch struct {
		Points     []metricstorage.Point
		Attributes AttributesInsert
		Resource   ResourceInsert
	}
)
