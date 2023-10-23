package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/internal/metricsharding"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		const (
			rate     = 5000
			tenantID = 222
		)
		yc, err := ythttp.NewClient(&yt.Config{
			DisableProxyDiscovery: true,
		})
		if err != nil {
			return errors.Wrap(err, "yt.NewClient")
		}

		sharder := metricsharding.NewSharder(yc, nil, metricsharding.ShardingOptions{})
		now := time.Now()
		if err := sharder.CreateTenant(ctx, tenantID, now); err != nil {
			return errors.Wrap(err, "sharder.CreateTenant")
		}

		rnd := rand.New(rand.NewSource(1)) // #nosec G404
		for {
			var points []any
			var rh, ah otelstorage.Hash
			_, _ = rnd.Read(rh[:])
			_, _ = rnd.Read(ah[:])
			now = now.Add(time.Minute)
			for j := 0; j < rate; j++ {
				delta := time.Duration(j) * time.Millisecond
				ts := now.Add(delta)
				points = append(points, metricstorage.Point{
					Metric:        "foo",
					ResourceHash:  rh,
					AttributeHash: ah,
					Timestamp:     otelstorage.NewTimestampFromTime(ts),
					Point:         float64(j),
				})
			}
			if err := yc.InsertRows(ctx, "//oteldb/metrics/tenant_222/active/points", points, nil); err != nil {
				return errors.Wrap(err, "yc.InsertRows")
			}
		}
	})
}
