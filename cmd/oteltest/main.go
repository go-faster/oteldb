package main

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/metricsharding"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		var arg struct {
			Proxy          string
			Token          string
			ProxyDiscovery bool
			TotalPoints    int
			BatchSize      int
			TenantID       int
			Workers        int
		}
		flag.IntVar(&arg.Workers, "j", 1, "concurrent jobs")
		flag.IntVar(&arg.TenantID, "t", 222, "tenant id")
		flag.IntVar(&arg.TotalPoints, "p", 1_000_000_000, "total points to insert")
		flag.IntVar(&arg.BatchSize, "b", 5_000, "batch size")
		flag.StringVar(&arg.Proxy, "proxy", "localhost:8000", "proxy address")
		flag.StringVar(&arg.Token, "token", "admin", "token")
		flag.BoolVar(&arg.ProxyDiscovery, "proxy-discovery", false, "proxy discovery")
		flag.Parse()

		yc, err := ythttp.NewClient(&yt.Config{
			Proxy:                 arg.Proxy,
			Token:                 arg.Token,
			DisableProxyDiscovery: !arg.ProxyDiscovery,
		})
		if err != nil {
			return errors.Wrap(err, "yt.NewClient")
		}

		sharder := metricsharding.NewSharder(yc, metricsharding.ShardingOptions{})
		if err := sharder.CreateTenant(ctx, metricsharding.TenantID(arg.TenantID), time.Now()); err != nil {
			return errors.Wrap(err, "sharder.CreateTenant")
		}
		var (
			tenant = sharder.TenantPath(metricsharding.TenantID(arg.TenantID))
			active = tenant.Child("active")
			now    = time.Now()
		)

		var loaded atomic.Uint64
		data := make(chan []any, arg.Workers)
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < arg.Workers; i++ {
			g.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case points, ok := <-data:
						if !ok {
							return nil
						}
						bo := backoff.NewExponentialBackOff()
						fn := func() error {
							return yc.InsertRows(ctx, active.Child("points"), points, nil)
						}
						if err := backoff.RetryNotify(fn, backoff.WithContext(bo, ctx), func(err error, duration time.Duration) {
							lg.Warn("retrying", zap.Error(err), zap.Duration("duration", duration))
						}); err != nil {
							return errors.Wrap(err, "yc.InsertRows")
						}
						loaded.Add(uint64(len(points)))
					}
				}
			})
		}
		g.Go(func() error {
			defer close(data)
			for i := 0; i < arg.TotalPoints/arg.BatchSize; i++ {
				var points []any
				rh := sha1.Sum([]byte(fmt.Sprintf("r%d", i)))
				ah := sha1.Sum([]byte(fmt.Sprintf("a%d", i)))
				for j := 0; j < arg.BatchSize; j++ {
					delta := time.Duration(i+j) * time.Millisecond
					ts := now.Add(delta)
					points = append(points, metricstorage.Point{
						Metric:        "some.metric.name",
						ResourceHash:  rh[:],
						AttributeHash: ah[:],
						Timestamp:     otelstorage.NewTimestampFromTime(ts),
						Point:         float64(j),
					})
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case data <- points:
				}
			}
			return nil
		})
		g.Go(func() error {
			ticker := time.NewTicker(time.Second)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					value := float64(loaded.Swap(0))
					lg.Info("loaded", zap.String("points", humanize.SI(value, "P")))
				}
			}
		})

		return g.Wait()
	})
}
