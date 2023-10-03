package main

import (
	"context"
	"flag"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newMetrics() pmetric.Metrics {
	m := pmetric.NewMetrics()
	rl := m.ResourceMetrics().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "testHost")
	rl.SetSchemaUrl("resource_schema")
	il := rl.ScopeMetrics().AppendEmpty()
	il.Scope().SetName("name")
	il.Scope().SetVersion("version")
	il.Scope().Attributes().PutStr("oteldb.name", "testDB")
	il.Scope().SetDroppedAttributesCount(1)
	il.SetSchemaUrl("scope_schema")
	im := il.Metrics().AppendEmpty()
	im.SetName("metric_name")
	im.SetDescription("metric_description")
	dp := im.SetEmptyGauge().DataPoints()
	for i := 0; i < 10000; i++ {
		dp.AppendEmpty().SetIntValue(0)
	}

	return m
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		var arg struct {
			Jobs int
		}
		flag.IntVar(&arg.Jobs, "j", 16, "jobs")
		flag.Parse()
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < arg.Jobs; i++ {
			g.Go(func() error {
				conn, err := grpc.DialContext(ctx, "127.0.0.1:3951",
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					return errors.Wrap(err, "dial oteldb")
				}
				client := pmetricotlp.NewGRPCClient(conn)
				metrics := newMetrics()
				req := pmetricotlp.NewExportRequestFromMetrics(metrics)
				for {
					if _, err := client.Export(ctx, req); err != nil {
						return errors.Wrap(err, "send metrics")
					}
				}
			})
		}
		return g.Wait()
	})
}
