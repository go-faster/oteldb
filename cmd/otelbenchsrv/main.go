package main

import (
	"context"
	"net"
	"time"

	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
	points := im.SetEmptyGauge().DataPoints().AppendEmpty()
	points.SetIntValue(0)
	return m
}

type noopServer struct {
	pmetricotlp.UnimplementedGRPCServer
	count atomic.Uint64
}

func (n *noopServer) Export(ctx context.Context, request pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	n.count.Add(1)
	return pmetricotlp.NewExportResponse(), nil
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		srv := grpc.NewServer()
		h := &noopServer{}
		go func() {
			t := time.NewTicker(time.Second)
			for range t.C {
				lg.Info("Got", zap.Uint64("n", h.count.Swap(0)))
			}
		}()
		go func() {
			<-ctx.Done()
			srv.GracefulStop()
		}()
		pmetricotlp.RegisterGRPCServer(srv, h)
		ln, err := net.Listen("tcp", "127.0.0.1:3951")
		if err != nil {
			return err
		}
		return srv.Serve(ln)
	})
}
