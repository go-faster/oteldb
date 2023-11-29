package main

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getLogs(ctx context.Context, tracer trace.Tracer, rnd *rand.Rand, now time.Time) plog.Logs {
	_, span := tracer.Start(ctx, "getLogs")
	defer span.End()
	var (
		spanContext = span.SpanContext()
		traceID     = spanContext.TraceID()
		spanID      = spanContext.SpanID()
		ld          = plog.NewLogs()
		rl          = ld.ResourceLogs().AppendEmpty()
	)
	{
		a := rl.Resource().Attributes()
		a.PutStr("service.name", "testService")
		a.PutStr("service.version", "1.0.0")
		a.PutStr("service.instance.id", "testInstance")
		a.PutStr("service.namespace", "testNamespace")
		a.PutStr("host.name", "testHost")
	}
	rl.SetSchemaUrl("resource_schema")
	il := rl.ScopeLogs().AppendEmpty()
	{
		s := il.Scope()
		s.SetName("name")
		s.SetVersion("version")
		s.Attributes().PutStr("oteldb.name", "testDB")
		s.SetDroppedAttributesCount(1)
	}
	il.SetSchemaUrl("scope_schema")
	lg := il.LogRecords().AppendEmpty()
	lg.SetSeverityNumber(plog.SeverityNumber(logspb.SeverityNumber_SEVERITY_NUMBER_INFO))
	lg.SetSeverityText("Info")
	lg.SetTraceID(pcommon.TraceID(traceID))
	lg.SetSpanID(pcommon.SpanID(spanID))
	lg.Body().SetStr("hello world")
	lg.SetTimestamp(pcommon.Timestamp(now.UnixNano()))
	lg.SetObservedTimestamp(pcommon.Timestamp(now.UnixNano()))
	{
		a := lg.Attributes()
		a.PutStr("sdkVersion", "1.0.1")
		a.PutStr("http.method", "GET")
		a.PutBool("http.server", true)
		a.PutInt("http.status_code", 200)
		if rnd.Float32() < 0.5 {
			a.PutStr("http.url", "https://example.com")
			a.PutStr("http.status_text", "OK")
		} else {
			a.PutStr("http.user_agent", "test-agent")
		}
		a.PutDouble("http.duration_seconds", 1.1054)
		a.PutInt("http.duration", (time.Second + time.Millisecond*105).Nanoseconds())
	}
	lg.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	return ld
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		// Generate fake telemetry signals to test oteldb.
		otelOptions := []otelgrpc.Option{
			otelgrpc.WithTracerProvider(m.TracerProvider()),
			otelgrpc.WithMeterProvider(m.MeterProvider()),
		}
		target := os.Getenv("OTEL_TARGET")
		if target == "" {
			target = "oteldb.faster.svc.cluster.local:4317"
		}
		conn, err := grpc.DialContext(ctx, target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOptions...)),
		)
		if err != nil {
			return errors.Wrap(err, "dial oteldb")
		}
		client := plogotlp.NewGRPCClient(conn)
		tracer := m.TracerProvider().Tracer("otelfaker")
		rnd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
		for now := range time.NewTicker(time.Second).C {
			if _, err := client.Export(ctx, plogotlp.NewExportRequestFromLogs(getLogs(ctx, tracer, rnd, now))); err != nil {
				return errors.Wrap(err, "send logs")
			}
		}
		return nil
	})
}
