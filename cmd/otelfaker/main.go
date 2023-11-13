package main

import (
	"context"
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

func getLogs(ctx context.Context, tracer trace.Tracer, now time.Time) plog.Logs {
	_, span := tracer.Start(ctx, "getLogs")
	defer span.End()
	var (
		spanContext = span.SpanContext()
		traceID     = spanContext.TraceID()
		spanID      = spanContext.SpanID()
	)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "testHost")
	rl.SetSchemaUrl("resource_schema")
	il := rl.ScopeLogs().AppendEmpty()
	il.Scope().SetName("name")
	il.Scope().SetVersion("version")
	il.Scope().Attributes().PutStr("oteldb.name", "testDB")
	il.Scope().SetDroppedAttributesCount(1)
	il.SetSchemaUrl("scope_schema")
	lg := il.LogRecords().AppendEmpty()
	lg.SetSeverityNumber(plog.SeverityNumber(logspb.SeverityNumber_SEVERITY_NUMBER_INFO))
	lg.SetSeverityText("Info")
	lg.SetFlags(plog.LogRecordFlags(logspb.LogRecordFlags_LOG_RECORD_FLAGS_DO_NOT_USE))
	lg.SetTraceID(pcommon.TraceID(traceID))
	lg.SetSpanID(pcommon.SpanID(spanID))
	lg.Body().SetStr("hello world")
	lg.SetTimestamp(pcommon.Timestamp(now.UnixNano()))
	lg.SetObservedTimestamp(pcommon.Timestamp(now.UnixNano()))
	lg.Attributes().PutStr("sdkVersion", "1.0.1")
	lg.Attributes().PutStr("http.method", "GET")
	lg.Attributes().PutBool("http.server", true)
	lg.Attributes().PutInt("http.status_code", 200)
	lg.Attributes().PutStr("http.url", "https://example.com")
	lg.Attributes().PutStr("http.user_agent", "test-agent")
	lg.Attributes().PutDouble("http.duration_seconds", 1.1054)
	lg.Attributes().PutInt("http.duration", (time.Second + time.Millisecond*105).Nanoseconds())
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
		conn, err := grpc.DialContext(ctx, "oteldb.faster.svc.cluster.local:4317",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOptions...)),
		)
		if err != nil {
			return errors.Wrap(err, "dial oteldb")
		}
		client := plogotlp.NewGRPCClient(conn)
		tracer := m.TracerProvider().Tracer("otelfaker")
		for now := range time.NewTicker(time.Second).C {
			if _, err := client.Export(ctx, plogotlp.NewExportRequestFromLogs(getLogs(ctx, tracer, now))); err != nil {
				return errors.Wrap(err, "send logs")
			}
		}
		return nil
	})
}
