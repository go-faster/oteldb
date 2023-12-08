package zapotel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type mockClient struct {
	logs plog.LogRecordSlice
	plogotlp.GRPCClient
}

func (c *mockClient) Export(_ context.Context, request plogotlp.ExportRequest, _ ...grpc.CallOption) (plogotlp.ExportResponse, error) {
	resLogs := request.Logs().ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		scopeLogs := resLogs.At(i).ScopeLogs()
		for i := 0; i < scopeLogs.Len(); i++ {
			records := scopeLogs.At(i).LogRecords()
			for i := 0; i < records.Len(); i++ {
				records.At(i).CopyTo(c.logs.AppendEmpty())
			}
		}
	}
	return plogotlp.NewExportResponse(), nil
}

func TestLogger(t *testing.T) {
	a := require.New(t)

	mock := &mockClient{
		logs: plog.NewLogRecordSlice(),
	}
	core := New(zapcore.InfoLevel, resource.Empty(), mock)
	logger := zap.New(core).With(
		zap.Bool("test", true),
	)

	logger.Debug("debug message")
	logger.Info("info message",
		zap.String("trace_id", "4bf92f3577b34da6a3ce929d0e0e4736"),
		zap.String("span_id", "00f067aa0ba902b7"),
	)
	logger.Named("warner").Warn("warn message")
	logger.Error("error message", zap.Error(errors.New("test error")))

	// zapotel would send first record immediately.
	a.Equal(1, mock.logs.Len())
	a.NoError(core.Sync())
	a.Equal(3, mock.logs.Len())

	for i, expect := range []struct {
		message    string
		severity   plog.SeverityNumber
		traceID    string
		spanID     string
		attributes map[string]any
	}{
		{
			"info message",
			plog.SeverityNumberInfo,
			"4bf92f3577b34da6a3ce929d0e0e4736",
			"00f067aa0ba902b7",
			map[string]any{
				"test": true,
			},
		},
		{
			"warn message",
			plog.SeverityNumberWarn,
			"",
			"",
			map[string]any{
				"test":   true,
				"logger": "warner",
			},
		},
		{
			"error message",
			plog.SeverityNumberError,
			"",
			"",
			map[string]any{
				"test":  true,
				"error": "test error",
			},
		},
	} {
		record := mock.logs.At(i)
		a.Equal(expect.message, record.Body().AsString())
		a.Equal(expect.severity, record.SeverityNumber())
		a.Equal(expect.traceID, record.TraceID().String())
		a.Equal(expect.spanID, record.SpanID().String())
		a.Equal(expect.attributes, record.Attributes().AsRaw())
	}
}
