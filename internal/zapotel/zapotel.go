// Package zapotel provides OpenTelemetry logs exporter zap core implementation.
package zapotel

import (
	"context"
	"encoding/hex"
	"math"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap/zapcore"
)

// New initializes new zapcore.Core from grpc client and resource.
func New(res *resource.Resource, client plogotlp.GRPCClient) zapcore.Core {
	return &contextObserver{
		client: client,
		res:    res,
	}
}

type contextObserver struct {
	zapcore.LevelEnabler
	context []zapcore.Field
	client  plogotlp.GRPCClient
	res     *resource.Resource
}

var (
	_ zapcore.Core         = (*contextObserver)(nil)
	_ zapcore.LevelEnabler = (*contextObserver)(nil)
)

func (co *contextObserver) Level() zapcore.Level {
	return zapcore.LevelOf(co.LevelEnabler)
}

func (co *contextObserver) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if co.Enabled(ent.Level) {
		return ce.AddCore(ent, co)
	}
	return ce
}

func (co *contextObserver) With(fields []zapcore.Field) zapcore.Core {
	return &contextObserver{
		LevelEnabler: co.LevelEnabler,
		context:      append(co.context[:len(co.context):len(co.context)], fields...),
	}
}

func (co *contextObserver) toLogs(ent zapcore.Entry, fields []zapcore.Field) plog.Logs {
	var (
		ld = plog.NewLogs()
		rl = ld.ResourceLogs().AppendEmpty()
	)
	{
		a := rl.Resource().Attributes()
		for _, kv := range co.res.Attributes() {
			k := string(kv.Key)
			switch kv.Value.Type() {
			case attribute.STRING:
				a.PutStr(k, kv.Value.AsString())
			case attribute.BOOL:
				a.PutBool(k, kv.Value.AsBool())
			default:
				a.PutStr(k, kv.Value.AsString())
			}
		}
	}

	il := rl.ScopeLogs().AppendEmpty()

	scope := il.Scope()
	scope.SetName("zapotel")
	scope.SetVersion("v0.1")

	lg := il.LogRecords().AppendEmpty()
	lg.Body().SetStr(ent.Message)
	lg.SetTimestamp(pcommon.NewTimestampFromTime(ent.Time))
	lg.SetObservedTimestamp(pcommon.NewTimestampFromTime(ent.Time))
	{
		a := lg.Attributes()
		var skipped uint32
		for _, f := range fields {
			k := f.Key
			switch f.Type {
			case zapcore.BoolType:
				a.PutBool(k, f.Integer == 1)
			case zapcore.StringType:
				l := len(f.String)
				if (k == "trace_id" && l == 32) || (k == "span_id" && l == 16) {
					// Checking for tracing.
					var (
						traceID pcommon.TraceID
						spanID  pcommon.SpanID
					)
					v, err := hex.DecodeString(strings.ToLower(f.String))
					if err == nil {
						switch k {
						case "trace_id":
							copy(traceID[:], v)
							lg.SetTraceID(traceID)
						case "span_id":
							copy(spanID[:], v)
							lg.SetSpanID(spanID)
						}
						// Don't add as regular string.
						continue
					}
				}
				a.PutStr(k, f.String)
			case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type,
				zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
				a.PutInt(k, f.Integer)
			case zapcore.Float32Type:
				a.PutDouble(k, float64(math.Float32frombits(uint32(f.Integer))))
			case zapcore.Float64Type:
				a.PutDouble(k, math.Float64frombits(uint64(f.Integer)))
			default:
				// Time, duration, "any", ...
				// TODO(ernado): support
				skipped++
			}
		}
		if skipped > 0 {
			scope.SetDroppedAttributesCount(skipped)
		}
	}
	return ld
}

func (co *contextObserver) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	all := make([]zapcore.Field, 0, len(fields)+len(co.context))
	all = append(all, co.context...)
	all = append(all, fields...)

	ctx := context.TODO()

	logs := co.toLogs(ent, all)
	if _, err := co.client.Export(ctx, plogotlp.NewExportRequestFromLogs(logs)); err != nil {
		return errors.Wrap(err, "send logs")
	}

	return nil
}

func (co *contextObserver) Sync() error { return nil }
