// Package zapotel provides OpenTelemetry logs exporter zap core implementation.
package zapotel

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap/zapcore"
)

// New initializes new zapcore.Core from grpc client and resource.
func New(enab zapcore.LevelEnabler, res *resource.Resource, client plogotlp.GRPCClient) zapcore.Core {
	return &exporter{
		LevelEnabler: enab,

		sender: &sender{
			client:   client,
			res:      res,
			logs:     plog.NewLogs(),
			rate:     time.Second * 3,
			maxBatch: 5000,
		},
	}
}

type sender struct {
	client   plogotlp.GRPCClient
	res      *resource.Resource
	logs     plog.Logs
	rate     time.Duration
	maxBatch int
	mux      sync.Mutex
	sent     time.Time
}

func (s *sender) append(ent zapcore.Entry, fields []zapcore.Field) {
	// https://github.com/open-telemetry/oteps/blob/main/text/logs/0097-log-data-model.md#zap
	rl := s.logs.ResourceLogs().AppendEmpty()
	{
		a := rl.Resource().Attributes()
		for _, kv := range s.res.Attributes() {
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
	// TODO: update mapping from spec
	switch ent.Level {
	case zapcore.DebugLevel:
		lg.SetSeverityNumber(plog.SeverityNumberDebug)
	case zapcore.InfoLevel:
		lg.SetSeverityNumber(plog.SeverityNumberInfo)
	case zapcore.WarnLevel:
		lg.SetSeverityNumber(plog.SeverityNumberWarn)
	case zapcore.ErrorLevel:
		lg.SetSeverityNumber(plog.SeverityNumberError)
	case zapcore.DPanicLevel:
		lg.SetSeverityNumber(plog.SeverityNumberFatal)
	case zapcore.PanicLevel:
		lg.SetSeverityNumber(plog.SeverityNumberFatal)
	case zapcore.FatalLevel:
		lg.SetSeverityNumber(plog.SeverityNumberFatal)
	}
	lg.SetSeverityText(ent.Level.String())
	lg.SetTimestamp(pcommon.NewTimestampFromTime(ent.Time))
	lg.SetObservedTimestamp(pcommon.NewTimestampFromTime(ent.Time))
	{
		a := lg.Attributes()
		if ent.Caller.Defined {
			a.PutStr("caller", ent.Caller.TrimmedPath())
		}
		if ent.Stack != "" {
			a.PutStr("stack", ent.Stack)
		}
		if ent.LoggerName != "" {
			a.PutStr("logger", ent.LoggerName)
		}
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
			case zapcore.TimeType:
				a.PutInt(f.Key, f.Integer)
			case zapcore.TimeFullType:
				a.PutStr(k, f.Interface.(time.Time).Format(time.RFC3339Nano))
			case zapcore.ErrorType:
				encodeError(a, k, f.Interface.(error))
			case zapcore.DurationType:
				a.PutDouble(k, time.Duration(f.Integer).Seconds())
			default:
				// "Any", ...
				skipped++
			}
		}
		if skipped > 0 {
			scope.SetDroppedAttributesCount(skipped)
		}
	}
}

func (s *sender) send(ctx context.Context) error {
	req := plogotlp.NewExportRequestFromLogs(s.logs)
	if _, err := s.client.Export(ctx, req); err != nil {
		return errors.Wrap(err, "send logs")
	}
	s.logs = plog.NewLogs()
	s.sent = time.Now()
	return nil
}

func (s *sender) Send(ctx context.Context, ent zapcore.Entry, fields []zapcore.Field) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.append(ent, fields)
	if time.Since(s.sent) > s.rate || s.logs.LogRecordCount() >= s.maxBatch {
		return s.send(ctx)
	}
	return nil
}

type exporter struct {
	zapcore.LevelEnabler
	context []zapcore.Field
	sender  *sender
}

var (
	_ zapcore.Core         = (*exporter)(nil)
	_ zapcore.LevelEnabler = (*exporter)(nil)
)

func (e *exporter) Level() zapcore.Level {
	return zapcore.LevelOf(e.LevelEnabler)
}

func (e *exporter) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if e.Enabled(ent.Level) {
		return ce.AddCore(ent, e)
	}
	return ce
}

func (e *exporter) With(fields []zapcore.Field) zapcore.Core {
	return &exporter{
		LevelEnabler: e.LevelEnabler,

		context: append(e.context[:len(e.context):len(e.context)], fields...),
		sender:  e.sender,
	}
}

func (e *exporter) append(ent zapcore.Entry, fields []zapcore.Field) {

}

func encodeError(a pcommon.Map, key string, err error) {
	// TODO: update mapping from spec

	// Try to capture panics (from nil references or otherwise) when calling
	// the Error() method
	defer func() {
		if rerr := recover(); rerr != nil {
			// If it's a nil pointer, just say "<nil>". The likeliest causes are a
			// error that fails to guard against nil or a nil pointer for a
			// value receiver, and in either case, "<nil>" is a nice result.
			if v := reflect.ValueOf(err); v.Kind() == reflect.Ptr && v.IsNil() {
				a.PutStr(key, "<nil>")
			}
		}
	}()

	basic := err.Error()
	a.PutStr(key, basic)

	switch e := err.(type) {
	case interface{ Errors() []error }:
		for i, v := range e.Errors() {
			k := fmt.Sprintf("%s.%d", key, i)
			a.PutStr(k, v.Error())
		}
	case fmt.Formatter:
		verbose := fmt.Sprintf("%+v", e)
		if verbose != basic {
			// This is a rich error type, like those produced by
			// github.com/pkg/errors.
			a.PutStr(key+".verbose", verbose)
		}
	}
}

func (e *exporter) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	all := make([]zapcore.Field, 0, len(fields)+len(e.context))
	all = append(all, e.context...)
	all = append(all, fields...)
	return e.sender.Send(context.Background(), ent, all)
}

func (e *exporter) Sync() error { return nil }
