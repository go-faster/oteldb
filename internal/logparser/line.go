package logparser

import (
	"encoding/hex"
	"time"

	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// Line represents single parsed line that can be converted to [logstorage.Record].
type Line struct {
	Timestamp      otelstorage.Timestamp `json:"timestamp"`
	TraceID        otelstorage.TraceID   `json:"trace_id"`
	SpanID         otelstorage.SpanID    `json:"span_id"`
	Attrs          otelstorage.Attrs     `json:"attrs"`
	SeverityNumber plog.SeverityNumber   `json:"severity_number"`
	SeverityText   string                `json:"severity_text"`
	Body           string                `json:"body"`
}

func (l Line) String() string {
	e := &jx.Encoder{}
	e.SetIdent(2)
	l.Encode(e)
	return e.String()
}

// Encode line as json.
func (l Line) Encode(e *jx.Encoder) {
	e.Obj(func(e *jx.Encoder) {
		if l.SeverityNumber != 0 {
			e.Field("severity_number_str", func(e *jx.Encoder) {
				e.Str(l.SeverityNumber.String())
			})
			e.Field("severity_number", func(e *jx.Encoder) {
				e.Int64(int64(l.SeverityNumber))
			})
		}
		if l.SeverityText != "" {
			e.Field("severity_text", func(e *jx.Encoder) {
				e.Str(l.SeverityText)
			})
		}
		if l.Body != "" {
			e.Field("body", func(e *jx.Encoder) {
				e.Str(l.Body)
			})
		}
		if !l.Timestamp.AsTime().IsZero() && l.Timestamp != 0 {
			e.Field("timestamp", func(e *jx.Encoder) {
				e.Str(l.Timestamp.AsTime().Format(time.RFC3339Nano))
			})
		}
		if !l.Attrs.IsZero() {
			l.Attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
				return !e.Field(k, func(e *jx.Encoder) {
					encodeValue(v, e)
				})
			})
		}
		if !l.TraceID.IsEmpty() {
			e.Field("trace_id", func(e *jx.Encoder) {
				e.Str(hex.EncodeToString(l.TraceID[:]))
			})
		}
		if !l.SpanID.IsEmpty() {
			e.Field("span_id", func(e *jx.Encoder) {
				e.Str(hex.EncodeToString(l.SpanID[:]))
			})
		}
	})
}
