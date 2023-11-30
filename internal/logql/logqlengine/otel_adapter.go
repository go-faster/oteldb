package logqlengine

import (
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logstorage"
)

// LineFromRecord returns a JSON line from a log record.
func LineFromRecord(record logstorage.Record) string {
	// Create JSON object from record.
	e := &jx.Encoder{}
	e.Obj(func(e *jx.Encoder) {
		e.Field("msg", func(e *jx.Encoder) {
			e.Str(record.Body)
		})
		record.Attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
			e.Field(k, func(e *jx.Encoder) {
				switch v.Type() {
				case pcommon.ValueTypeStr:
					e.Str(v.Str())
				case pcommon.ValueTypeBool:
					e.Bool(v.Bool())
				case pcommon.ValueTypeInt:
					e.Int64(v.Int())
				case pcommon.ValueTypeDouble:
					e.Float64(v.Double())
				default:
					// Fallback.
					e.Str(v.AsString())
				}
			})
			return true
		})
	})
	return e.String()
}
