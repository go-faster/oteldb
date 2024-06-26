package logstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logparser"
)

// Consumer consumes given logs and inserts them using given Inserter.
type Consumer struct {
	inserter Inserter
}

// NewConsumer creates new Consumer.
func NewConsumer(i Inserter) *Consumer {
	return &Consumer{
		inserter: i,
	}
}

// ConsumeLogs implements otelreceiver.Consumer.
func (c *Consumer) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	labels := map[Label]struct{}{}
	addLabels := func(attrs pcommon.Map) {
		attrs.Range(func(k string, v pcommon.Value) bool {
			switch t := v.Type(); t {
			case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
			default:
				labels[Label{k, v.AsString()}] = struct{}{}
			}
			return true
		})
	}

	var (
		insertBatch []Record
		resLogs     = logs.ResourceLogs()
	)
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)
		res := resLog.Resource()
		addLabels(res.Attributes())

		scopeLogs := resLog.ScopeLogs()
		for i := 0; i < scopeLogs.Len(); i++ {
			scopeLog := scopeLogs.At(i)
			scope := scopeLog.Scope()
			addLabels(scope.Attributes())

			records := scopeLog.LogRecords()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				insertBatch = append(insertBatch, NewRecordFromOTEL(res, scope, record))
				addLabels(record.Attributes())
			}
		}
	}

	// Parse logs.
	for i, record := range insertBatch {
		if record.Attrs.IsZero() || record.ResourceAttrs.IsZero() {
			continue
		}

		// Assuming filelog.
		// Should contain "log" attribute.
		attrs := record.Attrs.AsMap()
		const logMessageKey = "log"
		v, ok := attrs.Get(logMessageKey)
		if !ok || v.Type() != pcommon.ValueTypeStr {
			continue
		}
		for _, parser := range []logparser.Parser{
			logparser.GenericJSONParser{},
			// Disable for lots of false-positive detections // logparser.LogFmtParser{},
		} {
			if !parser.Detect(v.Str()) {
				continue
			}
			data := []byte(v.Str())
			line, err := parser.Parse(data)
			if err != nil {
				continue
			}
			attrs.PutStr("logparser.type", parser.String())
			attrs.Remove(logMessageKey)
			if !line.Attrs.IsZero() {
				line.Attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
					target := attrs.PutEmpty(k)
					v.CopyTo(target)
					return true
				})
			}
			record.Body = line.Body
			if line.Timestamp != 0 {
				record.Timestamp = line.Timestamp
			}
			if line.SeverityNumber != 0 {
				record.SeverityNumber = line.SeverityNumber
				record.SeverityText = line.SeverityText
			}
			if !line.SpanID.IsEmpty() {
				record.SpanID = line.SpanID
			}
			if !line.TraceID.IsEmpty() {
				record.TraceID = line.TraceID
			}

			insertBatch[i] = record
			break
		}
	}

	if err := c.inserter.InsertRecords(ctx, insertBatch); err != nil {
		return errors.Wrap(err, "insert log records")
	}
	if err := c.inserter.InsertLogLabels(ctx, labels); err != nil {
		return errors.Wrap(err, "insert labels")
	}
	return nil
}
