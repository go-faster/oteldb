package logstorage

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
				labels[Label{k, v.AsString(), int32(t)}] = struct{}{}
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

	if err := c.inserter.InsertRecords(ctx, insertBatch); err != nil {
		return errors.Wrap(err, "insert log records")
	}
	if err := c.inserter.InsertLogLabels(ctx, labels); err != nil {
		return errors.Wrap(err, "insert labels")
	}
	return nil
}
