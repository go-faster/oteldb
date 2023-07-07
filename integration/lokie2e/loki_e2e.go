// Package lokie2e provides scripts for E2E testing Loki API implementation.
package lokie2e

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/logstorage"
)

// BatchSet is a set of batches.
type BatchSet struct {
	Batches []plog.Logs
	Labels  map[string][]logstorage.Label
	Records map[pcommon.Timestamp]plog.LogRecord
}

// ParseBatchSet parses JSON batches from given reader.
func ParseBatchSet(r io.Reader) (s BatchSet, _ error) {
	d := jx.Decode(r, 4096)
	u := plog.JSONUnmarshaler{}

	for d.Next() != jx.Invalid {
		data, err := d.Raw()
		if err != nil {
			return s, errors.Wrap(err, "read line")
		}

		raw, err := u.UnmarshalLogs(data)
		if err != nil {
			return s, errors.Wrap(err, "parse batch")
		}

		if err := s.addBatch(raw); err != nil {
			return s, errors.Wrap(err, "add batch")
		}
	}
	return s, nil
}

func (s *BatchSet) addBatch(raw plog.Logs) error {
	s.Batches = append(s.Batches, raw)

	resLogs := raw.ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)
		res := resLog.Resource()
		s.addLabels(res.Attributes())

		scopeLogs := resLog.ScopeLogs()
		for i := 0; i < scopeLogs.Len(); i++ {
			scopeLog := scopeLogs.At(i)
			scope := scopeLog.Scope()
			s.addLabels(scope.Attributes())

			records := scopeLog.LogRecords()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				if err := s.addRecord(record); err != nil {
					return errors.Wrap(err, "add record")
				}
				s.addLabels(record.Attributes())
			}
		}
	}
	return nil
}

func (s *BatchSet) addRecord(record plog.LogRecord) error {
	ts := record.Timestamp()

	if _, ok := s.Records[ts]; ok {
		return errors.Errorf("duplicate record with timestamp %v", ts)
	}

	if s.Records == nil {
		s.Records = map[pcommon.Timestamp]plog.LogRecord{}
	}
	s.Records[ts] = record
	return nil
}

func (s *BatchSet) addLabels(m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		switch t := v.Type(); t {
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		default:
			s.addLabel(logstorage.Label{
				Name:  k,
				Value: v.AsString(),
				Type:  int32(t),
			})
		}
		return true
	})
}

func (s *BatchSet) addLabel(label logstorage.Label) {
	if s.Labels == nil {
		s.Labels = map[string][]logstorage.Label{}
	}
	s.Labels[label.Name] = append(s.Labels[label.Name], label)
}
