package logqlengine

import (
	"slices"
	"strconv"
	"strings"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LabelSet is a log record's label set.
type LabelSet struct {
	labels map[logql.Label]pcommon.Value
}

func newLabelSet() LabelSet {
	return LabelSet{
		labels: map[logql.Label]pcommon.Value{},
	}
}

func (l *LabelSet) allowDots() bool {
	return true
}

func (l *LabelSet) reset() {
	if l.labels == nil {
		l.labels = map[logql.Label]pcommon.Value{}
	}
	maps.Clear(l.labels)
}

// AsLokiAPI returns lokiapi.LabelSet
func (l *LabelSet) AsLokiAPI() lokiapi.LabelSet {
	return lokiapi.LabelSet(l.AsMap())
}

// AsMap returns labels as strings map.
func (l *LabelSet) AsMap() map[string]string {
	set := make(map[string]string, len(l.labels))
	for k, v := range l.labels {
		set[string(k)] = v.AsString()
	}
	return set
}

// String returns text representation of labels.
func (l *LabelSet) String() string {
	var sb strings.Builder
	sb.WriteByte('{')

	keys := maps.Keys(l.labels)
	slices.Sort(keys)

	i := 0
	for _, k := range keys {
		v := l.labels[k]
		if i != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(string(k))
		sb.WriteByte('=')
		sb.WriteString(strconv.Quote(v.AsString()))
		i++
	}
	sb.WriteByte('}')
	return sb.String()
}

// SetFromRecord sets labels from given log record.
func (l *LabelSet) SetFromRecord(record logstorage.Record) {
	l.reset()

	if traceID := record.TraceID; !traceID.IsEmpty() {
		l.Set(logstorage.LabelTraceID, pcommon.NewValueStr(traceID.Hex()))
	}
	if spanID := record.SpanID; !spanID.IsEmpty() {
		l.Set(logstorage.LabelSpanID, pcommon.NewValueStr(spanID.Hex()))
	}
	if severity := record.SeverityNumber; severity != plog.SeverityNumberUnspecified {
		l.Set(logstorage.LabelSeverity, pcommon.NewValueStr(severity.String()))
	}
	if body := record.Body; body != "" {
		l.Set(logstorage.LabelBody, pcommon.NewValueStr(body))
	}
	l.SetAttrs(record.Attrs, record.ScopeAttrs, record.ResourceAttrs)
}

// SetAttrs sets labels from attrs.
func (l *LabelSet) SetAttrs(attrMaps ...otelstorage.Attrs) {
	for _, attrs := range attrMaps {
		m := attrs.AsMap()
		if m == (pcommon.Map{}) {
			continue
		}
		m.Range(func(k string, v pcommon.Value) bool {
			k = otelstorage.KeyToLabel(k)
			l.Set(logql.Label(k), v)
			return true
		})
	}
}

// Set sets label.
func (l *LabelSet) Set(s logql.Label, val pcommon.Value) {
	l.labels[s] = val
}

// Delete deletes label.
func (l *LabelSet) Delete(s logql.Label) {
	delete(l.labels, s)
}

// Range iterates over label set.
func (l *LabelSet) Range(cb func(logql.Label, pcommon.Value)) {
	for k, v := range l.labels {
		cb(k, v)
	}
}

// Get returns attr value.
func (l *LabelSet) Get(name logql.Label) (v pcommon.Value, ok bool) {
	v, ok = l.labels[name]
	return v, ok
}

// GetString returns stringified attr value.
func (l *LabelSet) GetString(name logql.Label) (string, bool) {
	v, ok := l.Get(name)
	if ok {
		return v.AsString(), true
	}
	return "", false
}

// GetFloat returns number attr value.
func (l *LabelSet) GetFloat(name logql.Label) (_ float64, ok bool, err error) {
	v, ok := l.Get(name)
	if !ok {
		return 0, false, nil
	}
	switch t := v.Type(); t {
	case pcommon.ValueTypeStr:
		v, err := strconv.ParseFloat(v.Str(), 64)
		return v, true, err
	case pcommon.ValueTypeInt:
		// TODO(tdakkota): check for overflow.
		return float64(v.Int()), true, nil
	case pcommon.ValueTypeDouble:
		return v.Double(), true, nil
	default:
		return 0, false, errors.Errorf("can't convert %q to float", t)
	}
}

// SetError sets special error label.
func (l *LabelSet) SetError(typ string, err error) {
	if _, ok := l.labels[logql.ErrorLabel]; ok {
		// Do not override old error.
		return
	}
	if err != nil {
		l.labels[logql.ErrorLabel] = pcommon.NewValueStr(typ)
		l.labels[logql.ErrorDetailsLabel] = pcommon.NewValueStr(err.Error())
	}
}

// GetError returns error label.
func (l *LabelSet) GetError() (string, bool) {
	return l.GetString(logql.ErrorLabel)
}
