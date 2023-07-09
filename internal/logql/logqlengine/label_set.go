package logqlengine

import (
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/go-faster/oteldb/internal/logql"
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

// AsLokiAPI returns lokiapi.LabelSet
func (l *LabelSet) AsLokiAPI() lokiapi.LabelSet {
	set := make(lokiapi.LabelSet, len(l.labels))
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

// SetAttrs sets labels from attrs.
func (l *LabelSet) SetAttrs(attrMaps ...otelstorage.Attrs) (rerr error) {
	if l.labels == nil {
		l.labels = map[logql.Label]pcommon.Value{}
	}
	maps.Clear(l.labels)
	for _, attrs := range attrMaps {
		m := attrs.AsMap()
		if m == (pcommon.Map{}) {
			continue
		}
		m.Range(func(k string, v pcommon.Value) bool {
			if err := logql.IsValidLabel(k, true); err != nil {
				rerr = err
				return false
			}
			l.labels[logql.Label(k)] = v
			return true
		})
	}
	return rerr
}

// Add adds new label.
func (l *LabelSet) Add(s logql.Label, val pcommon.Value) {
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

// SetError sets special error label.
func (l *LabelSet) SetError(err error) {
	if _, ok := l.labels[logql.ErrorLabel]; ok {
		// Do not override old error.
		return
	}
	if err != nil {
		l.labels[logql.ErrorLabel] = pcommon.NewValueStr(err.Error())
	}
}

// GetError returns error label.
func (l *LabelSet) GetError() (string, bool) {
	return l.GetString(logql.ErrorLabel)
}
