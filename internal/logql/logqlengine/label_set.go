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
	labels map[string]pcommon.Value
}

func newLabelSet() LabelSet {
	return LabelSet{
		labels: map[string]pcommon.Value{},
	}
}

// AsLokiAPI returns lokiapi.LabelSet
func (l *LabelSet) AsLokiAPI() lokiapi.LabelSet {
	set := make(lokiapi.LabelSet, len(l.labels))
	for k, v := range l.labels {
		set[k] = v.AsString()
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
		sb.WriteString(k)
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
		l.labels = map[string]pcommon.Value{}
	}
	maps.Clear(l.labels)
	for _, attrs := range attrMaps {
		attrs.AsMap().Range(func(k string, v pcommon.Value) bool {
			if err := logql.IsValidLabel(k, true); err != nil {
				rerr = err
				return false
			}
			l.labels[k] = v
			return true
		})
	}
	return rerr
}

// Add adds new label.
func (l *LabelSet) Add(s string, val pcommon.Value) {
	l.labels[s] = val
}

// Get returns attr value.
func (l *LabelSet) Get(name string) (v pcommon.Value, ok bool) {
	v, ok = l.labels[name]
	return v, ok
}

// GetString returns stringified attr value.
func (l *LabelSet) GetString(name string) (string, bool) {
	v, ok := l.Get(name)
	if ok {
		return v.AsString(), true
	}
	return "", false
}

const errorLabel = "__error__"

// SetError sets special error label.
func (l *LabelSet) SetError(err error) {
	if _, ok := l.labels[errorLabel]; ok {
		// Do not override old error.
		return
	}
	if err != nil {
		l.labels[errorLabel] = pcommon.NewValueStr(err.Error())
	}
}

// GetError returns error label.
func (l *LabelSet) GetError() (string, bool) {
	return l.GetString(errorLabel)
}
