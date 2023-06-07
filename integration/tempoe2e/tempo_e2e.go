// Package tempoe2e provides scripts for E2E testing Tempo API implementation.
package tempoe2e

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

// BatchSet is a set of batches.
type BatchSet struct {
	Batches []ptrace.Traces
	Tags    map[string][]tracestorage.Tag
}

// ParseBatchSet parses JSON batches from given reader.
func ParseBatchSet(r io.Reader) (s BatchSet, _ error) {
	d := jx.Decode(r, 4096)
	u := ptrace.JSONUnmarshaler{}

	for d.Next() != jx.Invalid {
		data, err := d.Raw()
		if err != nil {
			return s, errors.Wrap(err, "read line")
		}

		raw, err := u.UnmarshalTraces(data)
		if err != nil {
			return s, errors.Wrap(err, "parse batch")
		}

		s.addBatch(raw)
	}
	return s, nil
}

func (s *BatchSet) addBatch(raw ptrace.Traces) {
	s.Batches = append(s.Batches, raw)

	resSpans := raw.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)
		res := resSpan.Resource()
		s.addTags(res.Attributes())

		scopeSpans := resSpan.ScopeSpans()
		for i := 0; i < scopeSpans.Len(); i++ {
			scopeSpan := scopeSpans.At(i)
			scope := scopeSpan.Scope()
			s.addTags(scope.Attributes())

			spans := scopeSpan.Spans()
			for i := 0; i < spans.Len(); i++ {
				span := spans.At(i)
				// Add span name as well. For some reason, Grafana is looking for it too.
				s.addName(span.Name())
				s.addTags(span.Attributes())
			}
		}
	}
}

func (s *BatchSet) addName(name string) {
	s.addTag(tracestorage.Tag{
		Name:  "name",
		Value: name,
		Type:  int32(pcommon.ValueTypeStr),
	})
}

func (s *BatchSet) addTags(m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		switch t := v.Type(); t {
		case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		default:
			s.addTag(tracestorage.Tag{
				Name:  k,
				Value: v.AsString(),
				Type:  int32(t),
			})
		}
		return true
	})
}

func (s *BatchSet) addTag(tag tracestorage.Tag) {
	if s.Tags == nil {
		s.Tags = map[string][]tracestorage.Tag{}
	}
	s.Tags[tag.Name] = append(s.Tags[tag.Name], tag)
}
