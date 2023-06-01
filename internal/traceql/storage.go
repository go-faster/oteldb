package traceql

import (
	"fmt"
	"time"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

type Operands []Static

type Condition struct {
	Attribute Attribute
	Op        Operator
	Operands  Operands
}

// SecondPassFn is a method that is called in between the first and second
// pass of a fetch spans request. See below.
type SecondPassFn func(*Spanset) ([]*Spanset, error)

type FetchSpansRequest struct {
	StartTimeUnixNanos uint64
	EndTimeUnixNanos   uint64
	Conditions         []Condition

	// Hints

	// By default the storage layer fetches spans meeting any of the criteria.
	// This hint is for common cases like { x && y && z } where the storage layer
	// can make extra optimizations by returning only spansets that meet
	// all criteria.
	AllConditions bool

	// SecondPassFn and Conditions allow a caller to retrieve one set of data
	// in the first pass, filter using the SecondPassFn callback and then
	// request a different set of data in the second pass. This is particularly
	// useful for retrieving data required to resolve a TraceQL query in the first
	// pass and only selecting metadata in the second pass.
	// TODO: extend this to an arbitrary number of passes
	SecondPass           SecondPassFn
	SecondPassConditions []Condition
}

func (f *FetchSpansRequest) appendCondition(c ...Condition) {
	f.Conditions = append(f.Conditions, c...)
}

type Span interface {
	// these are the actual fields used by the engine to evaluate queries
	// if a Filter parameter is passed the spans returned will only have this field populated
	Attributes() map[Attribute]Static

	ID() [8]byte
	StartTimeUnixNanos() uint64
	DurationNanos() uint64
}

// should we just make matched a field on the spanset instead of a special attribute?
const attributeMatched = "__matched"

type SpansetAttribute struct {
	Name string
	Val  Static
}

type Spanset struct {
	// these fields are actually used by the engine to evaluate queries
	Scalar Static
	Spans  []Span

	TraceID            [16]byte
	RootSpanName       string
	RootServiceName    string
	StartTimeUnixNanos uint64
	DurationNanos      uint64
	Attributes         []*SpansetAttribute
}

func (spanset *Spanset) AsTraceSearchMetadata() tempoapi.TraceSearchMetadata {
	metadata := tempoapi.TraceSearchMetadata{
		TraceID:           fmt.Sprintf("%x", spanset.TraceID),
		RootServiceName:   spanset.RootServiceName,
		RootTraceName:     spanset.RootSpanName,
		StartTimeUnixNano: time.Unix(0, int64(spanset.StartTimeUnixNanos)),
		DurationMs:        int(spanset.DurationNanos / 1_000_000),
	}

	for _, span := range spanset.Spans {
		tempopbSpan := tempoapi.TempoSpan{
			SpanID:            fmt.Sprintf("%x", span.ID()),
			StartTimeUnixNano: time.Unix(0, int64(span.StartTimeUnixNanos())),
			DurationNanos:     int64(span.DurationNanos()),
			Attributes:        nil,
		}

		atts := span.Attributes()

		if name, ok := atts[NewIntrinsic(IntrinsicName)]; ok {
			tempopbSpan.Name = name.S
		}

		for attribute, static := range atts {
			if attribute.Intrinsic == IntrinsicName ||
				attribute.Intrinsic == IntrinsicDuration ||
				attribute.Intrinsic == IntrinsicTraceDuration ||
				attribute.Intrinsic == IntrinsicTraceRootService ||
				attribute.Intrinsic == IntrinsicTraceRootSpan {
				continue
			}

			staticAnyValue := static.asAnyValue()

			keyValue := tempoapi.KeyValue{
				Key:   attribute.Name,
				Value: staticAnyValue,
			}

			if tempopbSpan.Attributes == nil {
				tempopbSpan.Attributes = new(tempoapi.Attributes)
			}
			*tempopbSpan.Attributes = append(*tempopbSpan.Attributes, keyValue)
		}

		metadata.SpanSet.Spans = append(metadata.SpanSet.Spans, tempopbSpan)
	}

	// add attributes
	for _, att := range spanset.Attributes {
		staticAnyValue := att.Val.asAnyValue()
		keyValue := tempoapi.KeyValue{
			Key:   att.Name,
			Value: staticAnyValue,
		}
		if metadata.SpanSet.Attributes == nil {
			metadata.SpanSet.Attributes = new(tempoapi.Attributes)
		}
		*metadata.SpanSet.Attributes = append(*metadata.SpanSet.Attributes, keyValue)
	}

	return metadata
}

func (s *Spanset) AddAttribute(key string, value Static) {
	s.Attributes = append(s.Attributes, &SpansetAttribute{Name: key, Val: value})
}

func (s *Spanset) clone() *Spanset {
	return &Spanset{
		TraceID:            s.TraceID,
		Scalar:             s.Scalar,
		RootSpanName:       s.RootSpanName,
		RootServiceName:    s.RootServiceName,
		StartTimeUnixNanos: s.StartTimeUnixNanos,
		DurationNanos:      s.DurationNanos,
		Spans:              s.Spans, // we're not deep cloning into the spans or attributes
		Attributes:         s.Attributes,
	}
}
