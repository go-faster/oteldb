package traceql

import (
	"fmt"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

// ParseQuery parses TraceQL query.
func ParseQuery(q string) (p Pipeline, _ error) {
	r, err := Parse(q)
	if err != nil {
		return p, err
	}
	if err := r.validate(); err != nil {
		return p, err
	}
	return r.Pipeline, nil
}

// EvaluatePipeline evaluates pipeline.
func EvaluatePipeline(in *Spanset, pipeline Pipeline) ([]*Spanset, error) {
	return pipeline.evaluate([]*Spanset{in})
}

// CreateFetchSpansRequest will flatten the SpansetFilter in simple conditions the storage layer
// can work with.
func CreateFetchSpansRequest(params tempoapi.SearchParams, pipeline Pipeline) FetchSpansRequest {
	// TODO handle SearchRequest.MinDurationMs and MaxDurationMs, this refers to the trace level duration which is not the same as the intrinsic duration
	req := FetchSpansRequest{
		StartTimeUnixNanos: 0,
		EndTimeUnixNanos:   0,
		Conditions:         nil,
		AllConditions:      true,
	}
	if n, ok := params.Start.Get(); ok {
		req.StartTimeUnixNanos = uint64(n.UnixNano())
	}
	if n, ok := params.End.Get(); ok {
		req.EndTimeUnixNanos = uint64(n.UnixNano())
	}

	pipeline.extractConditions(&req)
	return req
}

func (s Static) asAnyValue() (val tempoapi.AnyValue) {
	switch s.Type {
	case TypeInt:
		val.SetIntValue(tempoapi.IntValue{
			IntValue: int64(s.N),
		})
	case TypeString:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: s.S,
		})
	case TypeFloat:
		val.SetDoubleValue(tempoapi.DoubleValue{
			DoubleValue: s.F,
		})
	case TypeBoolean:
		val.SetBoolValue(tempoapi.BoolValue{
			BoolValue: s.B,
		})
	case TypeDuration:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: s.D.String(),
		})
	case TypeStatus:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: s.Status.String(),
		})
	case TypeNil:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: "nil",
		})
	case TypeKind:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: s.Kind.String(),
		})
	default:
		val.SetStringValue(tempoapi.StringValue{
			StringValue: fmt.Sprintf("error formatting val: static has unexpected type %v", s.Type),
		})
	}
	return val
}
