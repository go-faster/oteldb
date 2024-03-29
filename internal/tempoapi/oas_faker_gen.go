// Code generated by ogen, DO NOT EDIT.

package tempoapi

import (
	"time"
)

// SetFake set fake values.
func (s *AnyValue) SetFake() {
	var variant StringValue

	{
		variant.SetFake()
	}
	s.SetStringValue(variant)
}

// SetFake set fake values.
func (s *ArrayValue) SetFake() {
	{
		{
			s.ArrayValue = nil
			for i := 0; i < 0; i++ {
				var elem AnyValue
				{
					elem.SetFake()
				}
				s.ArrayValue = append(s.ArrayValue, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *Attributes) SetFake() {
	var unwrapped []KeyValue
	{
		unwrapped = nil
		for i := 0; i < 0; i++ {
			var elem KeyValue
			{
				elem.SetFake()
			}
			unwrapped = append(unwrapped, elem)
		}
	}
	*s = Attributes(unwrapped)
}

// SetFake set fake values.
func (s *BoolValue) SetFake() {
	{
		{
			s.BoolValue = true
		}
	}
}

// SetFake set fake values.
func (s *BytesValue) SetFake() {
	{
		{
			s.BytesValue = []byte("[]byte")
		}
	}
}

// SetFake set fake values.
func (s *DoubleValue) SetFake() {
	{
		{
			s.DoubleValue = float64(0)
		}
	}
}

// SetFake set fake values.
func (s *Error) SetFake() {
	var unwrapped string
	{
		unwrapped = "string"
	}
	*s = Error(unwrapped)
}

// SetFake set fake values.
func (s *IntValue) SetFake() {
	{
		{
			s.IntValue = int64(0)
		}
	}
}

// SetFake set fake values.
func (s *KeyValue) SetFake() {
	{
		{
			s.Key = "string"
		}
	}
	{
		{
			s.Value.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *KvlistValue) SetFake() {
	{
		{
			s.KvlistValue = nil
			for i := 0; i < 0; i++ {
				var elem KeyValue
				{
					elem.SetFake()
				}
				s.KvlistValue = append(s.KvlistValue, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *OptInt) SetFake() {
	var elem int
	{
		elem = int(0)
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptString) SetFake() {
	var elem string
	{
		elem = "string"
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *OptTempoSpanSet) SetFake() {
	var elem TempoSpanSet
	{
		elem.SetFake()
	}
	s.SetTo(elem)
}

// SetFake set fake values.
func (s *StringValue) SetFake() {
	{
		{
			s.StringValue = "string"
		}
	}
}

// SetFake set fake values.
func (s *TagNames) SetFake() {
	{
		{
			s.TagNames = nil
			for i := 0; i < 0; i++ {
				var elem string
				{
					elem = "string"
				}
				s.TagNames = append(s.TagNames, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *TagValue) SetFake() {
	{
		{
			s.Type = "string"
		}
	}
	{
		{
			s.Value = "string"
		}
	}
}

// SetFake set fake values.
func (s *TagValues) SetFake() {
	{
		{
			s.TagValues = nil
			for i := 0; i < 0; i++ {
				var elem string
				{
					elem = "string"
				}
				s.TagValues = append(s.TagValues, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *TagValuesV2) SetFake() {
	{
		{
			s.TagValues = nil
			for i := 0; i < 0; i++ {
				var elem TagValue
				{
					elem.SetFake()
				}
				s.TagValues = append(s.TagValues, elem)
			}
		}
	}
}

// SetFake set fake values.
func (s *TempoSpan) SetFake() {
	{
		{
			s.SpanID = "string"
		}
	}
	{
		{
			s.Name.SetFake()
		}
	}
	{
		{
			s.StartTimeUnixNano = time.Now()
		}
	}
	{
		{
			s.DurationNanos = int64(0)
		}
	}
	{
		{
			s.Attributes.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *TempoSpanSet) SetFake() {
	{
		{
			s.Spans = nil
			for i := 0; i < 0; i++ {
				var elem TempoSpan
				{
					elem.SetFake()
				}
				s.Spans = append(s.Spans, elem)
			}
		}
	}
	{
		{
			s.Matched.SetFake()
		}
	}
	{
		{
			s.Attributes.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *TraceSearchMetadata) SetFake() {
	{
		{
			s.TraceID = "string"
		}
	}
	{
		{
			s.RootServiceName.SetFake()
		}
	}
	{
		{
			s.RootTraceName.SetFake()
		}
	}
	{
		{
			s.StartTimeUnixNano = time.Now()
		}
	}
	{
		{
			s.DurationMs.SetFake()
		}
	}
	{
		{
			s.SpanSet.SetFake()
		}
	}
}

// SetFake set fake values.
func (s *Traces) SetFake() {
	{
		{
			s.Traces = nil
			for i := 0; i < 0; i++ {
				var elem TraceSearchMetadata
				{
					elem.SetFake()
				}
				s.Traces = append(s.Traces, elem)
			}
		}
	}
}
