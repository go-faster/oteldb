package traceql

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestStaticSet(t *testing.T) {
	var s Static

	s.SetString("foo")
	require.Equal(t, TypeString, s.ValueType())
	require.Equal(t, "foo", s.AsString())

	s.SetInt(-10)
	require.Equal(t, TypeInt, s.ValueType())
	require.Equal(t, int64(-10), s.AsInt())

	s.SetNumber(3.14)
	require.Equal(t, TypeNumber, s.ValueType())
	require.Equal(t, 3.14, s.AsNumber())
	s.SetNumber(math.NaN())
	require.Equal(t, TypeNumber, s.ValueType())
	require.True(t, math.IsNaN(s.AsNumber()))

	s.SetBool(true)
	require.Equal(t, TypeBool, s.ValueType())
	require.Equal(t, true, s.AsBool())

	s.SetNil()
	require.Equal(t, TypeNil, s.ValueType())
	require.True(t, s.IsNil())

	s.SetDuration(time.Second)
	require.Equal(t, TypeDuration, s.ValueType())
	require.Equal(t, time.Second, s.AsDuration())

	s.SetSpanStatus(ptrace.StatusCodeOk)
	require.Equal(t, TypeSpanStatus, s.ValueType())
	require.Equal(t, ptrace.StatusCodeOk, s.AsSpanStatus())

	s.SetSpanKind(ptrace.SpanKindClient)
	require.Equal(t, TypeSpanKind, s.ValueType())
	require.Equal(t, ptrace.SpanKindClient, s.AsSpanKind())
}
