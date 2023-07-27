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
	require.Equal(t, StaticString, s.Type)
	require.Equal(t, "foo", s.AsString())

	s.SetInteger(-10)
	require.Equal(t, StaticInteger, s.Type)
	require.Equal(t, int64(-10), s.AsInteger())

	s.SetNumber(3.14)
	require.Equal(t, StaticNumber, s.Type)
	require.Equal(t, 3.14, s.AsNumber())
	s.SetNumber(math.NaN())
	require.Equal(t, StaticNumber, s.Type)
	require.True(t, math.IsNaN(s.AsNumber()))

	s.SetBool(true)
	require.Equal(t, StaticBool, s.Type)
	require.Equal(t, true, s.AsBool())

	s.SetNil()
	require.Equal(t, StaticNil, s.Type)
	require.True(t, s.IsNil())

	s.SetDuration(time.Second)
	require.Equal(t, StaticDuration, s.Type)
	require.Equal(t, time.Second, s.AsDuration())

	s.SetSpanStatus(ptrace.StatusCodeOk)
	require.Equal(t, StaticSpanStatus, s.Type)
	require.Equal(t, ptrace.StatusCodeOk, s.AsSpanStatus())

	s.SetSpanKind(ptrace.SpanKindClient)
	require.Equal(t, StaticSpanKind, s.Type)
	require.Equal(t, ptrace.SpanKindClient, s.AsSpanKind())
}
