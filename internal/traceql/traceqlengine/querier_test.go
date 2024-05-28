package traceqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/internal/traceql"
)

func TestSpanMatcher_String(t *testing.T) {
	tests := []struct {
		m    SpanMatcher
		want string
	}{
		{
			SpanMatcher{
				Attribute: traceql.Attribute{
					Prop: traceql.SpanName,
				},
				Op: traceql.OpEq,
				Static: traceql.Static{
					Type: traceql.TypeString,
					Str:  "span",
				},
			},
			`name = "span"`,
		},
		{
			SpanMatcher{
				Attribute: traceql.Attribute{
					Name: "http.status_code",
				},
				Op: traceql.OpGte,
				Static: traceql.Static{
					Type: traceql.TypeInt,
					Data: 400,
				},
			},
			`.http.status_code >= 400`,
		},
		{
			SpanMatcher{
				Attribute: traceql.Attribute{
					Prop: traceql.TraceDuration,
				},
				Op: traceql.OpLte,
				Static: traceql.Static{
					Type: traceql.TypeDuration,
					Data: uint64(time.Second),
				},
			},
			`traceDuration <= 1s`,
		},
		{
			SpanMatcher{
				Attribute: traceql.Attribute{
					Prop: traceql.SpanStatus,
				},
				Op: traceql.OpEq,
				Static: traceql.Static{
					Type: traceql.TypeSpanStatus,
					Data: uint64(ptrace.StatusCodeOk),
				},
			},
			`status = ok`,
		},
		{
			SpanMatcher{
				Attribute: traceql.Attribute{
					Prop: traceql.SpanKind,
				},
				Op: traceql.OpEq,
				Static: traceql.Static{
					Type: traceql.TypeSpanKind,
					Data: uint64(ptrace.SpanKindClient),
				},
			},
			`kind = client`,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			s := tt.m.String()
			require.Equal(t, tt.want, s)

			// Ensure stringer produces valid TraceQL.
			_, err := traceql.Parse("{" + s + "}")
			require.NoError(t, err)
		})
	}
}
