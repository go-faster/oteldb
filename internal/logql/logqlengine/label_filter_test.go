package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
)

func TestIPLabelFilter(t *testing.T) {
	tests := []struct {
		input   map[string]pcommon.Value
		label   string
		pattern string
		wantOk  bool
	}{
		// No label.
		{
			map[string]pcommon.Value{},
			`not_exist`,
			"192.168.1.1",
			false,
		},
		{
			map[string]pcommon.Value{},
			`not_exist`,
			"192.168.1.0-192.168.1.255",
			false,
		},
		{
			map[string]pcommon.Value{},
			`not_exist`,
			"192.168.1.0/24",
			false,
		},

		// Has match.
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			`exist`,
			"192.168.1.1",
			true,
		},
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			`exist`,
			"192.168.1.0-192.168.1.255",
			true,
		},
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`192.168.1.1`),
			},
			`exist`,
			"192.168.1.0/24",
			true,
		},

		// No match.
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			`exist`,
			"192.168.1.1",
			false,
		},
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			`exist`,
			"192.168.1.0-192.168.1.255",
			false,
		},
		{
			map[string]pcommon.Value{
				"exist": pcommon.NewValueStr(`127.0.0.1`),
			},
			`exist`,
			"192.168.1.0/24",
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := newLabelSet()
			_, hasLabel := tt.input[tt.label]
			set.labels = tt.input

			for _, cse := range []struct {
				op     logql.BinOp
				wantOk bool
			}{
				{logql.OpEq, hasLabel && tt.wantOk},
				{logql.OpNotEq, hasLabel && !tt.wantOk},
			} {
				f, err := buildIPLabelFilter(&logql.IPFilter{
					Label: logql.Label(tt.label),
					Op:    cse.op,
					Value: tt.pattern,
				})
				require.NoError(t, err)

				newLine, ok := f.Process(0, ``, set)
				// Ensure that extractor does not change the line.
				require.Equal(t, ``, newLine)
				require.Equal(t, cse.wantOk, ok)
			}
		})
	}
}
