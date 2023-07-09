package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
)

func TestJSONExtractor(t *testing.T) {
	tests := []struct {
		input         string
		extract       []logql.Label
		expectLabels  map[logql.Label]pcommon.Value
		wantFilterErr bool
	}{
		{`{}`, nil, nil, false},
		{`{}`, []logql.Label{"foo"}, nil, false},
		{`{"foo": null}`, nil, nil, false},
		{`{"foo": null}`, []logql.Label{"foo"}, nil, false},
		{`{"bar": 10}`, []logql.Label{"foo"}, nil, false},
		{
			`{"foo": "extract", "bar": "not extract"}`,
			[]logql.Label{"foo"},
			map[logql.Label]pcommon.Value{
				"foo": pcommon.NewValueStr("extract"),
			},
			false,
		},
		{
			`{
				"str":"str",
				"int":10,
				"double": 3.14,
				"skip": null,
				"bool": true,
				"array": [1],
				"object": {"sub_key": 1}
			}`,
			nil,
			map[logql.Label]pcommon.Value{
				"str":    pcommon.NewValueStr("str"),
				"int":    pcommon.NewValueInt(10),
				"double": pcommon.NewValueDouble(3.14),
				"bool":   pcommon.NewValueBool(true),
				"array": func() pcommon.Value {
					r := pcommon.NewValueSlice()
					s := r.Slice()
					v := s.AppendEmpty()
					v.SetInt(1)
					return r
				}(),
				"object": func() pcommon.Value {
					r := pcommon.NewValueMap()
					m := r.Map()
					m.PutInt("sub_key", 1)
					return r
				}(),
			},
			false,
		},

		{`{"label": "foo}`, nil, nil, true},
		{`{"label": "foo}`, []logql.Label{"label"}, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			e, err := buildJSONExtractor(&logql.JSONExpressionParser{
				Labels: tt.extract,
			})
			require.NoError(t, err)

			set := newLabelSet()
			newLine, ok := e.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
			require.True(t, ok)

			if tt.wantFilterErr {
				_, ok = set.GetError()
				require.True(t, ok)
				return
			}
			errMsg, ok := set.GetError()
			require.False(t, ok, "got error: %s", errMsg)

			for k, expect := range tt.expectLabels {
				got, ok := set.Get(k)
				require.Truef(t, ok, "key %q", k)
				require.Equal(t, expect, got)
			}
		})
	}
}
