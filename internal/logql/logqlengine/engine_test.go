// Package logqlengine implements LogQL evaluation engine.
package logqlengine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type inputLine struct {
	line  string
	attrs pcommon.Map
}

type resultLine struct {
	line   string
	labels map[string]string
}

type mockQuerier struct {
	lines []inputLine
	step  time.Duration
}

func (m *mockQuerier) Сapabilities() (caps QuerierСapabilities) {
	return caps
}

func (m *mockQuerier) SelectLogs(_ context.Context, start, _ otelstorage.Timestamp, _ SelectLogsParams) (iterators.Iterator[logstorage.Record], error) {
	step := m.step
	if step == 0 {
		step = time.Millisecond
	}
	ts := start.AsTime()

	var (
		records    []logstorage.Record
		scopeAttrs = pcommon.NewMap()
		resAttrs   = pcommon.NewMap()
	)
	scopeAttrs.PutStr("scope", "test")
	resAttrs.PutStr("resource", "test")

	for _, l := range m.lines {
		ts = ts.Add(step)
		records = append(records, logstorage.Record{
			Timestamp:     otelstorage.NewTimestampFromTime(ts),
			Body:          l.line,
			Attrs:         otelstorage.Attrs(l.attrs),
			ScopeAttrs:    otelstorage.Attrs(scopeAttrs),
			ResourceAttrs: otelstorage.Attrs(resAttrs),
		})
	}

	return iterators.Slice(records), nil
}

func TestEngineEvalStream(t *testing.T) {
	justLines := func(lines ...string) []inputLine {
		r := make([]inputLine, len(lines))
		for i, line := range lines {
			r[i] = inputLine{
				line: line,
			}
		}
		return r
	}

	startTime := otelstorage.Timestamp(1688833731000000000)
	tests := []struct {
		query    string
		input    []inputLine
		wantData []resultLine
		wantErr  bool
	}{
		{
			`{resource="test"} | json | foo <= 5m, bar == 2s or baz == 1MB`,
			justLines(
				`{"id": 1, "foo": "4m", "bar": "1s", "baz": "1kb"}`,
				`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
				`{"id": 3, "foo": "6m", "bar": "3s", "baz": "1gb"}`,
			),
			[]resultLine{
				{
					`{"id": 2, "foo": "5m", "bar": "2s", "baz": "1mb"}`,
					map[string]string{
						"id":  "2",
						"foo": "5m",
						"bar": "2s",
						"baz": "1mb",
					},
				},
			},
			false,
		},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			ctx := context.Background()

			opts := Options{
				ParseOptions: logql.ParseOptions{AllowDots: true},
			}
			e := NewEngine(&mockQuerier{lines: tt.input}, opts)

			gotData, err := e.Eval(ctx, tt.query, EvalParams{
				Start: startTime,
				End:   startTime,
				Limit: 1000,
			})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			result, ok := gotData.GetStreamsResult()
			require.True(t, ok)

			entries := make([]resultLine, 0, len(tt.wantData))
			for _, s := range result.Result {
				set := s.Stream.Value

				// Ensure engine passes resource and scope attrs.
				for k, v := range map[string]string{
					"scope":    "test",
					"resource": "test",
				} {
					assert.Contains(t, set, k)
					assert.Equal(t, v, set[k])
					// Do add labels to the test.
					delete(set, k)
				}

				for _, e := range s.Values {
					entries = append(entries, resultLine{
						line:   e.V,
						labels: set,
					})
				}
			}

			assert.Len(t, entries, len(tt.wantData))
			for i, e := range entries {
				assert.Equal(t, e, tt.wantData[i])
			}
		})
	}
}
