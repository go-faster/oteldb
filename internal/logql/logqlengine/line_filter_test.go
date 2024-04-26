package logqlengine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
)

var ipLineFilterTests = []struct {
	input   string
	pattern string
	wantOk  bool
}{
	// Empty line.
	{
		``,
		"192.168.1.1",
		false,
	},
	// No IP in a line.
	{
		`foo`,
		"192.168.1.1",
		false,
	},
	{
		`foo`,
		"192.168.1.0-192.168.1.255",
		false,
	},
	{
		`foo`,
		"192.168.1.0/24",
		false,
	},

	// Has match.
	{
		`foo 192.168.1.1`,
		"192.168.1.1",
		true,
	},
	{
		`foo 192.168.1.1`,
		"192.168.1.0-192.168.1.255",
		true,
	},
	{
		`foo 192.168.1.1`,
		"192.168.1.0/24",
		true,
	},

	// No match.
	{
		`foo 127.0.0.1`,
		"192.168.1.1",
		false,
	},
	{
		`foo 127.0.0.1`,
		"192.168.1.0-192.168.1.255",
		false,
	},
	{
		`foo 127.0.0.1`,
		"192.168.1.0/24",
		false,
	},

	// Second IP match.
	{
		`foo=bar host=127.0.0.1 from=192.168.1.1`,
		"192.168.1.1",
		true,
	},

	// IPv6 match.
	{
		`::1`,
		"::1",
		true,
	},
	{
		`foo ::1 foo`,
		"::1",
		true,
	},
	{
		`2001:db8::68`,
		"2001:db8::68",
		true,
	},
	{
		`foo 2001:db8::68 foo`,
		"2001:db8::68",
		true,
	},
}

func TestIPLineFilter(t *testing.T) {
	for i, tt := range ipLineFilterTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			set := logqlabels.NewLabelSet()

			f, err := buildLineFilter(&logql.LineFilter{
				Op: logql.OpEq,
				By: logql.LineFilterValue{
					Value: tt.pattern,
					IP:    true,
				},
			})
			require.NoError(t, err)

			newLine, gotOk := f.Process(0, tt.input, set)
			// Ensure that extractor does not change the line.
			require.Equal(t, tt.input, newLine)
			require.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func FuzzIPLineFilter(f *testing.F) {
	for _, tt := range ipLineFilterTests {
		f.Add(tt.input, tt.pattern)
	}

	f.Fuzz(func(t *testing.T, line, pattern string) {
		f, err := buildLineFilter(&logql.LineFilter{
			Op: logql.OpEq,
			By: logql.LineFilterValue{
				Value: pattern,
				IP:    true,
			},
		})
		if err != nil {
			t.Skipf("Invalid pattern: %q", pattern)
		}

		// Ensure there is no crash.
		f.Process(1, line, logqlabels.LabelSet{})
	})
}

func Test_tryCaptureIPv4(t *testing.T) {
	tests := []struct {
		input   string
		capture string
	}{
		{`1.1.1.1`, `1.1.1.1`},
		{`10.1.1.1`, `10.1.1.1`},
		{`127.0.0.1`, `127.0.0.1`},
		{`127.0.0.1 foo`, `127.0.0.1`},

		{``, ``},
		{`1`, ``},
		{`foo`, ``},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, match := tryCaptureIPv4(tt.input)
			if tt.capture == "" {
				require.Empty(t, got)
				require.False(t, match)
				return
			}
			require.Equal(t, tt.capture, got)
			require.True(t, match)
		})
	}
}

func Test_tryCaptureIPv6(t *testing.T) {
	tests := []struct {
		input   string
		capture string
	}{
		{`::1`, `::1`},
		{`::1 foo`, `::1`},
		{`2001:db8::68`, `2001:db8::68`},
		{`2001:db8::68 foo`, `2001:db8::68`},

		{``, ``},
		{`:f`, ``},
		{`1`, ``},
		{`foo`, ``},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, match := tryCaptureIPv6(tt.input)
			if tt.capture == "" {
				require.Empty(t, got)
				require.False(t, match)
				return
			}
			require.Equal(t, tt.capture, got)
			require.True(t, match)
		})
	}
}
