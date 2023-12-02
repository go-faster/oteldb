package logqlpattern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql"
)

var matchTests = []struct {
	pattern string
	input   string
	match   map[string]string
	full    bool
}{
	{
		"status:<status>",
		`status:200`,
		map[string]string{
			"status": "200",
		},
		true,
	},
	{
		"<line>",
		`line`,
		map[string]string{
			"line": "line",
		},
		true,
	},
	{
		"<prefix>:<_>",
		`abc`,
		map[string]string{
			"prefix": "abc",
		},
		false,
	},
	{
		"<method> <path>",
		`GET /foo`,
		map[string]string{
			"method": "GET",
			"path":   "/foo",
		},
		true,
	},
	{
		`<ip> - <user> [<_>] "<method> <path> <_>" <status> <size> <user_agent> <_>`,
		`127.0.0.1 - - [01/Jan/2000:00:00:00 +0000] "GET /foo HTTP/1.1" 200 1337 "UserAgent" "13.76.247.102, 34.120.177.193" "TLSv1.2" "US" ""`,
		map[string]string{
			"ip":         "127.0.0.1",
			"user":       "-",
			"method":     "GET",
			"path":       "/foo",
			"status":     "200",
			"size":       "1337",
			"user_agent": `"UserAgent"`,
		},
		true,
	},

	// No match.
	{
		"status:<status>",
		``,
		map[string]string{},
		false,
	},
}

func TestMatch(t *testing.T) {
	for i, tt := range matchTests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			compiled, err := Parse(tt.pattern)
			require.NoError(t, err)

			matches := map[string]string{}
			fullMatch := Match(compiled, tt.input, func(label logql.Label, value string) {
				matches[string(label)] = value
			})
			require.Equal(t, tt.match, matches)
			require.Equal(t, tt.full, fullMatch)
		})
	}
}

func FuzzMatch(f *testing.F) {
	for _, tt := range matchTests {
		f.Add(tt.pattern, tt.input)
	}
	f.Fuzz(func(t *testing.T, pattern, input string) {
		compiled, err := Parse(pattern)
		if err != nil {
			t.Skipf("Invalid pattern %q: %+v", pattern, err)
			return
		}
		Match(compiled, input, func(logql.Label, string) {})
	})
}
