package lokicompliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpandQuery(t *testing.T) {
	arg := "offset"
	cfg := &Config{
		TestCases: []*TestCasePattern{
			{
				Query:       "{{ ." + arg + " }}",
				VariantArgs: []string{arg},
			},
		},
	}

	ts := time.Time{}
	tcs, err := ExpandQuery(cfg, ts, ts, 0)
	require.NoError(t, err)

	var queries []string
	for _, tc := range tcs {
		queries = append(queries, tc.Query)
	}
	require.ElementsMatch(t, queries, testVariantArgs[arg])
}
