package promhandler

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Time
		wantErr bool
	}{
		{`1600000000.123`, time.UnixMilli(1600000000123).UTC(), false},
		{`2015-07-01T20:10:51.781Z`, time.Date(2015, 7, 1, 20, 10, 51, int(781*time.Millisecond), time.UTC), false},

		{`foo`, time.Time{}, true},
		{``, time.Time{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseTimestamp(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseStep(t *testing.T) {
	tests := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{`10.256`, 10*time.Second + 256*time.Millisecond, false},
		{`1s`, time.Second, false},
		{`1h`, time.Hour, false},

		// Non-positive steps are not allowed.
		{`0`, 0, true},
		{`-10`, 0, true},
		{`foo`, 0, true},
		{``, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseStep(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseLabelMatchers(t *testing.T) {
	tests := []struct {
		param   []string
		want    [][]*labels.Matcher
		wantErr bool
	}{
		{nil, nil, false},
		{
			[]string{
				`{foo=~"foo"}`,
				`{bar=~"bar", baz="baz"}`,
			},
			[][]*labels.Matcher{
				{labels.MustNewMatcher(labels.MatchRegexp, "foo", "foo")},
				{
					labels.MustNewMatcher(labels.MatchRegexp, "bar", "bar"),
					labels.MustNewMatcher(labels.MatchEqual, "baz", "baz"),
				},
			},
			false,
		},

		// Invalid syntax.
		{[]string{"{"}, nil, true},
		// Invalid regexp.
		{[]string{`{foo=~"\\"}`}, nil, true},
		// At least one matcher is required.
		{[]string{"{}"}, nil, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got, err := parseLabelMatchers(tt.param)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// LabelMatcher cannot be compared with DeepEqual.
			//
			// See https://github.com/prometheus/prometheus/blob/3b8b57700c469c7cde84e1d8f9d383cb8fe11ab0/promql/parser/parse_test.go#L3719.
			require.Len(t, got, len(tt.want))
			for i, set := range tt.want {
				gotSet := got[i]
				require.Len(t, gotSet, len(set))
				for i, m := range set {
					gotMatcher := gotSet[i]
					require.Equal(t, m.String(), gotMatcher.String())
				}
			}
		})
	}
}
