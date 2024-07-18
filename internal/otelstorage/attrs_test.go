package otelstorage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyToLabel(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"", ""},
		{"foo", "foo"},
		{"f_oo", "f_oo"},

		{"0foo", "_0foo"},
		{"foo.bar", "foo_bar"},
		{"foo/bar", "foo_bar"},
		{"receiver/accepted_spans/0", "receiver_accepted_spans_0"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, KeyToLabel(tt.key))
		})
	}
}

func TestKeyToLabelAllocs(t *testing.T) {
	var sink string
	for _, tt := range []struct {
		key    string
		allocs float64
	}{
		{"", 0},
		{"foo", 0},
		{"foo.bar", 1},
		{"receiver/accepted_spans/0", 1},
		{"_" + strings.Repeat("receiver/accepted_spans/0", 25), 1},
	} {
		got := testing.AllocsPerRun(1000, func() {
			sink = KeyToLabel(tt.key)
		})
		require.LessOrEqual(t, tt.allocs, got)
		if tt.key != "" && sink == "" {
			t.Fail()
		}
	}
}
