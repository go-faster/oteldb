package otelstorage

import (
	"fmt"
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
