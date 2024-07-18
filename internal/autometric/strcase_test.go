package autometric

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_snakeCase(t *testing.T) {
	tests := []struct {
		s    string
		want string
	}{
		{"", ""},
		{"f", "f"},
		{"F", "f"},
		{"Foo", "foo"},
		{"FooB", "foo_b"},
		{" FooBar\t", "foo_bar"},
		{"foo__Bar", "foo_bar"},
		{"foo--Bar", "foo_bar"},
		{"foo  Bar", "foo_bar"},
		{"foo\tBar", "foo_bar"},
		{"Int64UpDownCounter", "int64_up_down_counter"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, snakeCase(tt.s))
		})
	}
}
