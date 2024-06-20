package chsql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSingleToken(t *testing.T) {
	tests := []struct {
		s    string
		want bool
	}{
		{``, false},
		{`10`, true},
		{`abc`, true},
		{`помидоры`, true},
		{`abc 10`, false},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			require.Equal(t, tt.want, IsSingleToken(tt.s))
		})
	}
}
