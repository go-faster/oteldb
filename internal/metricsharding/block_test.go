package metricsharding

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTenantID(t *testing.T) {
	tests := []struct {
		input   string
		want    TenantID
		wantErr bool
	}{
		{`tenant-19`, 19, false},

		{``, 0, true},
		{`tenant-`, 0, true},
		{`10`, 0, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			var (
				got   TenantID
				input = []byte(tt.input)
			)
			err := got.UnmarshalText(input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			input2, err := got.MarshalText()
			require.NoError(t, err)
			require.Equal(t, input, input2)
			require.Equal(t, tt.input, got.String())
		})
	}
}
