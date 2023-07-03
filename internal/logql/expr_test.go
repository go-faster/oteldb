package logql

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReduceBinOp(t *testing.T) {
	tests := []struct {
		input  string
		want   float64
		reduce bool
	}{
		{`1 + 1`, 2, true},
		{`2 - 1`, 1, true},
		{`2 * 2`, 4, true},
		{`2 / 2`, 1, true},
		{`2 / 0`, math.NaN(), true},
		{`5 % 2`, 1, true},
		{`5 % 0`, math.NaN(), true},
		{`2 ^ 3`, 8, true},

		{`2 + 2 * 2`, 6, true},
		{`(2 + 2) * 2`, 8, true},
		{`2 * 2 ^ 2`, 8, true},
		{`(2 * 2) ^ 2`, 16, true},

		{`2 == 2`, 1., true},
		{`2 == 3`, 0., true},
		{`2 != 2`, 0., true},
		{`2 != 3`, 1., true},

		{`3 > 2`, 1., true},
		{`3 > 3`, 0., true},
		{`3 > 4`, 0., true},
		{`3 >= 2`, 1., true},
		{`3 >= 3`, 1., true},
		{`3 >= 4`, 0., true},

		{`2 < 3`, 1., true},
		{`2 < 2`, 0., true},
		{`2 < 1`, 0., true},
		{`2 <= 3`, 1., true},
		{`2 <= 2`, 1., true},
		{`2 <= 1`, 0., true},

		{`{} + {}`, 0, false},
		{`0 + {}`, 0, false},
		{`{} + 0`, 0, false},
		{`({} + {}) * 2`, 0, false},
		{`2 * ({} + {})`, 0, false},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			expr, err := Parse(tt.input)
			require.NoError(t, err, "invalid input")

			require.IsType(t, &BinOpExpr{}, expr, "wrong expression type")
			binOp := expr.(*BinOpExpr)

			result, err := ReduceBinOp(binOp)
			require.NoError(t, err)

			if !tt.reduce {
				require.Nil(t, result)
				return
			}
			require.NotNil(t, result)

			if math.IsNaN(tt.want) {
				require.True(t, math.IsNaN(result.Value))
			} else {
				require.Equal(t, tt.want, result.Value)
			}
		})
	}
}
