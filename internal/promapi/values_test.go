package promapi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestV(t *testing.T) {
	t.Run("Negative", func(t *testing.T) {
		for i, tc := range []Value{
			{},
			{
				{},
			},
			{
				{}, {},
			},
			{
				{}, {}, {},
			},
			{
				NewStringValueItem(""), NewStringValueItem(""),
			},
			{
				NewStringValueItem(""), NewStringValueItem(""), NewStringValueItem(""),
			},
			{
				NewFloat64ValueItem(0), NewFloat64ValueItem(0),
			},
			{
				NewFloat64ValueItem(0), NewStringValueItem(""), NewStringValueItem(""),
			},
			{
				NewFloat64ValueItem(0), NewStringValueItem(""), NewFloat64ValueItem(0),
			},
		} {
			require.Panicsf(t, func() {
				_ = tc.ToV()
			}, "[%d]", i)
		}
	})
	t.Run("Positive", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			v     V
			value Value
		}{
			{
				name:  "value",
				v:     NewV(1, "0"),
				value: Value{NewFloat64ValueItem(1), NewStringValueItem("0")},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.v, tc.value.ToV(), "ToV")
				require.Equal(t, tc.value, tc.v.ToValue(), "ToValue")
			})
		}
	})
}
