package chsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInTimeRange(t *testing.T) {
	tests := []struct {
		column string
		start  time.Time
		end    time.Time
		want   string
	}{
		{"timestamp", time.Time{}, time.Time{}, "true"},
		{"timestamp", time.Unix(0, 1), time.Time{}, "timestamp >= toUnixTimestamp64Nano(1)"},
		{"timestamp", time.Time{}, time.Unix(0, 10), "timestamp <= toUnixTimestamp64Nano(10)"},
		{"timestamp", time.Unix(0, 1), time.Unix(0, 10), "timestamp >= toUnixTimestamp64Nano(1) AND timestamp <= toUnixTimestamp64Nano(10)"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := InTimeRange(tt.column, tt.start, tt.end)

			p := GetPrinter()
			require.NoError(t, p.WriteExpr(got))
			require.Equal(t, tt.want, p.String())
		})
	}
}

func TestJoinAnd(t *testing.T) {
	tests := []struct {
		args    []Expr
		want    string
		wantErr bool
	}{
		{nil, "true", false},
		{
			[]Expr{
				Ident("foo"),
				Ident("bar"),
			},
			"foo AND bar",
			false,
		},
		{
			[]Expr{
				Ident("foo"),
				Ident("bar"),
				Ident("baz"),
			},
			"foo AND bar AND baz",
			false,
		},

		{[]Expr{Ident("foo")}, "", true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := JoinAnd(tt.args...)

			p := GetPrinter()
			err := p.WriteExpr(got)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, p.String())
		})
	}
}
