package logqlengine

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestAlignLeft(t *testing.T) {
	tests := []struct {
		input string
		count int
		want  string
	}{
		{``, 2, `  `},
		{``, 3, `   `},
		{`a`, 2, `a `},
		{`a`, 3, `a  `},
		{`ab`, 2, `ab`},
		{`abc`, 2, `ab`},
		{`хлеб`, 3, `хле`},
		{`хлеб`, 4, `хлеб`},
		{`хлеб`, 5, `хлеб `},

		{`хлеб`, -1, `хлеб`},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := alignLeft(tt.count, tt.input)
			require.Equal(t, tt.want, got)
			if tt.count >= 0 {
				require.Equal(t, utf8.RuneCountInString(got), tt.count)
			}
		})
	}
}

func TestAlignRight(t *testing.T) {
	tests := []struct {
		input string
		count int
		want  string
	}{
		{``, 2, `  `},
		{``, 3, `   `},
		{`a`, 2, ` a`},
		{`a`, 3, `  a`},
		{`ab`, 2, `ab`},
		{`abc`, 2, `bc`},
		{`хлеб`, 3, `леб`},
		{`хлеб`, 4, `хлеб`},
		{`хлеб`, 5, ` хлеб`},

		{`хлеб`, -1, `хлеб`},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			got := alignRight(tt.count, tt.input)
			require.Equal(t, tt.want, got)
			if tt.count >= 0 {
				require.Equal(t, utf8.RuneCountInString(got), tt.count)
			}
		})
	}
}

func FuzzAlign(f *testing.F) {
	f.Add(2, ``)
	f.Add(2, `foo`)
	f.Add(3, `хлеб`)
	f.Add(4, `хлеб`)
	f.Add(5, `хлеб`)

	f.Fuzz(func(t *testing.T, count int, input string) {
		if !utf8.ValidString(input) {
			t.Skip("Invalid UTF-8")
			return
		}

		gotLeft := alignLeft(count, input)
		gotRight := alignRight(count, input)

		if count >= 0 {
			require.Equal(t, utf8.RuneCountInString(gotLeft), count)
			require.Equal(t, utf8.RuneCountInString(gotRight), count)
		} else {
			require.Equal(t, gotLeft, input)
			require.Equal(t, gotRight, input)
		}
	})
}
