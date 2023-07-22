package ytlocal

import (
	"testing"
)

func Test_GenerateCellID(t *testing.T) {
	for _, tc := range []struct {
		Tag      int16
		Name     string
		Expected string
	}{
		{
			Tag:      112,
			Name:     "test",
			Expected: "98da34e1-95e1bf50-00700259-96e17d7a",
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			got := GenerateCellID(tc.Tag, tc.Name)
			if got != tc.Expected {
				t.Errorf("expected %v, got %v", tc.Expected, got)
			}
		})
	}
}
