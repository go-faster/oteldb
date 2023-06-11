package promapi

import "testing"

func TestFailToCode(t *testing.T) {
	// Test cases.
	for _, tc := range []struct {
		name string
		f    FailErrorType
		want int
	}{
		{
			name: "BadData",
			f:    FailErrorTypeBadData,
			want: 400,
		},
		{
			name: "Execution",
			f:    FailErrorTypeExecution,
			want: 422,
		},
		{
			name: "Canceled",
			f:    FailErrorTypeCanceled,
			want: 503,
		},
		{
			name: "Timeout",
			f:    FailErrorTypeTimeout,
			want: 503,
		},
		{
			name: "Internal",
			f:    FailErrorTypeInternal,
			want: 500,
		},
		{
			name: "NotFound",
			f:    FailErrorTypeNotFound,
			want: 404,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := FailToCode(tc.f)
			if got != tc.want {
				t.Errorf("FailToCode(%v) = %v, want %v", tc.f, got, tc.want)
			}
		})
	}
}
