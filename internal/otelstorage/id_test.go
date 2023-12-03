package otelstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

var testTraceID = TraceID{
	10, 20, 30, 40, 50, 60, 70, 80,
	80, 70, 60, 50, 40, 30, 20, 10,
}

func TestTraceID_Hex(t *testing.T) {
	id := testTraceID
	require.Equal(t, id.Hex(), pcommon.TraceID(id[:]).String())
}

var testSpanID = SpanID{
	10, 20, 30, 40, 50, 60, 70, 80,
}

func TestSpanID_Hex(t *testing.T) {
	id := testSpanID
	require.Equal(t, id.Hex(), pcommon.SpanID(id[:]).String())
}

func TestParseTraceID(t *testing.T) {
	tests := []struct {
		input   string
		wantHex string
		wantErr bool
	}{
		{"0ab78e08df6f20dc3ad29d3915beab75", "0ab78e08df6f20dc3ad29d3915beab75", false},
		{"ab78e08df6f20dc3ad29d3915beab75", "0ab78e08df6f20dc3ad29d3915beab75", false},
		{"78e08df6f20dc3ad29d3915beab75", "00078e08df6f20dc3ad29d3915beab75", false},

		{"l", "", true},
		{"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "", true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			a := require.New(t)

			got, err := ParseTraceID(tt.input)
			if tt.wantErr {
				a.Error(err)
				return
			}
			a.NoError(err)
			a.Equal(tt.wantHex, got.Hex())
		})
	}
}
