package otelstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"
)

var testTraceID = TraceID{
	10, 20, 30, 40, 50, 60, 70, 80,
	80, 70, 60, 50, 40, 30, 20, 10,
}

func TestTraceIDYSON(t *testing.T) {
	tests := []struct {
		ID TraceID
	}{
		{testTraceID},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			id := tt.ID

			data, err := yson.Marshal(id)
			require.NoError(t, err)

			var id2 TraceID
			require.NoError(t, yson.Unmarshal(data, &id2))

			require.Equal(t, id, id2)
		})
	}
	t.Run("NilCheck", func(t *testing.T) {
		var idNil *TraceID
		require.EqualError(t,
			idNil.UnmarshalYSON(yson.NewReader(nil)),
			"can't unmarshal to (*tracestorage.TraceID)(nil)",
		)
	})
}

func TestTraceID_Hex(t *testing.T) {
	id := testTraceID
	require.Equal(t, id.Hex(), pcommon.TraceID(id[:]).String())
}

var testSpanID = SpanID{
	10, 20, 30, 40, 50, 60, 70, 80,
}

func TestSpanIDYSON(t *testing.T) {
	tests := []struct {
		ID SpanID
	}{
		{testSpanID},
		{SpanID{}},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			id := tt.ID

			data, err := yson.Marshal(id)
			require.NoError(t, err)

			var id2 SpanID
			require.NoError(t, yson.Unmarshal(data, &id2))

			require.Equal(t, id, id2)
		})
	}
	t.Run("NilCheck", func(t *testing.T) {
		var idNil *SpanID
		require.EqualError(t,
			idNil.UnmarshalYSON(yson.NewReader(nil)),
			"can't unmarshal to (*tracestorage.SpanID)(nil)",
		)
	})
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
