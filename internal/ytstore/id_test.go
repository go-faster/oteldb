package ytstore

import (
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
	id := testTraceID
	data, err := yson.Marshal(id)
	require.NoError(t, err)

	var id2 TraceID
	require.NoError(t, yson.Unmarshal(data, &id2))

	require.Equal(t, id, id2)
}

func TestTraceID_Hex(t *testing.T) {
	id := testTraceID
	require.Equal(t, id.Hex(), pcommon.TraceID(id[:]).String())
}

var testSpanID = SpanID{
	10, 20, 30, 40, 50, 60, 70, 80,
}

func TestSpanIDYSON(t *testing.T) {
	id := testSpanID

	data, err := yson.Marshal(id)
	require.NoError(t, err)

	var id2 SpanID
	require.NoError(t, yson.Unmarshal(data, &id2))

	require.Equal(t, id, id2)
}

func TestSpanID_Hex(t *testing.T) {
	id := testSpanID
	require.Equal(t, id.Hex(), pcommon.SpanID(id[:]).String())
}
