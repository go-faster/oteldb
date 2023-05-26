package ytstore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestTraceIDYSON(t *testing.T) {
	id := TraceID{
		10, 20, 30, 40, 50, 60, 70, 80,
		80, 70, 60, 50, 40, 30, 20, 10,
	}

	data, err := yson.Marshal(id)
	require.NoError(t, err)

	var id2 TraceID
	require.NoError(t, yson.Unmarshal(data, &id2))

	require.Equal(t, id, id2)
}

func TestSpanIDYSON(t *testing.T) {
	id := SpanID{10, 20, 30, 40, 50, 60, 70, 80}

	data, err := yson.Marshal(id)
	require.NoError(t, err)

	var id2 SpanID
	require.NoError(t, yson.Unmarshal(data, &id2))

	require.Equal(t, id, id2)
}
