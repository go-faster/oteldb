package ytstore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestYSON(t *testing.T) {
	attrs := pcommon.NewMap()

	attrs.PutStr("str", "string")
	attrs.PutInt("int", 10)
	attrs.PutDouble("double", 3.14)
	attrs.PutBool("bool", true)
	attrs.PutEmptyBytes("bytes").Append([]byte("bytes")...)
	m := attrs.PutEmptyMap("map")
	m.PutStr("key", "value")
	s := attrs.PutEmptySlice("slice")
	v := s.AppendEmpty()
	v.SetStr("first element")

	data, err := yson.Marshal(Attrs(attrs))
	require.NoError(t, err)

	var attrs2 Attrs
	require.NoError(t, yson.Unmarshal(data, &attrs2))

	require.Equal(t, attrs.AsRaw(), pcommon.Map(attrs2).AsRaw())
}
