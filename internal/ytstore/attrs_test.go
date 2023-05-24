package ytstore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestYSON(t *testing.T) {
	attrs := Attrs[string]{
		{"key", "value"},
		{"key2", "value2"},
	}

	data, err := yson.Marshal(attrs)
	require.NoError(t, err)

	var attrs2 Attrs[string]
	require.NoError(t, yson.Unmarshal(data, &attrs2))
	t.Logf("%#v", attrs2)
}
