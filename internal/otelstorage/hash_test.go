package otelstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestHash(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("foo", "bar")
	require.NotEqual(t, AttrHash(m), Hash{})
	require.NotEqual(t, StrHash("foo"), Hash{})
}
