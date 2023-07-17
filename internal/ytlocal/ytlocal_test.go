package ytlocal

import (
	"bytes"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func encode(t *testing.T, name string, v any) {
	t.Helper()
	out := new(bytes.Buffer)
	w := yson.NewWriterFormat(out, yson.FormatPretty)
	e := yson.NewEncoderWriter(w)
	require.NoError(t, e.Encode(v))
	gold.Str(t, out.String(), name+".yson")
}
