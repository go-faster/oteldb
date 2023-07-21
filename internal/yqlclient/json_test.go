package yqlclient

import (
	"embed"
	"fmt"
	"path"
	"testing"

	"github.com/go-faster/jx"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/yqlclient/ytqueryapi"
)

//go:embed _testdata
var testdata embed.FS

func TestQueryStatusJSON(t *testing.T) {
	testJSON[ytqueryapi.QueryStatus](t, "_testdata/get_query")
}

func TestStartedQueryJSON(t *testing.T) {
	testJSON[ytqueryapi.StartedQuery](t, "_testdata/start_query")
}

func testJSON[T any, P interface {
	*T
	Encode(*jx.Encoder)
	Decode(*jx.Decoder) error
}](
	t *testing.T,
	root string,
) {
	files, err := testdata.ReadDir(root)
	require.NoError(t, err)

	for i, f := range files {
		f := f
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			data, err := testdata.ReadFile(path.Join(root, f.Name()))
			require.NoError(t, err)

			var resp T
			require.NoError(t, P(&resp).Decode(jx.DecodeBytes(data)))
		})
	}
}
