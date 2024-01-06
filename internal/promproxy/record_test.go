package promproxy

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
)

func TestDecode(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "node-exporter.jsonl"))
	require.NoError(t, err)

	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var r Query
		err := dec.Decode(&r)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
}
