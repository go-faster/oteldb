package otelschema

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func modelPath() string {
	return filepath.Join("_testdata", "semantic-conventions", "model")
}

func TestParse(t *testing.T) {
	require.NoError(t, filepath.Walk(modelPath(), func(path string, info fs.FileInfo, err error) error {
		require.NoError(t, err)
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".yaml" {
			return nil
		}
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			var schema Type
			jsonData, err := yaml.YAMLToJSON(data)
			require.NoError(t, err)
			require.NoError(t, schema.UnmarshalJSON(jsonData))
		})
		return nil
	}))
}
