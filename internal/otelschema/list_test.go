package otelschema

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestParseAllAttributes(t *testing.T) {
	var parsed []TypeGroupsItem
	require.NoError(t, filepath.Walk(filepath.Join("_testdata", "model"), func(path string, info fs.FileInfo, err error) error {
		require.NoError(t, err)
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".yaml" {
			return nil
		}
		data, err := os.ReadFile(path)
		require.NoError(t, err)

		var schema Type
		jsonData, err := yaml.YAMLToJSON(data)
		require.NoError(t, err)
		require.NoError(t, schema.UnmarshalJSON(jsonData))
		parsed = append(parsed, schema.Groups...)
		return nil
	}))
	type entry struct {
		Name string
		Type string
	}
	var entries []entry
	for _, group := range parsed {
		for _, attr := range group.Attributes {
			v, ok := attr.GetAttribute()
			if !ok {
				continue
			}
			name := v.ID
			if prefix, ok := group.Prefix.Get(); ok {
				name = prefix + "." + name
			}
			typ := "enum"
			if s, ok := v.Type.GetString(); ok {
				typ = s
			}
			t.Logf("%s (%s)", name, typ)
			entries = append(entries, entry{
				Name: name,
				Type: typ,
			})
		}
	}
	t.Logf("total: %d", len(entries))
	data, err := yaml.Marshal(entries)
	require.NoError(t, err)

	gold.Str(t, string(data), "all_attributes.yaml")
}
