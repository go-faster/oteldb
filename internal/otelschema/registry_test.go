package otelschema

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

type registryEntry struct {
	Type     string           `json:"type"`
	Enum     []any            `json:"enum,omitempty"`
	Column   proto.ColumnType `json:"column"`
	Examples []any            `json:"examples,omitempty"`
	Brief    string           `json:"brief,omitempty"`
}

func anyTo[T any](s []any) (result []T) {
	result = make([]T, len(s))
	for i, v := range s {
		result[i] = v.(T)
	}
	return result
}

func columnType(name, t string, enum []any) proto.ColumnType {
	isEnum := len(enum) > 0
	if !isEnum || t == "int" {
		// We don't care about templates for storage data types.
		if strings.HasPrefix(t, "template[") {
			t = strings.TrimPrefix(t, "template[")
			t = strings.TrimSuffix(t, "]")
		}
		colType := map[string]proto.ColumnType{
			"string":   proto.ColumnTypeString,
			"int":      proto.ColumnTypeInt64,
			"boolean":  proto.ColumnTypeBool,
			"string[]": proto.ColumnTypeString.Array(),
			"int[]":    proto.ColumnTypeInt64.Array(),
			"double":   proto.ColumnTypeFloat64,
		}[t]
		if strings.HasPrefix(name, "k8s.") && strings.HasSuffix(name, ".uid") {
			// k8s.cluster.uid and others special case.
			return proto.ColumnTypeUUID
		}
		if strings.HasSuffix(name, ".port") {
			return proto.ColumnTypeUInt16
		}

		if isEnum {
			// Handle enum of integers.
			values := anyTo[int](enum)
			maxValue := slices.Max(values)
			minValue := slices.Min(values)
			switch {
			case minValue >= 0 && maxValue <= 255:
				colType = proto.ColumnTypeUInt8
			case minValue >= 0 && maxValue <= 65535:
				colType = proto.ColumnTypeUInt16
			case minValue < 0 && maxValue <= 127:
				colType = proto.ColumnTypeInt8
			default:
				colType = proto.ColumnTypeInt64
			}
		}
		if colType == "" {
			return "UNKNOWN"
		}
		return colType
	}

	// Handle enums.
	colType := proto.ColumnTypeEnum8
	if len(enum) > 255 {
		colType = proto.ColumnTypeEnum16
	}
	var params []string
	for i, v := range anyTo[string](enum) {
		// Should we escape?
		params = append(params, fmt.Sprintf("%d = '%s'", i, v))
	}
	return colType.With(params...)
}

func TestParseAllAttributes(t *testing.T) {
	var parsed []TypeGroupsItem
	require.NoError(t, filepath.Walk(modelPath(), func(path string, info fs.FileInfo, err error) error {
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

	type Statistics struct {
		Total   int `json:"total"`
		Enum    int `json:"enum"`
		Unknown int `json:"unknown"`
	}
	type Registry struct {
		Statistics Statistics               `json:"statistics"`
		Entries    map[string]registryEntry `json:"entries"`
	}
	out := Registry{
		Entries: map[string]registryEntry{},
	}
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
			var typ string
			if s, ok := v.Type.GetString(); ok {
				typ = s
			}
			var enum []interface{}
			if e, ok := v.Type.GetEnum(); ok {
				for _, m := range e.Members {
					switch m.Value.Type {
					case StringEnumMembersItemValue:
						enum = append(enum, m.Value.String)
						typ = "string"
					case IntEnumMembersItemValue:
						enum = append(enum, m.Value.Int)
						typ = "int"
					}
				}
				if e.AllowCustomValues.Value {
					// Not actually an enum?
					enum = nil
				}
			}
			t.Logf("%s (%s)", name, typ)
			var examples []any

			if e, ok := v.Examples.Get(); ok {
				toAny := func(ev ExampleValue) any {
					switch ev.Type {
					case BoolExampleValue:
						return ev.Bool
					case Float64ExampleValue:
						return ev.Float64
					case StringExampleValue:
						return ev.String
					}
					return nil
				}
				for _, v := range e.ExampleValueArray {
					examples = append(examples, toAny(v))
				}
				if v, ok := e.GetExampleValue(); ok {
					examples = append(examples, toAny(v))
				}
			}
			if len(enum) != 0 {
				examples = nil
			}
			out.Entries[name] = registryEntry{
				Type:     typ,
				Enum:     enum,
				Column:   columnType(name, typ, enum),
				Examples: examples,
				Brief:    v.Brief.Value,
			}
		}
	}

	t.Logf("total: %d", len(out.Entries))
	out.Statistics.Total = len(out.Entries)
	for _, e := range out.Entries {
		if e.Column == "UNKNOWN" {
			out.Statistics.Unknown++
		}
		if strings.HasPrefix(e.Column.String(), "Enum") {
			out.Statistics.Enum++
		}
		out.Statistics.Total++
	}

	data, err := yaml.Marshal(out)
	require.NoError(t, err)

	gold.Str(t, string(data), "registry.yaml")
}
