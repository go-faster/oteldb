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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"sigs.k8s.io/yaml"
)

func generateDDL(columns map[string]Entry) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE columns (\n")

	groups := make(map[string][]Entry)
	for _, c := range columns {
		prefix := c.Group
		groups[prefix] = append(groups[prefix], c)
	}

	orderedGroups := maps.Keys(groups)
	slices.Sort(orderedGroups)

	for i, groupName := range orderedGroups {
		if groupName != "" {
			sb.WriteString(fmt.Sprintf("  -- %s\n", groupName))
		}
		maxFieldLen := 10
		for _, c := range groups[groupName] {
			if len(c.Name) > maxFieldLen {
				maxFieldLen = len(c.Name)
			}
		}
		slices.SortFunc(groups[groupName], func(a, b Entry) int {
			return strings.Compare(a.Name, b.Name)
		})
		lastGroup := i == len(orderedGroups)-1
		for j, c := range groups[groupName] {
			sb.WriteString("  ")
			sb.WriteString(fmt.Sprintf("%-*s %s", maxFieldLen, c.Name, c.Column))
			sb.WriteString(fmt.Sprintf(" COMMENT '%s'", c.FullName))
			if !(lastGroup && j == len(groups[groupName])-1) {
				sb.WriteString(",\n")
			}
		}
		sb.WriteString("\n")
	}
	sb.WriteString(") ENGINE Null;")
	return sb.String()
}

func anyTo[T any](s []any) (result []T) {
	result = make([]T, len(s))
	for i, v := range s {
		result[i] = v.(T)
	}
	return result
}

func columnType(name, brief, t string, enum []any) proto.ColumnType {
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
		if strings.Contains(brief, "ISO 8601") && t == "string" {
			// Just seconds?
			return proto.ColumnTypeDateTime
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
	params = append(params, "'' = 0")
	for i, v := range anyTo[string](enum) {
		// Should we escape?
		params = append(params, fmt.Sprintf("'%s' = %d", v, i+1))
	}
	return colType.With(params...)
}

func appendSet[T comparable](s []T, v T) []T {
	for _, e := range s {
		if e == v {
			return s
		}
	}
	return append(s, v)
}

func TestParseAllAttributes(t *testing.T) {
	var parsed []TypeGroupsItem

	groupsToWhere := map[string][]Where{}

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
		for _, group := range schema.Groups {
			groupName := group.Prefix.Value
			dir := filepath.Dir(path)
			switch {
			case strings.Contains(dir, "resource"):
				groupsToWhere[groupName] = appendSet(groupsToWhere[groupName], WhereResource)
			case strings.Contains(dir, "scope"):
				groupsToWhere[groupName] = appendSet(groupsToWhere[groupName], WhereScope)
			}
		}
		parsed = append(parsed, schema.Groups...)
		return nil
	}))

	out := Registry{
		Entries: map[string]Entry{},
	}
	for _, group := range parsed {
		for _, attr := range group.Attributes {
			if attr.Attribute.Stability.Value == "deprecated" {
				out.Statistics.Deprecated++
				continue
			}
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
			groupName := group.Prefix.Value
			if i := strings.Index(name, "."); i != -1 {
				groupName = name[:i]
			}
			where := groupsToWhere[groupName]
			if groupName == "k8s" {
				where = appendSet(where, WhereResource)
				where = appendSet(where, WhereAttribute)
			}
			if len(where) == 0 {
				where = append(where, WhereAttribute)
			}
			out.Entries[name] = Entry{
				FullName: name,
				Group:    groupName,
				Type:     typ,
				Enum:     enum,
				Column:   columnType(name, v.Brief.Value, typ, enum),
				Examples: examples,
				Brief:    v.Brief.Value,
				Name:     strings.ReplaceAll(name, ".", "_"),
				Where:    where,
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
	}

	data, err := yaml.Marshal(out)
	require.NoError(t, err)

	gold.Str(t, string(data), "registry.yaml")
	gold.Str(t, generateDDL(out.Entries), "ddl.sql")

	assert.Zero(t, out.Statistics.Unknown, "Should be no unknown types")
}

func TestData(t *testing.T) {
	require.NotNil(t, Data)
	require.NotEmpty(t, Data.Entries)
}
