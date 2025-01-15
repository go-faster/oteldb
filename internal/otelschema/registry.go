package otelschema

import (
	_ "embed"
	"sort"

	"github.com/ClickHouse/ch-go/proto"
	"golang.org/x/exp/maps"
	"sigs.k8s.io/yaml"
)

//go:embed _golden/registry.yaml
var _registryData []byte

type Statistics struct {
	Total      int `json:"total"`
	Enum       int `json:"enum"`
	Unknown    int `json:"unknown"`
	Deprecated int `json:"deprecated"`
}
type Registry struct {
	Statistics Statistics       `json:"statistics"`
	Entries    map[string]Entry `json:"entries"`
}

func (r *Registry) All() []Entry {
	keys := maps.Keys(r.Entries)
	sort.Strings(keys)
	out := make([]Entry, 0, len(keys))
	for _, k := range keys {
		out = append(out, r.Entries[k])
	}
	return out
}

type Entry struct {
	FullName string           `json:"full_name,omitempty"`
	Group    string           `json:"group,omitempty"`
	Type     string           `json:"type"`
	Enum     []any            `json:"enum,omitempty"`
	Column   proto.ColumnType `json:"column"`
	Examples []any            `json:"examples,omitempty"`
	Brief    string           `json:"brief,omitempty"`
	Name     string           `json:"name,omitempty"`
	Where    string           `json:"where,omitempty"`
}

// Data is loaded otel schema registry.
var Data = loadRegistry()

func loadRegistry() *Registry {
	var out Registry
	err := yaml.Unmarshal(_registryData, &out)
	if err != nil {
		panic(err)
	}
	return &out
}
