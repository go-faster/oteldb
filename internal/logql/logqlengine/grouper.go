package logqlengine

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

type grouper interface {
	Group(e entry) (key string, metric lokiapi.LabelSet)
}

func buildGrouper(g *logql.Grouping) grouper {
	switch {
	case g == nil:
		return &nopGrouper{}
	case g.Without:
		m := make(map[logql.Label]struct{}, len(g.Labels))
		for _, l := range g.Labels {
			m[l] = struct{}{}
		}
		return &withoutGrouper{without: m}
	default:
		return &byGrouper{by: g.Labels}
	}
}

type nopGrouper struct{}

func (*nopGrouper) Group(e entry) (string, lokiapi.LabelSet) {
	return e.set.String(), e.set.AsLokiAPI()
}

type byGrouper struct {
	by []logql.Label
}

func (g *byGrouper) Group(e entry) (key string, metric lokiapi.LabelSet) {
	metric = make(lokiapi.LabelSet, len(g.by))

	var keyBuilder strings.Builder
	for _, l := range g.by {
		if val, ok := e.set.GetString(l); ok {
			metric[string(l)] = val
			keyBuilder.WriteString(string(l))
			keyBuilder.WriteString(val)
		}
	}

	return keyBuilder.String(), metric
}

type withoutGrouper struct {
	without map[logql.Label]struct{}
}

func (g *withoutGrouper) Group(e entry) (key string, metric lokiapi.LabelSet) {
	metric = lokiapi.LabelSet{}

	e.set.Range(func(l logql.Label, v pcommon.Value) {
		if _, ok := g.without[l]; ok {
			return
		}
		metric[string(l)] = v.AsString()
	})

	// Use ordered iteration over map to compute key.
	var (
		keyBuilder strings.Builder
		keys       = maps.Keys(metric)
	)
	slices.Sort(keys)
	for _, l := range keys {
		val := metric[l]
		keyBuilder.WriteString(l)
		keyBuilder.WriteString(val)
	}

	return keyBuilder.String(), metric
}
