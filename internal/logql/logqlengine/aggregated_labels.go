package logqlengine

import (
	"maps"
	"regexp"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlmetric"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

type aggregatedLabels struct {
	entries []labelEntry
	without map[string]struct{}
	by      map[string]struct{}
}

type labelEntry struct {
	name  string
	value string
}

func newAggregatedLabels(set LabelSet, by, without map[string]struct{}) *aggregatedLabels {
	labels := make([]labelEntry, 0, len(set.labels))
	set.Range(func(l logql.Label, v pcommon.Value) {
		labels = append(labels, labelEntry{
			name:  string(l),
			value: v.AsString(),
		})
	})

	return &aggregatedLabels{
		entries: labels,
		without: without,
		by:      by,
	}
}

// By returns new set of labels containing only given list of labels.
func (a *aggregatedLabels) By(labels ...logql.Label) logqlmetric.AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: a.without,
		by:      buildSet(maps.Clone(a.by), labels...),
	}
	return sub
}

// Without returns new set of labels without given list of labels.
func (a *aggregatedLabels) Without(labels ...logql.Label) logqlmetric.AggregatedLabels {
	if len(labels) == 0 {
		return a
	}

	sub := &aggregatedLabels{
		entries: a.entries,
		without: buildSet(maps.Clone(a.without), labels...),
		by:      a.by,
	}
	return sub
}

// Key computes grouping key from set of labels.
func (a *aggregatedLabels) Key() logqlmetric.GroupingKey {
	h := xxhash.New()
	a.forEach(func(k, v string) {
		_, _ = h.WriteString(k)
		_, _ = h.WriteString(v)
	})
	return h.Sum64()
}

// Replace replaces labels using given regexp.
func (a *aggregatedLabels) Replace(dstLabel, replacement, srcLabel string, re *regexp.Regexp) logqlmetric.AggregatedLabels {
	src := a.findEntry(srcLabel)

	idxs := re.FindStringSubmatchIndex(src)
	if idxs == nil {
		return a
	}

	dst := re.ExpandString(nil, replacement, src, idxs)
	if len(dst) == 0 {
		// Destination value is empty, delete it.
		a.deleteEntry(dstLabel)
	} else {
		a.setEntry(dstLabel, string(dst))
	}

	return a
}

func (a *aggregatedLabels) findEntry(key string) string {
	for _, e := range a.entries {
		if e.name == key {
			return e.value
		}
	}
	return ""
}

func (a *aggregatedLabels) deleteEntry(key string) {
	n := 0
	for _, e := range a.entries {
		if e.name == key {
			continue
		}
		a.entries[n] = e
		n++
	}
	a.entries = a.entries[:n]
}

func (a *aggregatedLabels) setEntry(key, value string) {
	var entry *labelEntry
	for i, e := range a.entries {
		if e.name == key {
			entry = &a.entries[i]
			break
		}
	}

	replacement := labelEntry{
		name:  key,
		value: value,
	}
	if entry == nil {
		a.entries = append(a.entries, replacement)
	} else {
		*entry = replacement
	}
}

// AsLokiAPI returns API structure for label set.
func (a *aggregatedLabels) AsLokiAPI() (r lokiapi.LabelSet) {
	r = lokiapi.LabelSet{}
	a.forEach(func(k, v string) {
		r[k] = v
	})
	return r
}

func (a *aggregatedLabels) forEach(cb func(k, v string)) {
	for _, e := range a.entries {
		if _, ok := a.without[e.name]; ok {
			continue
		}
		if len(a.by) > 0 {
			if _, ok := a.by[e.name]; !ok {
				continue
			}
		}
		cb(e.name, e.value)
	}
}

func buildSet[K ~string](r map[string]struct{}, input ...K) map[string]struct{} {
	if len(input) == 0 {
		return r
	}

	if r == nil {
		r = make(map[string]struct{}, len(input))
	}
	for _, k := range input {
		r[string(k)] = struct{}{}
	}
	return r
}
