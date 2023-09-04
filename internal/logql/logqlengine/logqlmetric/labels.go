package logqlmetric

import (
	"regexp"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

// GroupingKey is a key to group metrics by label.
type GroupingKey = uint64

// AggregatedLabels is a set of labels.
type AggregatedLabels interface {
	// By returns new set of labels containing only given list of labels.
	By(...logql.Label) AggregatedLabels
	// Without returns new set of labels without given list of labels.
	Without(...logql.Label) AggregatedLabels
	// Key computes grouping key from set of labels.
	Key() GroupingKey
	// Replace replaces labels using given regexp.
	Replace(dstLabel, replacement, srcLabel string, re *regexp.Regexp) AggregatedLabels

	// AsLokiAPI returns API structure for label set.
	AsLokiAPI() lokiapi.LabelSet
}

type emptyLabels struct{}

func (l *emptyLabels) By(_ ...logql.Label) AggregatedLabels                      { return l }
func (l *emptyLabels) Without(_ ...logql.Label) AggregatedLabels                 { return l }
func (l *emptyLabels) Key() GroupingKey                                          { return 0 }
func (l *emptyLabels) Replace(_, _, _ string, _ *regexp.Regexp) AggregatedLabels { return l }
func (l *emptyLabels) AsLokiAPI() lokiapi.LabelSet                               { return lokiapi.LabelSet{} }
