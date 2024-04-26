package logqlmetric

import "github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"

func emptyLabels() logqlabels.AggregatedLabels {
	return logqlabels.EmptyAggregatedLabels()
}

func mapLabels(m map[string]string) logqlabels.AggregatedLabels {
	return logqlabels.AggregatedLabelsFromMap(m)
}
