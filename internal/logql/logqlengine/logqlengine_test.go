package logqlengine

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlabels"
)

func newLabelSet[S ~string](m map[S]pcommon.Value) logqlabels.LabelSet {
	set := logqlabels.NewLabelSet()
	for k, v := range m {
		set.Set(logql.Label(k), v)
	}
	return set
}
