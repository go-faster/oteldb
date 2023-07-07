package logqlengine

import (
	"github.com/go-faster/oteldb/internal/logql"
)

type queryConditions struct {
	prefilter Processor
	params    SelectLogsParams
}

func extractQueryConditions(caps QuerierСapabilities, sel logql.Selector) (cond queryConditions, _ error) {
	var prefilters []Processor

	for _, lm := range sel.Matchers {
		// If storage supports the operation, just pass it as a parameter.
		if caps.Label.Supports(lm.Op) {
			cond.params.Labels = append(cond.params.Labels, lm)
			continue
		}

		// Otherwise, build a prefilter.
		proc, err := buildLabelMatcher(lm)
		if err != nil {
			return cond, err
		}
		prefilters = append(prefilters, proc)
	}

	switch len(prefilters) {
	case 0:
		cond.prefilter = NopProcessor
	case 1:
		cond.prefilter = prefilters[0]
	default:
		cond.prefilter = &Pipeline{Stages: prefilters}
	}

	return cond, nil
}
