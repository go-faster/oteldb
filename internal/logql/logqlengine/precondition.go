package logqlengine

import (
	"github.com/go-faster/oteldb/internal/logql"
)

type queryConditions struct {
	prefilter Processor
	params    SelectLogsParams
}

func extractQueryConditions(caps QuerierСapabilities, sel logql.Selector, stages []logql.PipelineStage) (cond queryConditions, _ error) {
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

stageLoop:
	for _, stage := range stages {
		switch stage := stage.(type) {
		case *logql.LineFilter:
			if stage.IP {
				// Do not offload IP line filter.
				continue
			}
			if !caps.Line.Supports(stage.Op) {
				continue
			}
			cond.params.Line = append(cond.params.Line, *stage)
		case *logql.JSONExpressionParser,
			*logql.LogfmtExpressionParser,
			*logql.RegexpLabelParser,
			*logql.PatternLabelParser,
			*logql.LabelFilter,
			*logql.LabelFormatExpr,
			*logql.DropLabelsExpr,
			*logql.KeepLabelsExpr,
			*logql.DistinctFilter:
			// Do nothing on line, just skip.
		case *logql.LineFormat,
			*logql.DecolorizeExpr,
			*logql.UnpackLabelParser:
			// Stage modify the line, can't offload line filters after this stage.
			break stageLoop
		}
	}

	return cond, nil
}
