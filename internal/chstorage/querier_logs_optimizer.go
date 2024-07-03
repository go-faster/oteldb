package chstorage

import (
	"context"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
)

// ClickhouseOptimizer replaces LogQL engine execution
// nodes with optimzied Clickhouse queries.
type ClickhouseOptimizer struct{}

var _ logqlengine.Optimizer = (*ClickhouseOptimizer)(nil)

// Name returns optimizer name.
func (o *ClickhouseOptimizer) Name() string {
	return "ClickhouseOptimizer"
}

// Optimize implements [Optimizer].
func (o *ClickhouseOptimizer) Optimize(ctx context.Context, q logqlengine.Query) (logqlengine.Query, error) {
	switch q := q.(type) {
	case *logqlengine.LogQuery:
		q.Root = o.optimizePipeline(q.Root)
	case *logqlengine.MetricQuery:
		if err := logqlengine.VisitNode(q.Root, func(n *logqlengine.SamplingNode) error {
			n.Input = o.optimizePipeline(n.Input)
			return nil
		}); err != nil {
			return nil, err
		}
		q.Root = o.optimizeSampling(q.Root)
	}
	return q, nil
}

func (o *ClickhouseOptimizer) optimizeSampling(n logqlengine.MetricNode) logqlengine.MetricNode {
	switch n := n.(type) {
	case *logqlengine.VectorAggregation:
		switch n.Expr.Op {
		case logql.VectorOpBottomk, logql.VectorOpTopk,
			logql.VectorOpSort, logql.VectorOpSortDesc:
			return n
		}

		labels, ok := getGroupByLabels(n.Expr.Grouping)
		if !ok {
			return n
		}

		rn, ok := n.Input.(*logqlengine.RangeAggregation)
		if !ok {
			return n
		}
		n.Input = o.buildRangeAggregationSampling(rn, labels)

		return n
	case *logqlengine.LabelReplace:
		n.Input = o.optimizeSampling(n.Input)
		return n
	case *logqlengine.LiteralBinOp:
		n.Input = o.optimizeSampling(n.Input)
		return n
	case *logqlengine.BinOp:
		n.Left = o.optimizeSampling(n.Left)
		n.Right = o.optimizeSampling(n.Right)
		return n
	default:
		return n
	}
}

func (o *ClickhouseOptimizer) buildRangeAggregationSampling(n *logqlengine.RangeAggregation, grouping []logql.Label) logqlengine.MetricNode {
	if g := n.Expr.Grouping; g != nil {
		return n
	}

	sampleNode, ok := n.Input.(*logqlengine.SamplingNode)
	if !ok {
		return n
	}

	// If it is possible to offload the pipeline to Clickhouse entirely
	// preceding optimizer should replace node with [InputNode].
	pipelineNode, ok := sampleNode.Input.(*InputNode)
	if !ok {
		return n
	}

	samplingOp, ok := getSamplingOp(n.Expr)
	if !ok {
		return n
	}

	n.Input = &SamplingNode{
		Sampling:       samplingOp,
		GroupingLabels: grouping,
		Labels:         pipelineNode.Labels,
		Line:           pipelineNode.Line,
		q:              pipelineNode.q,
	}
	return n
}

func getGroupByLabels(g *logql.Grouping) ([]logql.Label, bool) {
	if g == nil || g.Without || len(g.Labels) == 0 {
		return nil, false
	}
	return g.Labels, true
}

func getSamplingOp(e *logql.RangeAggregationExpr) (op SamplingOp, _ bool) {
	if er := e.Range; er.Unwrap != nil || er.Offset != nil {
		return op, false
	}
	switch e.Op {
	case logql.RangeOpCount:
		return CountSampling, true
	case logql.RangeOpBytes:
		return BytesSampling, true
	default:
		return op, false
	}
}

func (o *ClickhouseOptimizer) optimizePipeline(n logqlengine.PipelineNode) logqlengine.PipelineNode {
	pn, ok := n.(*logqlengine.ProcessorNode)
	if !ok {
		return n
	}

	sn, ok := pn.Input.(*InputNode)
	if !ok {
		// NOTE(tdakkota): this should not happen as long
		// 	as there is only one possible node made by storage.
		return n
	}

	var (
		line          []logql.LineFilter
		skippedStages int
	)
stageLoop:
	for _, stage := range pn.Pipeline {
		switch stage := stage.(type) {
		case *logql.LineFilter:
			if !o.canOffloadLineFilter(stage) {
				skippedStages++
				continue
			}
			// TODO(tdakkota): remove stages from pipeline.
			line = append(line, *stage)
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
			skippedStages++
		case *logql.LineFormat,
			*logql.DecolorizeExpr,
			*logql.UnpackLabelParser:
			// Stage modify the line, can't offload line filters after this stage.
			skippedStages++
			break stageLoop
		}
	}
	sn.Line = line
	// Replace original node with [InputNode], since we can execute filtering entirely in
	// Clickhouse.
	if skippedStages == 0 && !pn.EnableOTELAdapter {
		return sn
	}
	return n
}

func (o *ClickhouseOptimizer) canOffloadLineFilter(lf *logql.LineFilter) bool {
	switch lf.Op {
	case logql.OpPattern, logql.OpNotPattern:
		return false
	}
	if lf.By.IP {
		return false
	}
	for _, by := range lf.Or {
		if by.IP {
			return false
		}
	}
	return true
}
