package chstorage

import (
	"context"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
)

var _ logqlengine.Querier = (*Querier)(nil)

// Capabilities implements logqlengine.Querier.
func (q *Querier) Capabilities() (caps logqlengine.QuerierCapabilities) {
	caps.Label.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	caps.Line.Add(logql.OpEq, logql.OpNotEq, logql.OpRe, logql.OpNotRe)
	return caps
}

// Query creates new [InputNode].
func (q *Querier) Query(ctx context.Context, labels []logql.LabelMatcher) (logqlengine.PipelineNode, error) {
	return &InputNode{
		Labels: labels,
		q:      q,
	}, nil
}

// InputNode rebuilds LogQL pipeline in as Clickhouse query.
type InputNode struct {
	Labels []logql.LabelMatcher
	Line   []logql.LineFilter
	q      *Querier
}

var _ logqlengine.PipelineNode = (*InputNode)(nil)

// Traverse implements [logqlengine.Node].
func (n *InputNode) Traverse(cb logqlengine.NodeVisitor) error {
	return cb(n)
}

// EvalPipeline implements [logqlengine.PipelineNode].
func (n *InputNode) EvalPipeline(ctx context.Context, params logqlengine.EvalParams) (logqlengine.EntryIterator, error) {
	q := LogsQuery[logqlengine.Entry]{
		Start:     params.Start,
		End:       params.End,
		Direction: params.Direction,
		Labels:    n.Labels,
		Line:      n.Line,
		Mapper:    entryMapper,
	}
	return q.Eval(ctx, n.q)
}

func entryMapper(r logstorage.Record) (logqlengine.Entry, error) {
	set := logqlengine.NewLabelSet()
	e := logqlengine.Entry{
		Timestamp: r.Timestamp,
		Line:      r.Body,
		Set:       set,
	}
	set.SetFromRecord(r)
	return e, nil
}
