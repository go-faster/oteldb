package logqlengine

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/lokiapi"
)

// LogQuery represents a log query.
type LogQuery struct {
	Root             PipelineNode
	LookbackDuration time.Duration
}

var _ Query = (*LogQuery)(nil)

func (e *Engine) buildLogQuery(ctx context.Context, expr *logql.LogExpr) (Query, error) {
	root, err := e.buildPipelineNode(ctx, expr.Sel, expr.Pipeline)
	if err != nil {
		return nil, err
	}

	return &LogQuery{
		Root:             root,
		LookbackDuration: e.lookbackDuration,
	}, nil
}

// Eval implements [Query].
func (q *LogQuery) Eval(ctx context.Context, params EvalParams) (data lokiapi.QueryResponseData, _ error) {
	// Instant query, sub lookback duration from Start.
	if params.IsInstant() {
		params.Start = params.Start.Add(q.LookbackDuration)
	}
	if params.Direction == "" {
		params.Direction = DirectionForward
	}

	streams, err := q.eval(ctx, params)
	if err != nil {
		return data, errors.Wrap(err, "evaluate log query")
	}

	data.SetStreamsResult(lokiapi.StreamsResult{
		Result: streams,
	})
	return data, nil
}

func (q *LogQuery) eval(ctx context.Context, params EvalParams) (data lokiapi.Streams, _ error) {
	iter, err := q.Root.EvalPipeline(ctx, params)
	if err != nil {
		return data, err
	}
	defer func() {
		_ = iter.Close()
	}()

	streams, err := groupEntries(iter)
	if err != nil {
		return data, err
	}

	return streams, nil
}

func groupEntries(iter EntryIterator) (s lokiapi.Streams, _ error) {
	var (
		e       Entry
		streams = map[string]lokiapi.Stream{}
	)
	for iter.Next(&e) {
		// FIXME(tdakkota): allocates a string for every record.
		key := e.Set.String()
		stream, ok := streams[key]
		if !ok {
			stream = lokiapi.Stream{
				Stream: lokiapi.NewOptLabelSet(e.Set.AsLokiAPI()),
			}
		}
		stream.Values = append(stream.Values, lokiapi.LogEntry{T: uint64(e.Timestamp), V: e.Line})
		streams[key] = stream
	}
	if err := iter.Err(); err != nil {
		return s, err
	}

	result := maps.Values(streams)
	for _, stream := range result {
		slices.SortFunc(stream.Values, func(a, b lokiapi.LogEntry) int {
			return cmp.Compare(a.T, b.T)
		})
	}
	return result, nil
}

// ProcessorNode implements [PipelineNode].
type ProcessorNode struct {
	Input             PipelineNode
	Prefilter         Processor
	Selector          logql.Selector
	Pipeline          []logql.PipelineStage
	EnableOTELAdapter bool
}

var _ PipelineNode = (*ProcessorNode)(nil)

func (e *Engine) buildPipelineNode(ctx context.Context, sel logql.Selector, stages []logql.PipelineStage) (PipelineNode, error) {
	cond, err := extractQueryConditions(e.querierCaps, sel)
	if err != nil {
		return nil, errors.Wrap(err, "extract preconditions")
	}

	input, err := e.querier.Query(ctx, sel.Matchers)
	if err != nil {
		return nil, errors.Wrap(err, "create input node")
	}

	if p := cond.prefilter; (p == nil || p == NopProcessor) && len(stages) == 0 && !e.otelAdapter {
		// Empty processing pipeline, get data directly from storage.
		return input, nil
	}

	return &ProcessorNode{
		Input:             input,
		Prefilter:         cond.prefilter,
		Selector:          sel,
		Pipeline:          stages,
		EnableOTELAdapter: e.otelAdapter,
	}, nil
}

// Traverse implements [Node].
func (n *ProcessorNode) Traverse(cb NodeVisitor) error {
	if err := cb(n); err != nil {
		return err
	}
	return n.Input.Traverse(cb)
}

// EvalPipeline implements [PipelineNode].
func (n *ProcessorNode) EvalPipeline(ctx context.Context, params EvalParams) (_ EntryIterator, rerr error) {
	pipeline, err := BuildPipeline(n.Pipeline...)
	if err != nil {
		return nil, errors.Wrap(err, "build pipeline")
	}

	iter, err := n.Input.EvalPipeline(ctx, params)
	if err != nil {
		return nil, err
	}
	defer closeOnError(iter, &rerr)

	return &entryIterator{
		iter:        iter,
		prefilter:   n.Prefilter,
		pipeline:    pipeline,
		entries:     0,
		limit:       params.Limit,
		otelAdapter: n.EnableOTELAdapter,
	}, nil
}

type entryIterator struct {
	iter EntryIterator

	prefilter Processor
	pipeline  Processor

	entries int
	limit   int
	// TODO(tdakkota): what?
	otelAdapter bool
}

func (i *entryIterator) Next(e *Entry) bool {
	for {
		if !i.iter.Next(e) || (i.limit > 0 && i.entries >= i.limit) {
			return false
		}

		var (
			ts   = e.Timestamp
			keep bool
		)
		if i.otelAdapter {
			e.Line = LineFromEntry(*e)
		}

		e.Line, keep = i.prefilter.Process(ts, e.Line, e.Set)
		if !keep {
			continue
		}

		e.Line, keep = i.pipeline.Process(ts, e.Line, e.Set)
		if !keep {
			continue
		}

		i.entries++
		return true
	}
}

func (i *entryIterator) Err() error {
	return i.iter.Err()
}

func (i *entryIterator) Close() error {
	return i.iter.Close()
}
