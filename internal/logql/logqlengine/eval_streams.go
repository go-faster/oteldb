package logqlengine

import (
	"context"

	"github.com/go-faster/errors"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type entry struct {
	ts   otelstorage.Timestamp
	line string
	set  LabelSet
}

type entryIterator struct {
	iter iterators.Iterator[logstorage.Record]

	prefilter Processor
	pipeline  Processor

	entries int
	limit   int
}

func (i *entryIterator) Next(e *entry) bool {
	var record logstorage.Record

	for {
		if !i.iter.Next(&record) || i.entries >= i.limit {
			return false
		}

		ts := record.Timestamp
		e.set.SetFromRecord(record)

		line, keep := i.prefilter.Process(ts, record.Body, e.set)
		if !keep {
			continue
		}

		line, keep = i.pipeline.Process(ts, line, e.set)
		if !keep {
			continue
		}

		e.ts = ts
		e.line = line

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

func (e *Engine) selectLogs(ctx context.Context, sel logql.Selector, stages []logql.PipelineStage, params EvalParams) (*entryIterator, error) {
	// Instant query, sub lookback duration from Start.
	if params.IsInstant() {
		params.Start = addDuration(params.Start, e.lookbackDuration)
	}

	cond, err := extractQueryConditions(e.querierCaps, sel)
	if err != nil {
		return nil, errors.Wrap(err, "extract preconditions")
	}

	pipeline, err := BuildPipeline(stages...)
	if err != nil {
		return nil, errors.Wrap(err, "build pipeline")
	}

	iter, err := e.querier.SelectLogs(ctx,
		params.Start,
		params.End,
		cond.params,
	)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	return &entryIterator{
		iter:      iter,
		prefilter: cond.prefilter,
		pipeline:  pipeline,
		entries:   0,
		limit:     params.Limit,
	}, nil
}

func (e *Engine) evalLogExpr(ctx context.Context, expr *logql.LogExpr, params EvalParams) (s lokiapi.Streams, _ error) {
	iter, err := e.selectLogs(ctx, expr.Sel, expr.Pipeline, params)
	if err != nil {
		return nil, errors.Wrap(err, "select logs")
	}
	defer func() {
		_ = iter.Close()
	}()
	return groupEntries(iter)
}

func groupEntries(iter *entryIterator) (s lokiapi.Streams, _ error) {
	var (
		e       entry
		streams = map[string]lokiapi.Stream{}
	)
	for iter.Next(&e) {
		// FIXME(tdakkota): allocates a string for every record.
		key := e.set.String()
		stream, ok := streams[key]
		if !ok {
			stream = lokiapi.Stream{
				Stream: lokiapi.NewOptLabelSet(e.set.AsLokiAPI()),
			}
		}
		stream.Values = append(stream.Values, lokiapi.LogEntry{T: uint64(e.ts), V: e.line})
		streams[key] = stream
	}
	if err := iter.Err(); err != nil {
		return s, err
	}
	return maps.Values(streams), nil
}
