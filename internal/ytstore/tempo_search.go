package ytstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/go-logfmt/logfmt"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

func (h *TempoAPI) searchTags(ctx context.Context, params tempoapi.SearchParams) (map[TraceID]tempoapi.TraceSearchMetadata, error) {
	lg := zctx.From(ctx)

	var query strings.Builder
	fmt.Fprintf(&query, "* from [%s] where true", h.tables.spans)
	if s, ok := params.Start.Get(); ok {
		n := s.UnixNano()
		fmt.Fprintf(&query, " and start >= %d", n)
	}
	if s, ok := params.End.Get(); ok {
		n := s.UnixNano()
		fmt.Fprintf(&query, " and end <= %d", n)
	}
	if d, ok := params.MinDuration.Get(); ok {
		n := d.Nanoseconds()
		fmt.Fprintf(&query, " and (end-start) >= %d", n)
	}
	if d, ok := params.MaxDuration.Get(); ok {
		n := d.Nanoseconds()
		fmt.Fprintf(&query, " and (end-start) <= %d", n)
	}
	if tags, ok := params.Tags.Get(); ok {
		d := logfmt.NewDecoder(strings.NewReader(tags))
		for d.ScanRecord() {
			for d.ScanKeyval() {
				if string(d.Key()) == "name" {
					fmt.Fprintf(&query, " and name = %q", d.Value())
					continue
				}

				query.WriteString(" and (")
				for i, column := range []string{
					"attrs",
					"scope_attrs",
					"resource_attrs",
				} {
					if i != 0 {
						query.WriteString(" or ")
					}
					yp := append([]byte{'/'}, d.Key()...)
					yp = append(yp, "/1"...)
					fmt.Fprintf(&query, "try_get_string(%s, %q) = %q", column, yp, d.Value())
				}
				query.WriteByte(')')
			}
		}
		if err := d.Err(); err != nil {
			return nil, errors.Wrap(err, "parse tags")
		}
	}
	fmt.Fprintf(&query, " limit %d", params.Limit.Or(20))

	lg.Debug("Search traces",
		zap.Stringer("query", &query),
	)
	var c metadataCollector
	if err := h.querySpans(ctx, query.String(), c.AddSpan); err != nil {
		return nil, err
	}
	return c.Result(), nil
}

func (h *TempoAPI) queryParentSpans(ctx context.Context, metadatas map[TraceID]tempoapi.TraceSearchMetadata) error {
	traces := map[TraceID]struct{}{}

	for id, m := range metadatas {
		if m.StartTimeUnixNano.IsZero() {
			traces[id] = struct{}{}
		}
	}
	if len(traces) == 0 {
		return nil
	}

	var query strings.Builder
	fmt.Fprintf(&query, "* from [%s] where is_null(parent_span_id) and trace_id in (", h.tables.spans)
	n := 0
	for id := range traces {
		if n != 0 {
			query.WriteByte(',')
		}
		fmt.Fprintf(&query, "%q", id)
		n++
	}
	query.WriteByte(')')

	zctx.From(ctx).Debug("Query missing parent spans",
		zap.Stringer("query", &query),
		zap.Int("count", len(traces)),
	)

	return h.querySpans(ctx, query.String(), func(span Span) error {
		traceID := span.TraceID
		m := metadatas[traceID]
		span.FillTraceMetadata(&m)
		metadatas[traceID] = m
		return nil
	})
}
