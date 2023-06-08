package ytstorage

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-faster/errors"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*YTQLQuerier)(nil)

// YTQLQuerier implements tracestorage.Querier based on YTSaurus QL.
type YTQLQuerier struct {
	yc     yt.TabletClient
	tables Tables
}

// NewYTQLQuerier creates new YTQLQuerier.
func NewYTQLQuerier(yc yt.TabletClient, tables Tables) *YTQLQuerier {
	return &YTQLQuerier{
		yc:     yc,
		tables: tables,
	}
}

// SearchTags performs search by given tags.
func (q *YTQLQuerier) SearchTags(ctx context.Context, tags map[string]string, opts tracestorage.SearchTagsOptions) (tracestorage.Iterator[tracestorage.Span], error) {
	var traceIDQuery strings.Builder

	fmt.Fprintf(&traceIDQuery, "trace_id FROM [%s] WHERE true", q.tables.spans)
	if s := opts.Start; s != 0 {
		fmt.Fprintf(&traceIDQuery, " AND start >= %d", s)
	}
	if e := opts.End; e != 0 {
		fmt.Fprintf(&traceIDQuery, " AND end <= %d", e)
	}
	if d := opts.MinDuration; d != 0 {
		fmt.Fprintf(&traceIDQuery, " AND (end-start) >= %d", d)
	}
	if d := opts.MaxDuration; d != 0 {
		fmt.Fprintf(&traceIDQuery, " AND (end-start) <= %d", d)
	}
	for key, value := range tags {
		if key == "name" {
			fmt.Fprintf(&traceIDQuery, " AND name = %q", value)
			continue
		}

		traceIDQuery.WriteString(" AND (")
		for i, column := range []string{
			"attrs",
			"scope_attrs",
			"resource_attrs",
		} {
			if i != 0 {
				traceIDQuery.WriteString(" OR ")
			}
			yp := append([]byte{'/'}, key...)
			yp = append(yp, "/1"...)
			fmt.Fprintf(&traceIDQuery, "try_get_string(%s, %q) = %q", column, yp, value)
		}
		traceIDQuery.WriteByte(')')
	}

	// Query trace IDs first.
	traces := map[tracestorage.TraceID]struct{}{}
	if err := queryRows(ctx, q.yc, traceIDQuery.String(), func(s tracestorage.Span) {
		traces[s.TraceID] = struct{}{}
	}); err != nil {
		return nil, errors.Wrap(err, "query traceIDs")
	}

	// Then, query all spans for each found trace ID.
	var query strings.Builder
	fmt.Fprintf(&query, "* FROM [%s] WHERE trace_id IN (", q.tables.spans)
	n := 0
	for id := range traces {
		if n != 0 {
			query.WriteByte(',')
		}
		fmt.Fprintf(&query, "%q", id)
		n++
	}
	query.WriteByte(')')

	r, err := q.yc.SelectRows(ctx, query.String(), nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Span]{reader: r}, nil
}

// TagNames returns all available tag names.
func (q *YTQLQuerier) TagNames(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf("name FROM [%s]", q.tables.tags)
	names := map[string]struct{}{}
	err := queryRows(ctx, q.yc, query, func(tag tracestorage.Tag) {
		names[tag.Name] = struct{}{}
	})
	return maps.Keys(names), err
}

// TagValues returns all available tag values for given tag.
//
// If there is no such tag, returns ErrNotFound.
func (q *YTQLQuerier) TagValues(ctx context.Context, tagName string) (tracestorage.Iterator[tracestorage.Tag], error) {
	query := fmt.Sprintf("* FROM [%s] WHERE name = %q", q.tables.tags, tagName)
	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Tag]{reader: r}, nil
}

// TraceByID returns spans of given trace.
//
// If there is no such trace, returns ErrNotFound.
func (q *YTQLQuerier) TraceByID(ctx context.Context, id tracestorage.TraceID, opts tracestorage.TraceByIDOptions) (tracestorage.Iterator[tracestorage.Span], error) {
	query := fmt.Sprintf("* FROM [%s] WHERE trace_id = %q", q.tables.spans, id[:])

	if s := opts.Start; s != 0 {
		query += fmt.Sprintf(" AND start >= %d", s)
	}
	if e := opts.End; e != 0 {
		query += fmt.Sprintf(" AND end <= %d", e)
	}

	r, err := q.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return &ytIterator[tracestorage.Span]{reader: r}, nil
}

var _ tracestorage.Iterator[any] = (*ytIterator[any])(nil)

type ytIterator[T any] struct {
	reader yt.TableReader
	err    error
}

// Next returns true, if there is element and fills t.
func (i *ytIterator[T]) Next(t *T) bool {
	if i.err != nil {
		return false
	}

	ok := i.reader.Next()
	if !ok {
		return false
	}

	i.err = i.reader.Scan(t)
	return i.err == nil
}

// Err returns an error caused during iteration, if any.
func (i *ytIterator[T]) Err() error {
	if e := i.err; e != nil {
		return e
	}
	return i.reader.Err()
}

// Close closes iterator.
func (i *ytIterator[T]) Close() error {
	return i.reader.Close()
}

func queryRows[T any](ctx context.Context, yc yt.TabletClient, q string, cb func(T)) error {
	r, err := yc.SelectRows(ctx, q, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = r.Close()
	}()
	for r.Next() {
		var val T
		if err := r.Scan(&val); err != nil {
			return err
		}
		cb(val)
	}
	return r.Err()
}
