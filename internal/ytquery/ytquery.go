// Package ytquery is a YTSaurus query builder.
package ytquery

import (
	"context"
	"strconv"
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/constraints"

	"github.com/go-faster/errors"
)

// TableInfo represents a YTSaurus table.
type TableInfo struct {
	path ypath.Path
}

// Table creates new TableInfo.
func Table(path ypath.Path) TableInfo {
	return TableInfo{path: path}
}

// String returns table path.
func (t TableInfo) String() string {
	return t.path.String()
}

// SelectBuilder is a SELECT query builder.
type SelectBuilder struct {
	table TableInfo

	columns []string
	where   Predicate
	limit   int
}

// Select starts a SELECT query.
func (t TableInfo) Select(fields ...string) *SelectBuilder {
	if len(fields) == 0 {
		fields = []string{"*"}
	}
	return &SelectBuilder{
		table:   t,
		columns: fields,
	}
}

// Where adds a predicate to the list.
func (b *SelectBuilder) Where(pred Predicate) *SelectBuilder {
	b.where = pred
	return b
}

// Limit sets LIMIT.
func (b *SelectBuilder) Limit(n int) *SelectBuilder {
	b.limit = n
	return b
}

// Build returns query and placeholders.
func (b *SelectBuilder) Build() (query string, placeholders any) {
	var (
		sb strings.Builder
		p  placeholderSet
	)

	for i, c := range b.columns {
		if i != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(c)
	}
	sb.WriteString(" FROM ")

	sb.WriteByte('[')
	sb.WriteString(b.table.String())
	sb.WriteByte(']')

	if pred := b.where; pred != nil {
		sb.WriteString(" WHERE ")
		pred.writePred(&sb, &p)
	}

	if l := b.limit; l != 0 {
		sb.WriteString(" LIMIT ")
		writeInt(&sb, l)
	}
	return sb.String(), p
}

// Query executes query and scans all values to T, then calls given callback.
func Query[T any](ctx context.Context, yc yt.Client, b *SelectBuilder, cb func(T) error) error {
	query, placeholders := b.Build()

	r, err := yc.SelectRows(ctx, query, &yt.SelectRowsOptions{
		PlaceholderValues: placeholders,
	})
	if err != nil {
		return errors.Wrap(err, "select")
	}
	defer func() {
		_ = r.Close()
	}()

	for r.Next() {
		var span T
		if err := r.Scan(&span); err != nil {
			return errors.Wrap(err, "scan")
		}
		if err := cb(span); err != nil {
			return errors.Wrap(err, "callback")
		}
	}

	if err := r.Err(); err != nil {
		return errors.Wrap(err, "iter err")
	}
	return nil
}

func writeInt[I constraints.Integer](sb *strings.Builder, v I) {
	buf := make([]byte, 0, 32)
	buf = strconv.AppendInt(buf, int64(v), 10)
	sb.Write(buf)
}
