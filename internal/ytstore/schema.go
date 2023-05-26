package ytstore

import (
	"context"

	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"

	"github.com/go-faster/errors"
)

// Span is a data structure for span.
type Span struct {
	TraceID       TraceID `yson:"trace_id"`
	SpanID        SpanID  `yson:"span_id"`
	TraceState    string  `yson:"trace_state"`
	ParentSpanID  SpanID  `yson:"parent_span_id"`
	Name          string  `yson:"name"`
	Kind          int32   `yson:"kind"`
	Start         uint64  `yson:"start"`
	End           uint64  `yson:"end"`
	Attrs         Attrs   `yson:"attrs"`
	StatusCode    int32   `yson:"status_code"`
	StatusMessage string  `yson:"status_message"`

	BatchID       string `yson:"batch_id"`
	ResourceAttrs Attrs  `yson:"resource_attrs"`

	ScopeName    string `yson:"scope_name"`
	ScopeVersion string `yson:"scope_version"`
	ScopeAttrs   Attrs  `yson:"scope_attrs"`
}

// Schema returns table schema for this structure.
func (Span) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			// FIXME(tdakkota): where is UUID?
			{Name: "trace_id", ComplexType: schema.TypeBytes, SortOrder: schema.SortAscending},
			{Name: "span_id", ComplexType: schema.TypeUint64, SortOrder: schema.SortAscending},
			{Name: "trace_state", ComplexType: schema.TypeString},
			{Name: "parent_span_id", ComplexType: schema.Optional{Item: schema.TypeUint64}},
			{Name: "name", ComplexType: schema.TypeString},
			{Name: "kind", ComplexType: schema.TypeInt32},
			// Start and end are nanoseconds, so we can't use Timestamp.
			{Name: "start", ComplexType: schema.TypeUint64},
			{Name: "end", ComplexType: schema.TypeUint64},
			{Name: "attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
			{Name: "status_code", ComplexType: schema.TypeInt32},
			{Name: "status_message", ComplexType: schema.TypeString},

			{Name: "batch_id", ComplexType: schema.TypeString},
			{Name: "resource_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},

			{Name: "scope_name", ComplexType: schema.TypeString},
			{Name: "scope_version", ComplexType: schema.TypeString},
			{Name: "scope_attrs", ComplexType: schema.Optional{Item: schema.TypeAny}},
		},
	}
}

// Tag is a data structure for tag.
type Tag struct {
	Name  string `yson:"name"`
	Value string `yson:"value"`
	Type  int32  `yson:"type"`
}

// Schema returns table schema for this structure.
func (Tag) Schema() schema.Schema {
	return schema.Schema{
		UniqueKeys: true,
		Columns: []schema.Column{
			{Name: "name", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "value", ComplexType: schema.TypeString, SortOrder: schema.SortAscending},
			{Name: "type", ComplexType: schema.TypeInt32},
		},
	}
}

type tables struct {
	spans ypath.Path
	tags  ypath.Path
}

func newTables(prefix ypath.Path) tables {
	return tables{
		spans: prefix.Child("spans"),
		tags:  prefix.Child("tags"),
	}
}

// Migrate setups YTSaurus tables for storage.
func (s *Store) Migrate(ctx context.Context) error {
	tables := map[ypath.Path]migrate.Table{
		s.tables.spans: {
			Schema: Span{}.Schema(),
		},
		s.tables.tags: {
			Schema: Tag{}.Schema(),
		},
	}
	if err := migrate.EnsureTables(ctx, s.yc, tables, migrate.OnConflictFail); err != nil {
		return errors.Wrap(err, "ensure tables")
	}
	return nil
}
