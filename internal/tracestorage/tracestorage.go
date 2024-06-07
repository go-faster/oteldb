// Package tracestorage defines storage structure for trace storage.
package tracestorage

import (
	"context"
	"time"

	"github.com/go-faster/oteldb/internal/iterators"
	"github.com/go-faster/oteldb/internal/otelstorage"
	"github.com/go-faster/oteldb/internal/traceql"
)

// Querier is a trace storage query interface.
type Querier interface {
	// SearchTags performs search by given tags.
	SearchTags(ctx context.Context, tags map[string]string, opts SearchTagsOptions) (iterators.Iterator[Span], error)

	// TagNames returns all available tag names.
	TagNames(ctx context.Context, opts TagNamesOptions) ([]TagName, error)
	// TagValues returns all available tag values for given tag.
	TagValues(ctx context.Context, attr traceql.Attribute, opts TagValuesOptions) (iterators.Iterator[Tag], error)

	// TraceByID returns spans of given trace.
	TraceByID(ctx context.Context, id otelstorage.TraceID, opts TraceByIDOptions) (iterators.Iterator[Span], error)
}

// SearchTagsOptions defines options for [Querier.SearchTags].
type SearchTagsOptions struct {
	MinDuration time.Duration
	MaxDuration time.Duration

	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
}

// TagNamesOptions defines options for [Querier.TagNames].
type TagNamesOptions struct {
	// Scope defines attribute scope to lookup.
	//
	// Querier should return attributes from all scopes, if it is zero.
	Scope traceql.AttributeScope
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
}

// TagValuesOptions defines options for [Querier.TagValues].
type TagValuesOptions struct {
	// AutocompleteQuery is a set of spanset matchers to only return tags seen
	// on matching spansets.
	AutocompleteQuery traceql.Autocomplete
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
}

// TraceByIDOptions defines options for [Querier.TraceByID] method.
type TraceByIDOptions struct {
	// Start defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	Start time.Time
	// End defines time range for search.
	//
	// Querier ignores parameter, if it is zero.
	End time.Time
}

// TagNames is a set of tags by scope.
type TagName struct {
	Scope traceql.AttributeScope
	Name  string
}

// Inserter is a trace storage insert interface.
type Inserter interface {
	// InsertSpans inserts given spans.
	//
	// FIXME(tdakkota): probably, it's better to return some kind of batch writer.
	InsertSpans(ctx context.Context, spans []Span) error
	// InsertTags insert given set of tags to the storage.
	//
	// FIXME(tdakkota): probably, storage should do tag extraction by itself.
	InsertTags(ctx context.Context, tags map[Tag]struct{}) error
}
