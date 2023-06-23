package chstorage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
)

const (
	spansSchema = `CREATE TABLE IF NOT EXISTS %s
(
	trace_id UUID,
	span_id UInt64,
	trace_state String,
	parent_span_id UInt64,
	name LowCardinality(String),
	kind Enum8(
		'KIND_UNSPECIFIED' = 0,
		'KIND_INTERNAL' = 1,
		'KIND_SERVER' = 2,
		'KIND_CLIENT' = 3,
		'KIND_PRODUCER' = 4,
		'KIND_CONSUMER' = 5
	),
	start DateTime64(9),
	end DateTime64(9),
	attrs_str_keys        Array(LowCardinality(String)),
	attrs_str_values      Array(String),
	attrs_int_keys        Array(LowCardinality(String)),
	attrs_int_values      Array(Int64),
	attrs_float_keys      Array(LowCardinality(String)),
	attrs_float_values    Array(Float64),
	attrs_bool_keys       Array(LowCardinality(String)),
	attrs_bool_values     Array(Bool),
	attrs_bytes_keys      Array(LowCardinality(String)),
	attrs_bytes_values    Array(String),
	status_code Int32,
	status_message String,

	batch_id UUID,
	resource_attrs_str_keys    		Array(LowCardinality(String)),
	resource_attrs_str_values  		Array(String),
	resource_attrs_int_keys    		Array(LowCardinality(String)),
	resource_attrs_int_values  		Array(Int64),
	resource_attrs_float_keys  		Array(LowCardinality(String)),
	resource_attrs_float_values		Array(Float64),
	resource_attrs_bool_keys   		Array(LowCardinality(String)),
	resource_attrs_bool_values 		Array(Bool),
	resource_attrs_bytes_keys  		Array(LowCardinality(String)),
	resource_attrs_bytes_values		Array(String),

	scope_name String,
	scope_version String,
	scope_attrs_str_keys    		Array(LowCardinality(String)),
	scope_attrs_str_values  		Array(String),
	scope_attrs_int_keys    		Array(LowCardinality(String)),
	scope_attrs_int_values  		Array(Int64),
	scope_attrs_float_keys  		Array(LowCardinality(String)),
	scope_attrs_float_values		Array(Float64),
	scope_attrs_bool_keys   		Array(LowCardinality(String)),
	scope_attrs_bool_values 		Array(Bool),
	scope_attrs_bytes_keys  		Array(LowCardinality(String)),
	scope_attrs_bytes_values		Array(String),

	events_timestamps Array(DateTime64(9)),
	events_names Array(String),
	events_attrs_str_keys    		Array(Array(LowCardinality(String))),
	events_attrs_str_values  		Array(Array(String)),
	events_attrs_int_keys    		Array(Array(LowCardinality(String))),
	events_attrs_int_values  		Array(Array(Int64)),
	events_attrs_float_keys  		Array(Array(LowCardinality(String))),
	events_attrs_float_values		Array(Array(Float64)),
	events_attrs_bool_keys   		Array(Array(LowCardinality(String))),
	events_attrs_bool_values 		Array(Array(Bool)),
	events_attrs_bytes_keys  		Array(Array(LowCardinality(String))),
	events_attrs_bytes_values		Array(Array(String)),

	links_trace_ids Array(UUID),
	links_span_ids Array(UInt64),
	links_tracestates Array(String),
	links_attrs_str_keys    		Array(Array(LowCardinality(String))),
	links_attrs_str_values  		Array(Array(String)),
	links_attrs_int_keys    		Array(Array(LowCardinality(String))),
	links_attrs_int_values  		Array(Array(Int64)),
	links_attrs_float_keys  		Array(Array(LowCardinality(String))),
	links_attrs_float_values		Array(Array(Float64)),
	links_attrs_bool_keys   		Array(Array(LowCardinality(String))),
	links_attrs_bool_values 		Array(Array(Bool)),
	links_attrs_bytes_keys  		Array(Array(LowCardinality(String))),
	links_attrs_bytes_values		Array(Array(String)),
)
ENGINE = MergeTree()
PRIMARY KEY (trace_id, span_id);`
	kindDDL = `'KIND_UNSPECIFIED' = 0,'KIND_INTERNAL' = 1,'KIND_SERVER' = 2,'KIND_CLIENT' = 3,'KIND_PRODUCER' = 4,'KIND_CONSUMER' = 5`

	tagsSchema = `CREATE TABLE IF NOT EXISTS %s
(
	name LowCardinality(String),
	value String,
	value_type Enum8(
		'EMPTY' = 0,
		'STR' = 1,
		'INT' = 2,
		'DOUBLE' = 3,
		'BOOL' = 4,
		'MAP' = 5,
		'SLICE' = 6,
		'BYTES' = 7
	)
)
ENGINE = MergeTree()
PRIMARY KEY (name);`
	valueTypeDDL = `'EMPTY' = 0,'STR' = 1,'INT' = 2,'DOUBLE' = 3,'BOOL' = 4,'MAP' = 5,'SLICE' = 6,'BYTES' = 7`
)

// Tables define table names.
type Tables struct {
	Spans string
	Tags  string
}

type chClient interface {
	Do(ctx context.Context, q ch.Query) (err error)
}

// Create creates tables.
func (t Tables) Create(ctx context.Context, c chClient) error {
	type schema struct {
		name  string
		query string
	}
	for _, s := range []schema{
		{t.Spans, spansSchema},
		{t.Tags, tagsSchema},
	} {
		if err := c.Do(ctx, ch.Query{
			Body: fmt.Sprintf(s.query, s.name),
		}); err != nil {
			return errors.Wrapf(err, "create %q", s.name)
		}
	}
	return nil
}
