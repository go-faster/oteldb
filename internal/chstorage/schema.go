package chstorage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
)

// Tables define table names.
type Tables struct {
	Spans string
	Tags  string

	Points string
	Labels string
}

// Validate checks table names
func (t Tables) Validate() error {
	validateTableName := func(name string) error {
		if name == "" {
			return errors.New("table name must be non-empty")
		}
		return nil
	}

	for _, table := range []struct {
		name      string
		fieldName string
	}{
		{t.Spans, "Spans"},
		{t.Tags, "Tags"},

		{t.Points, "Points"},
		{t.Labels, "Labels"},
	} {
		if err := validateTableName(table.name); err != nil {
			return errors.Wrapf(err, "table %s", table.fieldName)
		}
	}
	return nil
}

var defaultTables = Tables{
	Spans: "traces_spans",
	Tags:  "traces_tags",

	Points: "metrics_points",
	Labels: "metrics_labels",
}

type chClient interface {
	Do(ctx context.Context, q ch.Query) (err error)
}

// Create creates tables.
func (t Tables) Create(ctx context.Context, c chClient) error {
	if err := t.Validate(); err != nil {
		return errors.Wrap(err, "validate")
	}

	type schema struct {
		name  string
		query string
	}
	for _, s := range []schema{
		{t.Spans, spansSchema},
		{t.Tags, tagsSchema},

		{t.Points, pointsSchema},
		{t.Labels, labelsSchema},
	} {
		if err := c.Do(ctx, ch.Query{
			Body: fmt.Sprintf(s.query, s.name),
		}); err != nil {
			return errors.Wrapf(err, "create %q", s.name)
		}
	}
	return nil
}
