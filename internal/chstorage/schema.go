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

	Points        string
	Histograms    string
	ExpHistograms string
	Summaries     string
	Labels        string

	Logs string
}

// Validate checks table names
func (t *Tables) Validate() error {
	return t.Each(func(name *string) error {
		if *name == "" {
			return errors.New("table name must be non-empty")
		}
		return nil
	})
}

// Each calls given callback for each table.
func (t *Tables) Each(cb func(name *string) error) error {
	for _, table := range []struct {
		field     *string
		fieldName string
	}{
		{&t.Spans, "Spans"},
		{&t.Tags, "Tags"},

		{&t.Points, "Points"},
		{&t.Histograms, "Histograms"},
		{&t.ExpHistograms, "ExpHistograms"},
		{&t.Summaries, "Summaries"},
		{&t.Labels, "Labels"},

		{&t.Logs, "Logs"},
	} {
		if err := cb(table.field); err != nil {
			return errors.Wrapf(err, "table %s", table.fieldName)
		}
	}
	return nil
}

// DefaultTables returns default tables.
func DefaultTables() Tables {
	return Tables{
		Spans: "traces_spans",
		Tags:  "traces_tags",

		Points:        "metrics_points",
		Histograms:    "metrics_histograms",
		ExpHistograms: "metrics_exp_histograms",
		Summaries:     "metrics_summaries",
		Labels:        "metrics_labels",

		Logs: "logs",
	}
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
		{t.Histograms, histogramsSchema},
		{t.ExpHistograms, expHistogramsSchema},
		{t.Summaries, summariesSchema},
		{t.Labels, labelsSchema},

		{t.Logs, logsSchema},
	} {
		if err := c.Do(ctx, ch.Query{
			Body: fmt.Sprintf(s.query, s.name),
		}); err != nil {
			return errors.Wrapf(err, "create %q", s.name)
		}
	}
	return nil
}
