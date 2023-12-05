package chstorage

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
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

	Logs     string
	LogAttrs string

	Migration string
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
		{&t.LogAttrs, "LogAttrs"},

		{&t.Migration, "Migration"},
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

		Logs:     "logs",
		LogAttrs: "logs_attrs",

		Migration: "migration",
	}
}

type chClient interface {
	Do(ctx context.Context, q ch.Query) (err error)
}

func (t Tables) getHashes(ctx context.Context, c chClient) (map[string]string, error) {
	col := newMigrationColumns()
	if err := c.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT table, ddl FROM %s FINAL", t.Migration),
		Result: col.Result(),
	}); err != nil {
		return nil, errors.Wrap(err, "query")
	}
	return col.Mapping(), nil
}

func (t Tables) saveHashes(ctx context.Context, c chClient, m map[string]string) error {
	col := newMigrationColumns()
	col.Save(m)
	if err := c.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Input:  col.Input(),
		Body:   col.Input().Into(t.Migration),
	}); err != nil {
		return errors.Wrap(err, "query")
	}
	return nil
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
		{t.Migration, schemaMigration},
	} {
		if err := c.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   fmt.Sprintf(s.query, s.name),
		}); err != nil {
			return errors.Wrapf(err, "create %q", s.name)
		}
	}
	hashes, err := t.getHashes(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get hashes")
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
		{t.LogAttrs, logAttrsSchema},
	} {
		target := fmt.Sprintf("%x", sha256.Sum256([]byte(s.query)))
		if current, ok := hashes[s.name]; ok && current != target {
			// HACK: this will DROP all data in the table
			// TODO: implement ALTER TABLE
			zctx.From(ctx).Warn("DROPPING TABLE (schema changed!)",
				zap.String("table", s.name),
				zap.String("current", current),
				zap.String("target", target),
			)
			if err := c.Do(ctx, ch.Query{
				Logger: zctx.From(ctx).Named("ch"),
				Body:   fmt.Sprintf("DROP TABLE %s", s.name),
			}); err != nil {
				return errors.Wrapf(err, "drop %q", s.name)
			}
		}
		hashes[s.name] = target
		if err := c.Do(ctx, ch.Query{
			Logger: zctx.From(ctx).Named("ch"),
			Body:   fmt.Sprintf(s.query, s.name),
		}); err != nil {
			return errors.Wrapf(err, "create %q", s.name)
		}
	}
	if err := t.saveHashes(ctx, c, hashes); err != nil {
		return errors.Wrap(err, "save hashes")
	}

	return nil
}
