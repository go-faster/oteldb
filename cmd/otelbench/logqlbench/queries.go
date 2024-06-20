package logqlbench

import (
	"context"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"

	"github.com/go-faster/oteldb/internal/lokihandler"
)

// ConfigQuery defines LogQL query parameters.
type ConfigQuery struct {
	Title       string        `yaml:"title,omitempty"`
	Description string        `yaml:"description,omitempty"`
	Start       string        `yaml:"start,omitempty"`
	End         string        `yaml:"end,omitempty"`
	Step        time.Duration `yaml:"step,omitempty"`
	Query       string        `yaml:"query,omitempty"`
	Match       []string      `yaml:"match,omitempty"`
}

// Query is a benchmarked query.
type Query struct {
	ID   int
	Type string

	Title       string
	Description string
	Start       time.Time
	End         time.Time
	Step        time.Duration
	Query       string
	Match       []string
}

// Input defines queries config.
type Input struct {
	Instant []ConfigQuery `yaml:"instant"`
	Range   []ConfigQuery `yaml:"range"`
	Series  []ConfigQuery `yaml:"series"`
}

func (p *LogQLBenchmark) each(ctx context.Context, fn func(ctx context.Context, q Query) error) error {
	data, err := os.ReadFile(p.Input)
	if err != nil {
		return errors.Wrap(err, "read input")
	}

	var input Input
	if err := yaml.Unmarshal(data, &input); err != nil {
		return errors.Wrap(err, "unmarshal input")
	}

	var id int
	mapQuery := func(typ string, cq ConfigQuery) (Query, error) {
		q := Query{
			Type:        typ,
			ID:          id,
			Title:       cq.Title,
			Description: cq.Description,
			Step:        cq.Step,
			Query:       cq.Query,
		}

		var err error
		q.Start, err = lokihandler.ParseTimestamp(cq.Start, p.start)
		if err != nil {
			return q, errors.Wrap(err, "parse start")
		}
		q.End, err = lokihandler.ParseTimestamp(cq.End, p.end)
		if err != nil {
			return q, errors.Wrap(err, "parse end")
		}

		id++
		q.ID = id
		return q, nil
	}

	for _, cq := range input.Instant {
		q, err := mapQuery("instant", cq)
		if err != nil {
			return err
		}
		if err := fn(ctx, q); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, cq := range input.Range {
		q, err := mapQuery("range", cq)
		if err != nil {
			return err
		}
		if err := fn(ctx, q); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, cq := range input.Series {
		q, err := mapQuery("series", cq)
		if err != nil {
			return err
		}
		if err := fn(ctx, q); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	return nil
}
