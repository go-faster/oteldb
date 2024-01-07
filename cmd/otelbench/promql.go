package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
)

type PromQL struct {
	Addr           string
	Input          string
	RequestTimeout time.Duration

	client *promapi.Client
}

func (p *PromQL) Setup() error {
	var err error
	p.client, err = promapi.NewClient(p.Addr)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	return nil
}

func toPrometheusTimestamp(t time.Time) promapi.PrometheusTimestamp {
	return promapi.PrometheusTimestamp(strconv.FormatInt(t.Unix(), 10))
}

func (p *PromQL) sendRangeQuery(ctx context.Context, q promproxy.RangeQuery) error {
	if _, err := p.client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query: q.Query,
		Step:  strconv.Itoa(q.Step),
		Start: toPrometheusTimestamp(q.Start),
		End:   toPrometheusTimestamp(q.End),
	}); err != nil {
		return errors.Wrap(err, "get query range")
	}
	return nil
}

func toOptPrometheusTimestamp(t promproxy.OptDateTime) promapi.OptPrometheusTimestamp {
	if !t.IsSet() {
		return promapi.OptPrometheusTimestamp{}
	}
	return promapi.NewOptPrometheusTimestamp(toPrometheusTimestamp(t.Value))
}

func (p *PromQL) sendInstantQuery(ctx context.Context, q promproxy.InstantQuery) error {
	if _, err := p.client.GetQuery(ctx, promapi.GetQueryParams{
		Query: q.Query,
		Time:  toOptPrometheusTimestamp(q.Time),
	}); err != nil {
		return errors.Wrap(err, "get query")
	}
	return nil
}

func (p *PromQL) sendSeriesQuery(ctx context.Context, query promproxy.SeriesQuery) error {
	if _, err := p.client.GetSeries(ctx, promapi.GetSeriesParams{
		Start: toOptPrometheusTimestamp(query.Start),
		End:   toOptPrometheusTimestamp(query.End),
		Match: query.Matchers,
	}); err != nil {
		return errors.Wrap(err, "get series")
	}
	return nil
}

func (p *PromQL) send(ctx context.Context, q promproxy.Query) error {
	ctx, cancel := context.WithTimeout(ctx, p.RequestTimeout)
	defer cancel()
	switch q.Type {
	case promproxy.InstantQueryQuery:
		return p.sendInstantQuery(ctx, q.InstantQuery)
	case promproxy.RangeQueryQuery:
		return p.sendRangeQuery(ctx, q.RangeQuery)
	case promproxy.SeriesQueryQuery:
		return p.sendSeriesQuery(ctx, q.SeriesQuery)
	default:
		return errors.Errorf("unknown query type %q", q.Type)
	}
}

func (p *PromQL) each(ctx context.Context, fn func(ctx context.Context, q promproxy.Query) error) error {
	f, err := os.Open(p.Input)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	defer func() {
		_ = f.Close()
	}()
	d := json.NewDecoder(f)
	for {
		var q promproxy.Query
		if err := d.Decode(&q); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.Wrap(err, "decode query")
		}
		if err := fn(ctx, q); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	return nil
}

func (p *PromQL) Run(ctx context.Context) error {
	fmt.Println("sending", p.Input, "to", p.Addr)
	var total int64
	if err := p.each(ctx, func(ctx context.Context, q promproxy.Query) error {
		total++
		return nil
	}); err != nil {
		return errors.Wrap(err, "count total")
	}
	pb := progressbar.Default(total)
	start := time.Now()
	if err := p.each(ctx, func(ctx context.Context, q promproxy.Query) error {
		if err := p.send(ctx, q); err != nil {
			return errors.Wrap(err, "send")
		}
		if err := pb.Add(1); err != nil {
			return errors.Wrap(err, "update progress bar")
		}
		return nil
	}); err != nil {
		_ = pb.Exit()
		return errors.Wrap(err, "send")
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}
	fmt.Println("done in", time.Since(start).Round(time.Millisecond))
	return nil
}

func newPromQLCommand() *cobra.Command {
	p := &PromQL{}
	cmd := &cobra.Command{
		Use:   "promql",
		Short: "Run promql queries",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := p.Setup(); err != nil {
				return errors.Wrap(err, "setup")
			}
			return p.Run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&p.Addr, "addr", "http://localhost:9090", "Prometheus address")
	f.StringVarP(&p.Input, "input", "i", "queries.jsonl", "Input file")
	f.DurationVar(&p.RequestTimeout, "request-timeout", time.Second*10, "Request timeout")
	return cmd
}
