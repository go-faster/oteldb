// Package logqlbench defines utilities to benchmark LogQL queries.
package logqlbench

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/tempoapi"
)

type LogQLBenchmark struct {
	Addr   string
	Count  int
	Warmup int
	Trace  bool

	StartTime  string
	EndTime    string
	AllowEmpty bool

	TracesExporterAddr string
	TempoAddr          string

	Input          string
	Output         string
	RequestTimeout time.Duration

	batchSpanProcessor sdktrace.SpanProcessor
	tracerProvider     trace.TracerProvider
	tempo              *tempoapi.Client

	client *lokiapi.Client
	start  time.Time
	end    time.Time

	queries    []tracedQuery
	queriesMux sync.Mutex

	reports []LogQLReportQuery
	tracer  trace.Tracer
}

// Setup setups benchmark using given flags.
func (p *LogQLBenchmark) Setup(cmd *cobra.Command) error {
	ctx := cmd.Context()

	var err error
	if p.start, err = lokihandler.ParseTimestamp(p.StartTime, time.Time{}); err != nil {
		return errors.Wrap(err, "parse start time")
	}
	if p.end, err = lokihandler.ParseTimestamp(p.EndTime, time.Time{}); err != nil {
		return errors.Wrap(err, "parse end time")
	}

	if err := p.setupTracing(ctx); err != nil {
		return errors.Wrap(err, "setup tracing")
	}
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	httpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport,
			otelhttp.WithTracerProvider(p.tracerProvider),
			otelhttp.WithPropagators(propagator),
		),
	}
	p.client, err = lokiapi.NewClient(p.Addr,
		lokiapi.WithTracerProvider(p.tracerProvider),
		lokiapi.WithClient(httpClient),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	return nil
}

// Run starts the benchmark.
func (p *LogQLBenchmark) Run(ctx context.Context) error {
	fmt.Println("sending LogQL queries from", p.Input, "to", p.Addr)
	if !p.start.IsZero() {
		fmt.Println("start time override:", p.start.Format(time.RFC3339))
	}
	if !p.end.IsZero() {
		fmt.Println("end time override:", p.end.Format(time.RFC3339))
	}

	var total int
	if err := p.each(ctx, func(ctx context.Context, _ int, q Query) error {
		total += p.Count
		total += p.Warmup
		return nil
	}); err != nil {
		return errors.Wrap(err, "count total")
	}

	pb := progressbar.Default(int64(total))
	start := time.Now()
	if err := p.each(ctx, func(ctx context.Context, id int, q Query) (rerr error) {
		// Warmup.
		for i := 0; i < p.Warmup; i++ {
			if err := p.send(ctx, q); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		// Run.
		for i := 0; i < p.Count; i++ {
			if err := p.sendAndRecord(ctx, id, q); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
		}
		return nil
	}); err != nil {
		_ = pb.Exit()
		return errors.Wrap(err, "send queries")
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}
	fmt.Println("done in", time.Since(start).Round(time.Millisecond))

	if p.Trace {
		fmt.Println("waiting for traces")
		if err := p.flushTraces(ctx); err != nil {
			return errors.Wrap(err, "flush traces")
		}
	} else {
		fmt.Println("saving")
	}

	pb = progressbar.Default(int64(len(p.queries)))
	for _, v := range p.queries {
		if err := p.report(ctx, v); err != nil {
			return errors.Wrap(err, "wait for trace")
		}
		if err := pb.Add(1); err != nil {
			return errors.Wrap(err, "update progress bar")
		}
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}

	report := LogQLReport{
		Queries: p.reports,
	}
	slices.SortFunc(report.Queries, func(a, b LogQLReportQuery) int {
		if a.DurationNanos == b.DurationNanos {
			return strings.Compare(a.Query, b.Query)
		}
		if a.DurationNanos > b.DurationNanos {
			return -1
		}
		return 1
	})
	buf := new(bytes.Buffer)
	enc := yamlx.NewEncoder(buf)
	enc.SetIndent(2)
	if err := enc.Encode(report); err != nil {
		return errors.Wrap(err, "encode report")
	}
	reportData := buf.Bytes()

	if err := os.WriteFile(p.Output, reportData, 0o644); err != nil {
		return errors.Wrap(err, "write report")
	}
	fmt.Println("done")
	return nil
}
