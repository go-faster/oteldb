package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"

	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
	"github.com/go-faster/oteldb/internal/tempoapi"
)

type PromQL struct {
	Addr string

	TracesExporterAddr string
	TempoAddr          string

	Input          string
	RequestTimeout time.Duration

	client             *promapi.Client
	batchSpanProcessor sdktrace.SpanProcessor
	tracerProvider     *sdktrace.TracerProvider
	tempo              *tempoapi.Client

	traces []string
}

func (p *PromQL) setupTracing(ctx context.Context) error {
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return errors.Wrap(err, "create exporter")
	}
	p.batchSpanProcessor = sdktrace.NewBatchSpanProcessor(exporter)
	p.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", "otelbench.promql"),
		)),
		sdktrace.WithSpanProcessor(p.batchSpanProcessor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	httpClient := &http.Client{
		Transport: newTempoTransport(http.DefaultTransport),
	}
	tempoClient, err := tempoapi.NewClient(p.TempoAddr,
		tempoapi.WithClient(httpClient),
	)
	if err != nil {
		return errors.Wrap(err, "create tempo client")
	}
	p.tempo = tempoClient
	return nil
}

func (p *PromQL) Setup(ctx context.Context) error {
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
	var err error
	p.client, err = promapi.NewClient(p.Addr,
		promapi.WithTracerProvider(p.tracerProvider),
		promapi.WithClient(httpClient),
	)
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

// tempoTransport sets Accept for some endpoints.
//
// FIXME(tdakkota): probably, we need to add an Accept header.
type tempoTransport struct {
	next http.RoundTripper
}

func newTempoTransport(next http.RoundTripper) http.RoundTripper {
	return &tempoTransport{next: next}
}

func (t *tempoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	if strings.Contains(req.URL.Path, "api/traces/") {
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/protobuf")
		}
	}
	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (p *PromQL) waitForTrace(ctx context.Context, traceID string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	bo := backoff.NewConstantBackOff(time.Millisecond * 100)
	res, err := backoff.RetryWithData(func() (v ptrace.Traces, err error) {
		res, err := p.tempo.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: traceID})
		if err != nil {
			return v, backoff.Permanent(err)
		}
		switch r := res.(type) {
		case *tempoapi.TraceByIDNotFound:
			return v, errors.Errorf("trace %q not found", traceID)
		case *tempoapi.TraceByID:
			var um ptrace.ProtoUnmarshaler
			buf, err := io.ReadAll(r.Data)
			if err != nil {
				return v, backoff.Permanent(errors.Wrap(err, "read data"))
			}
			traces, err := um.UnmarshalTraces(buf)
			if err != nil {
				return v, backoff.Permanent(errors.Wrap(err, "unmarshal traces"))
			}
			services := make(map[string]int)
			list := traces.ResourceSpans()
			for i := 0; i < list.Len(); i++ {
				rs := list.At(i)
				attrValue, ok := rs.Resource().Attributes().Get("service.name")
				if !ok {
					return v, backoff.Permanent(errors.New("service name not found"))
				}
				services[attrValue.AsString()]++
			}
			for _, svc := range []string{
				"otelbench.promql",
				"go-faster.oteldb",
				"clickhouse",
			} {
				if _, ok := services[svc]; !ok {
					return v, errors.Errorf("service %q not found", svc)
				}
			}
			return traces, nil
		default:
			return v, backoff.Permanent(errors.Errorf("unknown response type %T", res))
		}
	}, backoff.WithContext(bo, ctx))
	if err != nil {
		return errors.Wrap(err, "retry")
	}
	if res.SpanCount() < 1 {
		return errors.Errorf("trace %q spans length is zero", traceID)
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
	tracer := p.tracerProvider.Tracer("promql")
	if err := p.each(ctx, func(ctx context.Context, q promproxy.Query) (rerr error) {
		ctx, span := tracer.Start(ctx, "Send",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		defer func() {
			if rerr != nil {
				span.RecordError(rerr)
				span.SetStatus(codes.Error, rerr.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
			if err := p.batchSpanProcessor.ForceFlush(ctx); err != nil {
				rerr = multierr.Append(rerr, errors.Wrap(err, "force flush"))
			}

			p.traces = append(p.traces, span.SpanContext().TraceID().String())
		}()
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
	fmt.Println("waiting for traces")

	pb = progressbar.Default(int64(len(p.traces)))
	for _, traceID := range p.traces {
		if err := p.waitForTrace(ctx, traceID); err != nil {
			return errors.Wrap(err, "wait for trace")
		}
		if err := pb.Add(1); err != nil {
			return errors.Wrap(err, "update progress bar")
		}
	}
	if err := pb.Finish(); err != nil {
		return errors.Wrap(err, "finish progress bar")
	}
	fmt.Println("done")
	return nil
}

func newPromQLCommand() *cobra.Command {
	p := &PromQL{}
	cmd := &cobra.Command{
		Use:   "promql",
		Short: "Run promql queries",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if err := p.Setup(ctx); err != nil {
				return errors.Wrap(err, "setup")
			}
			return p.Run(ctx)
		},
	}
	f := cmd.Flags()
	f.StringVar(&p.Addr, "addr", "http://localhost:9090", "Prometheus address")
	f.StringVarP(&p.Input, "input", "i", "queries.jsonl", "Input file")
	f.DurationVar(&p.RequestTimeout, "request-timeout", time.Second*10, "Request timeout")
	f.StringVar(&p.TracesExporterAddr, "traces-exporter-addr", "http://127.0.0.1:4317", "Traces exporter OTLP endpoint")
	f.StringVar(&p.TempoAddr, "tempo-addr", "http://127.0.0.1:3200", "Tempo endpoint")
	return cmd
}
