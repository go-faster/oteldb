package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
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
	"go.opentelemetry.io/otel/trace/noop"
	"sigs.k8s.io/yaml"

	"github.com/go-faster/oteldb/internal/logparser"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
	"github.com/go-faster/oteldb/internal/tempoapi"
)

type tracedQuery struct {
	TraceID  string
	Query    promproxy.Query
	Duration time.Duration
}

type PromQL struct {
	Addr   string
	Trace  bool
	Count  int
	Warmup int

	StartTime  string
	EndTime    string
	AllowEmpty bool

	TracesExporterAddr string
	TempoAddr          string

	Input          string
	Output         string
	RequestTimeout time.Duration

	client             *promapi.Client
	batchSpanProcessor sdktrace.SpanProcessor
	tracerProvider     trace.TracerProvider
	tempo              *tempoapi.Client
	start              time.Time
	end                time.Time

	queries []tracedQuery
	reports []PromQLReportQuery
	tracer  trace.Tracer
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, format := range []string{
		time.RFC3339,
		time.RFC3339Nano,
	} {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "parse int")
	}
	nanos, ok := logparser.DeductNanos(v)
	if !ok {
		return time.Time{}, errors.Errorf("invalid unix timestamp %d", v)
	}
	return time.Unix(0, nanos), nil
}

func (p *PromQL) setupTracing(ctx context.Context) error {
	if !p.Trace {
		p.tracerProvider = noop.NewTracerProvider()
		return nil
	}
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
	p.tracer = p.tracerProvider.Tracer("promql")
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
	var err error
	if p.start, err = parseTime(p.StartTime); err != nil {
		return errors.Wrap(err, "parse start time")
	}
	if p.end, err = parseTime(p.EndTime); err != nil {
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
	res, err := p.client.GetQueryRange(ctx, promapi.GetQueryRangeParams{
		Query: q.Query,
		Step:  strconv.Itoa(q.Step.Value),
		Start: toPrometheusTimestamp(q.Start.Value),
		End:   toPrometheusTimestamp(q.End.Value),
	})
	if err != nil {
		return errors.Wrap(err, "get query range")
	}
	if len(res.Data.Matrix.Result) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for range query %q", q.Query)
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
	res, err := p.client.GetQuery(ctx, promapi.GetQueryParams{
		Query: q.Query,
		Time:  toOptPrometheusTimestamp(q.Time),
	})
	if err != nil {
		return errors.Wrap(err, "get query")
	}
	if len(res.Data.Vector.Result) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for query %q", q.Query)
	}
	return nil
}

func (p *PromQL) sendSeriesQuery(ctx context.Context, query promproxy.SeriesQuery) error {
	res, err := p.client.GetSeries(ctx, promapi.GetSeriesParams{
		Start: toOptPrometheusTimestamp(query.Start),
		End:   toOptPrometheusTimestamp(query.End),
		Match: query.Matchers,
	})
	if err != nil {
		return errors.Wrap(err, "get series")
	}
	if len(res.Data) == 0 && !p.AllowEmpty {
		return errors.Errorf("no results for series query %q", query.Matchers)
	}
	return nil
}

func (p *PromQL) sendAndRecord(ctx context.Context, q promproxy.Query) (rerr error) {
	start := time.Now()
	tq := tracedQuery{
		Query: q,
	}
	if p.Trace {
		traceCtx, span := p.tracer.Start(ctx, "Send",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		tq.TraceID = span.SpanContext().TraceID().String()
		ctx = traceCtx
		defer func() {
			if rerr != nil {
				span.RecordError(rerr)
				span.SetStatus(codes.Error, rerr.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}()
	}
	if err := p.send(ctx, q); err != nil {
		return errors.Wrap(err, "send")
	}
	tq.Duration = time.Since(start)
	p.queries = append(p.queries, tq)
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

func (p *PromQL) eachFromReport(ctx context.Context, f *os.File, fn func(ctx context.Context, q promproxy.Query) error) error {
	data, err := io.ReadAll(f)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return errors.Wrap(err, "convert yaml to json")
	}
	var report promproxy.Record
	if err := report.UnmarshalJSON(jsonData); err != nil {
		return errors.Wrap(err, "decode report")
	}
	for _, q := range report.Range {
		if !q.Start.Set {
			q.Start = report.Start
		}
		if !q.End.Set {
			q.End = report.End
		}
		if !q.Step.Set {
			q.Step = report.Step
		}
		if !p.start.IsZero() {
			q.Start.Value = p.start
		}
		if !p.end.IsZero() {
			q.End.Value = p.end
		}
		if err := fn(ctx, promproxy.NewRangeQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, q := range report.Series {
		if !q.End.Set {
			q.End = report.End
		}
		if !q.Start.Set {
			q.Start = report.Start
		}
		if !p.start.IsZero() {
			q.Start.Value = p.start
		}
		if !p.end.IsZero() {
			q.End.Value = p.end
		}
		if err := fn(ctx, promproxy.NewSeriesQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	for _, q := range report.Instant {
		if err := fn(ctx, promproxy.NewInstantQueryQuery(q)); err != nil {
			return errors.Wrap(err, "callback")
		}
	}
	return nil
}

func (p *PromQL) each(ctx context.Context, fn func(ctx context.Context, q promproxy.Query) error) error {
	f, err := os.Open(p.Input)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	defer func() {
		_ = f.Close()
	}()
	if filepath.Ext(p.Input) == ".yaml" || filepath.Ext(p.Input) == ".yml" {
		return p.eachFromReport(ctx, f, fn)
	}
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

func (p *PromQL) report(ctx context.Context, q tracedQuery) error {
	// Produce query report.
	reportEntry := PromQLReportQuery{
		DurationNanos: q.Duration.Nanoseconds(),
	}
	switch q.Query.Type {
	case promproxy.InstantQueryQuery:
		reportEntry.Query = q.Query.InstantQuery.Query
		reportEntry.Title = q.Query.InstantQuery.Title.Value
		reportEntry.Description = q.Query.InstantQuery.Description.Value
	case promproxy.RangeQueryQuery:
		reportEntry.Query = q.Query.RangeQuery.Query
		reportEntry.Title = q.Query.RangeQuery.Title.Value
		reportEntry.Description = q.Query.RangeQuery.Description.Value
	case promproxy.SeriesQueryQuery:
		reportEntry.Matchers = q.Query.SeriesQuery.Matchers
		reportEntry.Title = q.Query.SeriesQuery.Title.Value
		reportEntry.Description = q.Query.SeriesQuery.Description.Value
	default:
		return errors.Errorf("unknown query type %q", q.Query.Type)
	}

	if !p.Trace {
		p.reports = append(p.reports, reportEntry)
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := p.batchSpanProcessor.ForceFlush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}
	bo := backoff.NewConstantBackOff(time.Millisecond * 100)
	res, err := backoff.RetryWithData(func() (v ptrace.Traces, err error) {
		res, err := p.tempo.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: q.TraceID})
		if err != nil {
			return v, backoff.Permanent(err)
		}
		switch r := res.(type) {
		case *tempoapi.TraceByIDNotFound:
			return v, errors.Errorf("trace %q not found", q.TraceID)
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
		return errors.Errorf("trace %q spans length is zero", q.TraceID)
	}

	// For each clickhouse query ID, save query.
	rsl := res.ResourceSpans()
	for i := 0; i < rsl.Len(); i++ {
		rs := rsl.At(i)
		spansSlices := rs.ScopeSpans()
		for j := 0; j < spansSlices.Len(); j++ {
			spans := spansSlices.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Name() != "query" {
					continue
				}
				var reportQuery ClickhouseQueryReport
				attrs := span.Attributes()
				statement, ok := attrs.Get("db.statement")
				if !ok {
					continue
				}
				reportQuery.Query = statement.AsString()
				if readBytes, ok := attrs.Get("clickhouse.read_bytes"); ok {
					reportQuery.ReadBytes = readBytes.Int()
				}
				if readRows, ok := attrs.Get("clickhouse.read_rows"); ok {
					reportQuery.ReadRows = readRows.Int()
				}
				if memoryUsage, ok := attrs.Get("clickhouse.memory_usage"); ok {
					reportQuery.MemoryUsage = memoryUsage.Int()
				}
				reportQuery.DurationNanos = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds()
				reportEntry.Queries = append(reportEntry.Queries, reportQuery)
			}
		}
	}

	p.reports = append(p.reports, reportEntry)

	return nil
}

type ClickhouseQueryReport struct {
	DurationNanos int64  `yaml:"duration_nanos,omitempty"`
	Query         string `yaml:"query,omitempty"`
	ReadBytes     int64  `yaml:"read_bytes,omitempty"`
	ReadRows      int64  `yaml:"read_rows,omitempty"`
	MemoryUsage   int64  `yaml:"memory_usage,omitempty"`
}

type PromQLReportQuery struct {
	Query         string                  `yaml:"query,omitempty"`
	Title         string                  `yaml:"title,omitempty"`
	Description   string                  `yaml:"description,omitempty"`
	DurationNanos int64                   `yaml:"duration_nanos,omitempty"`
	Matchers      []string                `yaml:"matchers,omitempty"`
	Queries       []ClickhouseQueryReport `yaml:"queries,omitempty"`
}

type PromQLReport struct {
	Queries []PromQLReportQuery `json:"queries"`
}

func (p *PromQL) Run(ctx context.Context) error {
	fmt.Println("sending promql queries from", p.Input, "to", p.Addr)
	if !p.start.IsZero() {
		fmt.Println("start time override:", p.start.Format(time.RFC3339))
	}
	if !p.end.IsZero() {
		fmt.Println("end time override:", p.end.Format(time.RFC3339))
	}
	var total int
	if err := p.each(ctx, func(ctx context.Context, q promproxy.Query) error {
		total += p.Count
		total += p.Warmup
		return nil
	}); err != nil {
		return errors.Wrap(err, "count total")
	}

	pb := progressbar.Default(int64(total))
	start := time.Now()
	if err := p.each(ctx, func(ctx context.Context, q promproxy.Query) (rerr error) {
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
			if err := p.sendAndRecord(ctx, q); err != nil {
				return errors.Wrap(err, "send")
			}
			if err := pb.Add(1); err != nil {
				return errors.Wrap(err, "update progress bar")
			}
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

	if p.Trace {
		fmt.Println("waiting for traces")
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

	report := PromQLReport{
		Queries: p.reports,
	}
	slices.SortFunc(report.Queries, func(a, b PromQLReportQuery) int {
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

	// #nosec G306
	if err := os.WriteFile(p.Output, reportData, 0644); err != nil {
		return errors.Wrap(err, "write report")
	}
	fmt.Println("done")
	return nil
}

func newPromQLBenchmarkCommand() *cobra.Command {
	p := &PromQL{}
	cmd := &cobra.Command{
		Use:     "bench",
		Aliases: []string{"benchmark"},
		Short:   "Run promql queries",
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
	f.StringVarP(&p.Output, "output", "o", "report.yml", "Output report file")
	f.DurationVar(&p.RequestTimeout, "request-timeout", time.Second*10, "Request timeout")
	f.StringVar(&p.TracesExporterAddr, "traces-exporter-addr", "http://127.0.0.1:4317", "Traces exporter OTLP endpoint")
	f.StringVar(&p.TempoAddr, "tempo-addr", "http://127.0.0.1:3200", "Tempo endpoint")
	f.StringVar(&p.StartTime, "start", "", "Start time override (RFC3339 or unix timestamp)")
	f.StringVar(&p.EndTime, "end", "", "End time override (RFC3339 or unix timestamp)")
	f.BoolVar(&p.AllowEmpty, "allow-empty", true, "Allow empty results")
	f.BoolVar(&p.Trace, "trace", false, "Trace queries")
	f.IntVar(&p.Count, "count", 1, "Number of times to run each query")
	f.IntVar(&p.Warmup, "warmup", 0, "Number of warmup runs")

	return cmd
}
