package logqlbench

import (
	"context"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/go-faster/oteldb/internal/tempoapi"
)

type tracedQuery struct {
	ID       int
	TraceID  string
	Query    Query
	Duration time.Duration
}

func (p *LogQLBenchmark) setupTracing(ctx context.Context) error {
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
			attribute.String("service.name", "otelbench.logql"),
		)),
		sdktrace.WithSpanProcessor(p.batchSpanProcessor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	p.tracer = p.tracerProvider.Tracer("logql")
	tempoClient, err := tempoapi.NewClient(p.TempoAddr)
	if err != nil {
		return errors.Wrap(err, "create tempo client")
	}
	p.tempo = tempoClient
	return nil
}

func (p *LogQLBenchmark) flushTraces(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	if err := p.batchSpanProcessor.ForceFlush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (p *LogQLBenchmark) report(ctx context.Context, q tracedQuery) error {
	// Produce query report.
	reportEntry := LogQLReportQuery{
		ID:            q.ID,
		DurationNanos: q.Duration.Nanoseconds(),
		Query:         q.Query.Query,
		Title:         q.Query.Title,
		Description:   q.Query.Description,
		Matchers:      q.Query.Match,
	}

	if !p.Trace {
		p.reports = append(p.reports, reportEntry)
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	bo := backoff.NewConstantBackOff(time.Millisecond * 100)
	res, err := backoff.RetryWithData(func() (v ptrace.Traces, err error) {
		res, err := p.tempo.TraceByID(ctx, tempoapi.TraceByIDParams{
			TraceID: q.TraceID,
			Accept:  "application/protobuf",
		})
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
				"otelbench.logql",
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
	type queryReport struct {
		// "query" span coming from Clickhouse
		clickhouseSpan ptrace.Span
		// "Do" span coming from ch-go
		chgoSpan ptrace.Span
	}
	var (
		rsl     = res.ResourceSpans()
		queries = map[string]queryReport{}
	)
	for i := 0; i < rsl.Len(); i++ {
		rs := rsl.At(i)
		spansSlices := rs.ScopeSpans()
		for j := 0; j < spansSlices.Len(); j++ {
			spans := spansSlices.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				attrs := span.Attributes()
				if _, ok := attrs.Get("db.statement"); !ok {
					continue
				}

				switch span.Name() {
				case "query":
					queryIDVal, ok := attrs.Get("clickhouse.query_id")
					if !ok {
						continue
					}
					queryID := queryIDVal.AsString()

					report := queries[queryID]
					report.clickhouseSpan = span
					queries[queryID] = report
				case "Do":
					queryIDVal, ok := attrs.Get("ch.query.id")
					if !ok {
						continue
					}
					queryID := queryIDVal.AsString()

					report := queries[queryID]
					report.chgoSpan = span
					queries[queryID] = report
				default:
					continue
				}
			}
		}
	}

	for _, r := range queries {
		var reportQuery ClickhouseQueryReport

		if span := r.clickhouseSpan; span != (ptrace.Span{}) {
			attrs := span.Attributes()

			if statement, ok := attrs.Get("db.statement"); ok {
				reportQuery.Query = statement.AsString()
			}
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
		} else {
			continue
		}

		if span := r.chgoSpan; span != (ptrace.Span{}) {
			attrs := span.Attributes()
			if receivedBytes, ok := attrs.Get("ch.bytes"); ok {
				reportQuery.ReceivedBytes = receivedBytes.Int()
			}
			if receivedRows, ok := attrs.Get("ch.rows_received"); ok {
				reportQuery.ReceivedRows = receivedRows.Int()
			}
		}

		reportEntry.Queries = append(reportEntry.Queries, reportQuery)
	}

	p.reports = append(p.reports, reportEntry)

	return nil
}
