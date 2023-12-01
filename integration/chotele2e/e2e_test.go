package chotele2e

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/integration"
	"github.com/go-faster/oteldb/internal/chtrace"
)

type randomIDGenerator struct {
	sync.Mutex
	rand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *randomIDGenerator) NewSpanID(context.Context, trace.TraceID) (sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(sid[:])
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID from a
// randomly-chosen sequence.
func (gen *randomIDGenerator) NewIDs(context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(tid[:])
	gen.rand.Read(sid[:])
	return tid, sid
}

func discardResult() proto.Result {
	return (&proto.Results{}).Auto()
}

func ConnectOpt(t *testing.T, connOpt ch.Options) *ch.Client {
	t.Helper()
	integration.Skip(t)
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Name:         "oteldb-chotel-clickhouse",
		Image:        "clickhouse/clickhouse-server:23.10",
		ExposedPorts: []string{"8123/tcp", "9000/tcp"},
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainers.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := chContainer.PortEndpoint(ctx, "9000", "")
	require.NoError(t, err, "container endpoint")

	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute

	connOpt.Address = endpoint
	conn, err := backoff.RetryWithData(func() (*ch.Client, error) {
		c, err := ch.Dial(ctx, connOpt)
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func Connect(t *testing.T) *ch.Client {
	t.Helper()
	return ConnectOpt(t, ch.Options{
		Logger: zap.NewNop(),
	})
}

func TestIntegrationTrace(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	randSource := rand.NewSource(15)
	tp := tracesdk.NewTracerProvider(
		// Using deterministic random ids.
		tracesdk.WithIDGenerator(&randomIDGenerator{
			rand: rand.New(randSource),
		}),
		tracesdk.WithBatcher(exporter,
			tracesdk.WithBatchTimeout(0), // instant
		),
	)
	conn := ConnectOpt(t, ch.Options{
		Logger:                       zap.NewNop(),
		OpenTelemetryInstrumentation: true,
		TracerProvider:               tp,
		Settings: []ch.Setting{
			{
				Key:       "send_logs_level",
				Value:     "trace",
				Important: true,
			},
		},
	})

	// Should record trace and spans.
	var traceID trace.TraceID
	require.NoError(t, conn.Do(ctx, ch.Query{
		Body:   "SELECT 1",
		Result: discardResult(),
		OnLog: func(ctx context.Context, l ch.Log) error {
			sc := trace.SpanContextFromContext(ctx)
			traceID = sc.TraceID()
			t.Logf("[%s-%s]: %s", sc.TraceID(), sc.SpanID(), l.Text)
			return nil
		},
	}))

	require.True(t, traceID.IsValid(), "trace id not registered")
	t.Log("trace_id", traceID)

	// Force flushing.
	require.NoError(t, tp.ForceFlush(ctx))
	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	require.NoError(t, conn.Do(ctx, ch.Query{Body: "system flush logs"}))

	table := chtrace.NewTable()
	var traces []chtrace.Trace
	require.NoError(t, conn.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("SELECT %s FROM system.opentelemetry_span_log", strings.Join(table.Columns(), ", ")),
		Result: table.Result(),
		OnResult: func(ctx context.Context, block proto.Block) error {
			traces = append(traces, table.Rows()...)
			return nil
		},
	}))
	require.NotEmpty(t, traces)

	var gotTraces []chtrace.Trace
	var foundQuery bool
	for _, tt := range traces {
		if tt.TraceID != traceID {
			continue
		}
		t.Logf("%+v", tt)
		require.True(t, tt.SpanID != [8]byte{})
		require.True(t, tt.ParentSpanID != [8]byte{})
		require.False(t, tt.StartTime.IsZero())
		require.False(t, tt.FinishTime.IsZero())
		require.Less(t, time.Since(tt.FinishTime), time.Hour)
		require.NotEmpty(t, tt.OperationName)
		if tt.OperationName == "query" {
			require.Equal(t, tt.Attributes["db.statement"], "SELECT 1")
			foundQuery = true
		}
		gotTraces = append(gotTraces, tt)
	}
	require.NotEmpty(t, gotTraces, "no traces found by trace_id")
	require.True(t, foundQuery, "query span should be found")
}
