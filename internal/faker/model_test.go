package faker

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type randomIDGenerator struct {
	sync.Mutex
	rand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *randomIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) (sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(sid[:])
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID from a
// randomly-chosen sequence.
func (gen *randomIDGenerator) NewIDs(_ context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(tid[:])
	gen.rand.Read(sid[:])
	return tid, sid
}

type traceProviderFactory struct {
	options   []tracesdk.TracerProviderOption
	providers []*tracesdk.TracerProvider
}

type meterProviderFactory struct {
	options   []metric.Option
	providers []*metric.MeterProvider
}

func (f *meterProviderFactory) New(options ...metric.Option) *metric.MeterProvider {
	opts := make([]metric.Option, 0, len(f.options)+len(options))
	opts = append(opts, f.options...)
	opts = append(opts, options...)
	provider := metric.NewMeterProvider(opts...)
	f.providers = append(f.providers, provider)
	return provider
}

func (f *traceProviderFactory) New(options ...tracesdk.TracerProviderOption) *tracesdk.TracerProvider {
	opts := make([]tracesdk.TracerProviderOption, 0, len(f.options)+len(options))
	opts = append(opts, f.options...)
	opts = append(opts, options...)
	provider := tracesdk.NewTracerProvider(opts...)
	f.providers = append(f.providers, provider)
	return provider
}

func TestModel(t *testing.T) {
	// Initialize test tracer.
	exporter := tracetest.NewInMemoryExporter()
	randSource := rand.NewSource(42)
	randInstance := rand.New(randSource)
	randGen := &randomIDGenerator{
		rand: randInstance,
	}
	batchProcessor := tracesdk.NewBatchSpanProcessor(exporter,
		tracesdk.WithBatchTimeout(0), // instant
	)
	tpFactory := &traceProviderFactory{
		options: []tracesdk.TracerProviderOption{
			// Using deterministic random ids.
			tracesdk.WithIDGenerator(randGen),
			tracesdk.WithSpanProcessor(batchProcessor),
		},
	}
	mpFactory := &meterProviderFactory{
		options: []metric.Option{
			// ...
		},
	}
	m := modelFromConfig(Config{
		Rand:                  randInstance,
		Nodes:                 10,
		RPS:                   1000,
		TracerProviderFactory: tpFactory,
		MeterProviderFactory:  mpFactory,
		Services: Services{
			API: API{
				Replicas: 2,
			},
			Backend: Backend{
				Replicas: 3,
			},
			DB: DB{
				Replicas: 3,
			},
			Cache: Cache{
				Replicas: 1,
			},
			Frontend: Frontend{
				Replicas: 3,
			},
		},
	})
	assert.Equal(t, 10, len(m.cluster.servers))
	assert.Equal(t, 1000, m.rps)
	assert.Equal(t, 3, len(m.frontends))

	var services int
	for _, s := range m.cluster.servers {
		services += len(s.services)
	}
	assert.Equal(t, 9, services)
	t.Run("Request", func(t *testing.T) {
		// Issue request.
		m.IssueRequest()
		t.Run("Spans", func(t *testing.T) {
			ctx := context.Background()
			for _, provider := range tpFactory.providers {
				require.NoError(t, provider.ForceFlush(ctx))
			}
			spans := exporter.GetSpans()
			require.NotEmpty(t, spans)
			require.Len(t, spans, 6)
		})
	})
}
