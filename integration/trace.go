package integration

import (
	"context"
	"math/rand"
	"sync"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type randomIDGenerator struct {
	sync.Mutex
	rand *rand.Rand
}

// NewSpanID returns a non-zero span ID from a randomly-chosen sequence.
func (gen *randomIDGenerator) NewSpanID(context.Context, trace.TraceID) (sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(sid[:]) // #nosec G104
	return sid
}

// NewIDs returns a non-zero trace ID and a non-zero span ID from a
// randomly-chosen sequence.
func (gen *randomIDGenerator) NewIDs(context.Context) (tid trace.TraceID, sid trace.SpanID) {
	gen.Lock()
	defer gen.Unlock()
	gen.rand.Read(tid[:]) // #nosec G104
	gen.rand.Read(sid[:]) // #nosec G104
	return tid, sid
}

// Provider is a helper for tests providing a TracerProvider and an
// InMemoryExporter.
type Provider struct {
	*tracesdk.TracerProvider
	Exporter *tracetest.InMemoryExporter
}

// Reset clears the current in-memory storage.
func (p *Provider) Reset() {
	p.Exporter.Reset()
}

// Flush forces a flush of all finished spans.
func (p *Provider) Flush() {
	if err := p.TracerProvider.ForceFlush(context.Background()); err != nil {
		panic(err)
	}
}

// NewProvider initializes and returns a new Provider along with an exporter.
func NewProvider() *Provider {
	exporter := tracetest.NewInMemoryExporter()
	randSource := rand.NewSource(10)
	tp := tracesdk.NewTracerProvider(
		// Using deterministic random ids.
		tracesdk.WithIDGenerator(&randomIDGenerator{
			// #nosec G404
			rand: rand.New(randSource),
		}),
		tracesdk.WithBatcher(exporter,
			tracesdk.WithBatchTimeout(0), // instant
		),
	)
	return &Provider{
		Exporter:       exporter,
		TracerProvider: tp,
	}
}
