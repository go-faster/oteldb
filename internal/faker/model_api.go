package faker

import (
	"context"
	"net/netip"

	"go.opentelemetry.io/otel/trace"
)

type apiService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
	port   int
}

func (s apiService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "request")
	defer span.End()
	s.router.Backend(ctx)
}

func (s apiService) Name() string { return serviceAPI }
