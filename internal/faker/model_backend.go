package faker

import (
	"context"
	"net/netip"

	"go.opentelemetry.io/otel/trace"
)

type backendService struct {
	id     int
	ip     netip.Addr
	port   int
	router Router
	tracer trace.Tracer
}

func (s backendService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "Handle")
	defer span.End()
	s.router.Cache(ctx)
	s.router.DB(ctx)
	s.router.Cache(ctx)
}

func (s backendService) Name() string { return serviceBackend }
