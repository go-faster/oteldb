package faker

import (
	"context"
	"net/netip"

	"go.opentelemetry.io/otel/trace"
)

type frontendService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
}

func (s frontendService) Name() string { return serviceFrontend }

func (s frontendService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "Browse")
	defer span.End()
	s.router.API(ctx)
}
