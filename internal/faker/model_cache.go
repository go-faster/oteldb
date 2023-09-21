package faker

import (
	"context"
	"net/netip"

	"go.opentelemetry.io/otel/trace"
)

type cacheService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
	port   int
}

func (s cacheService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "LoadData")
	defer span.End()
	_ = ctx
}

func (s cacheService) Name() string { return serviceCache }
