package faker

import (
	"context"
	"math/rand"
)

// Router routes request to services.
type Router interface {
	Frontend(ctx context.Context)
	API(ctx context.Context)
	Backend(ctx context.Context)
	Cache(ctx context.Context)
	DB(ctx context.Context)
}

type routerHandler interface {
	Handle(ctx context.Context)
}

type clusterRouter struct {
	random *rand.Rand
	routes map[string][]routerHandler
}

func (r *clusterRouter) addRoute(name string, handler routerHandler) {
	r.routes[name] = append(r.routes[name], handler)
}

func (r *clusterRouter) handle(ctx context.Context, name string) {
	routes := r.routes[name]
	routes[r.random.Intn(len(routes))].Handle(ctx)
}

func (r *clusterRouter) API(ctx context.Context) {
	r.handle(ctx, serviceAPI)
}

func (r *clusterRouter) Backend(ctx context.Context) {
	r.handle(ctx, serviceBackend)
}

func (r *clusterRouter) Cache(ctx context.Context) {
	r.handle(ctx, serviceCache)
}

func (r *clusterRouter) DB(ctx context.Context) {
	r.handle(ctx, serviceDB)
}

func (r *clusterRouter) Frontend(ctx context.Context) {
	r.handle(ctx, serviceFrontend)
}
