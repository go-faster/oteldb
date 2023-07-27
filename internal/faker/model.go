package faker

import (
	"context"
	"math/rand"
	"net/netip"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"go.opentelemetry.io/otel/trace"
)

type cluster struct {
	name    string
	servers []server
}

func (c *cluster) Attributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.K8SClusterName(c.name),
	}
}

func (c *cluster) getRandomServer(source *rand.Rand) int {
	return source.Intn(len(c.servers))
}

func (c *cluster) addServer(s server) {
	c.servers = append(c.servers, s)
}

type service interface {
	routerHandler
	Name() string
}

type server struct {
	name     string     // hostname
	ip       netip.Addr // address
	id       int        // unique id
	services []service
}

func (s *server) Attributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.NetHostName(s.name),
		attribute.Int("faker.server.id", s.id),
	}
}

func (s *server) addService(service service) {
	s.services = append(s.services, service)
}

type model struct {
	rps       int
	cluster   cluster
	frontends []frontendService
	router    Router
	rand      *rand.Rand
	tp        trace.TracerProvider
	tracer    trace.Tracer
}

// Request of the client.
type Request struct {
	TraceID  trace.TraceID
	ParentID trace.SpanID
}

func (m *model) IssueRequest() {
	ctx, span := m.tracer.Start(context.Background(), "request")
	defer span.End()
	m.router.API(ctx)
}

type apiService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
	port   int
}

func (s apiService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "request", trace.WithAttributes(s.Attributes()...))
	defer span.End()
	s.router.Backend(ctx)
}

func (s apiService) Attributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.ServiceName(s.Name()),
	}
}

func (s apiService) Name() string { return "api" }

type dbService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
	port   int
}

func (s dbService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "request")
	defer span.End()
	_ = ctx
}

func (s dbService) Name() string { return "db" }

type cacheService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
	port   int
}

func (s cacheService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "request")
	defer span.End()
	_ = ctx
}

func (s cacheService) Name() string { return "cache" }

type backendService struct {
	id     int
	ip     netip.Addr
	port   int
	router Router
	tracer trace.Tracer
}

func (s backendService) Handle(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "request")
	defer span.End()
	s.router.Cache(ctx)
	s.router.DB(ctx)
	s.router.Cache(ctx)
}

func (s backendService) Name() string { return "backend" }

type frontendService struct {
	router Router
	tracer trace.Tracer
	id     int
	ip     netip.Addr
}

func (s frontendService) Name() string { return "frontend" }

func (s frontendService) Handle(ctx context.Context) {
	s.router.API(ctx)
}

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
	// Pick random.
	routes[r.random.Intn(len(routes))].Handle(ctx)
}

func (r *clusterRouter) API(ctx context.Context) {
	r.handle(ctx, "api")
}

func (r *clusterRouter) Backend(ctx context.Context) {
	r.handle(ctx, "backend")
}

func (r *clusterRouter) Cache(ctx context.Context) {
	r.handle(ctx, "cache")
}

func (r *clusterRouter) DB(ctx context.Context) {
	r.handle(ctx, "db")
}

func (r *clusterRouter) Frontend(ctx context.Context) {
	r.handle(ctx, "frontend")
}

func modelFromConfig(c Config) model {
	router := &clusterRouter{
		routes: map[string][]routerHandler{},
		random: c.Rand,
	}
	m := model{
		rps:    c.RPS,
		rand:   c.Rand,
		router: router,
		tracer: c.TracerProvider.Tracer("faker"),
		tp:     c.TracerProvider,
	}
	m.cluster.name = "msk1"

	// Generate clients.
	residentialPool := newIPAllocator(netip.MustParseAddr("95.24.0.0"))
	for i := 0; i < c.Services.Frontend.Replicas; i++ {
		f := frontendService{
			router: router,
			id:     i,
			ip:     residentialPool.Next(),
			tracer: c.TracerProvider.Tracer("client"),
		}
		m.frontends = append(m.frontends, f)
		router.addRoute("frontend", f)
	}

	// Pool of external IP addresses.
	serverPool := newIPAllocator(netip.MustParseAddr("103.21.244.0"))
	for i := 0; i < c.Nodes; i++ {
		m.cluster.addServer(server{
			name: "node-" + strconv.Itoa(i),
			ip:   serverPool.Next(),
			id:   i,
		})
	}

	// Distribute HTTP API.
	for i := 0; i < c.Services.API.Replicas; i++ {
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		// Using note IP address as being exposed on node 80 port.
		s := apiService{
			router: router,
			tracer: c.TracerProvider.Tracer("api"),
			id:     i,
			ip:     m.cluster.servers[j].ip,
			port:   80,
		}
		m.cluster.servers[j].addService(s)
		router.addRoute("api", s)
	}

	// Distribute internal services.
	pool := newIPAllocator(netip.MustParseAddr("10.43.0.0"))

	// Distribute Backend.
	for i := 0; i < c.Services.Backend.Replicas; i++ {
		s := backendService{
			router: router,
			tracer: c.TracerProvider.Tracer("backend"),
			id:     i,
			ip:     pool.Next(),
			port:   8080,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
		router.addRoute("backend", s)
	}

	// Distribute DB.
	for i := 0; i < c.Services.DB.Replicas; i++ {
		s := dbService{
			router: router,
			tracer: c.TracerProvider.Tracer("db"),
			id:     i,
			ip:     pool.Next(),
			port:   5432,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
		router.addRoute("db", s)
	}

	// Distribute Cache.
	for i := 0; i < c.Services.Cache.Replicas; i++ {
		s := cacheService{
			router: router,
			tracer: c.TracerProvider.Tracer("cache"),
			id:     i,
			ip:     pool.Next(),
			port:   6379,
		}
		// Select random node.
		j := m.cluster.getRandomServer(m.rand)
		m.cluster.servers[j].addService(s)
		router.addRoute("cache", s)
	}

	return m
}
