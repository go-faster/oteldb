package faker

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

type model struct {
	rps       int
	cluster   *cluster
	frontends []*frontendService
	router    Router
}

const (
	serviceFrontend = "frontend"
	serviceAPI      = "api"
	serviceBackend  = "backend"
	serviceDB       = "db"
	serviceCache    = "cache"
)

func (m *model) IssueRequest() {
	ctx := context.Background()
	m.router.Frontend(ctx)
}

func mergeToRes(s ...[]attribute.KeyValue) *resource.Resource {
	res := resource.Empty()
	for _, a := range s {
		v, err := resource.Merge(res, resource.NewSchemaless(a...))
		if err != nil {
			panic(err)
		}
		res = v
	}
	return res
}

func withResAttrs(s ...[]attribute.KeyValue) tracesdk.TracerProviderOption {
	return tracesdk.WithResource(mergeToRes(s...))
}

func instanceID(id int) attribute.KeyValue {
	return semconv.ServiceInstanceID(fmt.Sprintf("%d", id))
}

func modelFromConfig(c Config) model {
	rootAttrs := []attribute.KeyValue{
		attribute.Bool("oteldb.faker", true),

		semconv.TelemetrySDKName("opentelemetry"),
		semconv.TelemetrySDKLanguageGo,
		semconv.TelemetrySDKVersion(sdk.Version()),
	}
	tpf := c.TracerProviderFactory
	mpf := c.MeterProviderFactory
	router := &clusterRouter{
		routes: map[string][]routerHandler{},
		random: c.Rand,
	}
	m := model{
		rps:    c.RPS,
		router: router,
		cluster: &cluster{
			name: "alpha",
			rand: c.Rand,
		},
	}

	residentialPool := newIPAllocator(netip.MustParseAddr("95.24.0.0"))
	for i := 0; i < c.Services.Frontend.Replicas; i++ {
		ip := residentialPool.Next()
		id := i
		f := &frontendService{
			router: router,
			id:     id,
			ip:     ip,
			tracer: tpf.New(withResAttrs(rootAttrs,
				[]attribute.KeyValue{
					semconv.ServiceName(serviceFrontend),
					semconv.ServiceInstanceID(fmt.Sprintf("%d", id)),
					semconv.BrowserLanguage("en"),
					semconv.BrowserMobile(false),
					semconv.BrowserPlatform("Linux"),
					semconv.BrowserBrands("Firefox"),
					attribute.Int("front.id", id),
				},
			)).Tracer(serviceFrontend),
		}
		m.frontends = append(m.frontends, f)
		router.addRoute(serviceFrontend, f)
	}

	serverPool := newIPAllocator(netip.MustParseAddr("103.21.244.0"))
	for i := 0; i < c.Nodes; i++ {
		srv := &server{
			name: "node-" + strconv.Itoa(i),
			ip:   serverPool.Next(),
			id:   i,
			rnd:  c.Rand,
			cpus: 168,
		}
		res := mergeToRes(rootAttrs, srv.Attributes())
		srv.meter = mpf.New(metric.WithResource(res)).Meter("server")
		if err := srv.Metrics(); err != nil {
			panic(err)
		}
		m.cluster.addServer(srv)
	}

	for i := 0; i < c.Services.API.Replicas; i++ {
		srv := m.cluster.getRandomServer()
		attrs := []attribute.KeyValue{
			semconv.ServiceName(serviceAPI),
			instanceID(i),
		}
		s := &apiService{
			router: router,
			id:     i,
			ip:     srv.ip,
			port:   80,
			tracer: tpf.New(withResAttrs(rootAttrs, attrs)).Tracer(serviceAPI),
		}
		srv.addService(s)
		router.addRoute(serviceAPI, s)
	}

	pool := newIPAllocator(netip.MustParseAddr("10.43.0.0"))
	for i := 0; i < c.Services.Backend.Replicas; i++ {
		srv := m.cluster.getRandomServer()
		attrs := []attribute.KeyValue{
			semconv.ServiceName(serviceBackend),
			instanceID(i),
		}
		s := backendService{
			router: router,
			id:     i,
			ip:     pool.Next(),
			port:   8080,
			tracer: tpf.New(withResAttrs(rootAttrs, srv.Attributes(), attrs)).Tracer(serviceBackend),
		}
		srv.addService(s)
		router.addRoute(serviceBackend, s)
	}

	for i := 0; i < c.Services.DB.Replicas; i++ {
		svc := m.cluster.getRandomServer()
		attrs := []attribute.KeyValue{
			semconv.ServiceName("db"),
			instanceID(i),
		}
		s := dbService{
			router: router,
			id:     i,
			ip:     pool.Next(),
			port:   5432,
			tracer: tpf.New(withResAttrs(rootAttrs, svc.Attributes(), attrs)).Tracer("db"),
		}
		svc.addService(s)
		router.addRoute(serviceDB, s)
	}

	for i := 0; i < c.Services.Cache.Replicas; i++ {
		svc := m.cluster.getRandomServer()
		attrs := []attribute.KeyValue{
			semconv.ServiceName(serviceCache),
			instanceID(i),
		}
		s := &cacheService{
			router: router,
			tracer: tpf.New(withResAttrs(rootAttrs, svc.Attributes(), attrs)).Tracer(serviceCache),
			id:     i,
			ip:     pool.Next(),
			port:   6379,
		}
		svc.addService(s)
		router.addRoute(serviceCache, s)
	}

	return m
}
