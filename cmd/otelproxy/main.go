// Command otelproxy is a Grafana datasource proxy.
package main

import (
	"context"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokiproxy"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promproxy"
	"github.com/go-faster/oteldb/internal/pyroproxy"
	"github.com/go-faster/oteldb/internal/pyroscopeapi"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempoproxy"
)

type service struct {
	addr    string
	name    string
	handler http.Handler
}

func (s service) Run(ctx context.Context, lg *zap.Logger) error {
	httpServer := &http.Server{
		Addr:              s.addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 15 * time.Second,
	}
	lg.Info("Starting HTTP server",
		zap.String("addr", s.addr),
	)
	parentCtx := ctx
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()

		lg.Info("Shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		return httpServer.Shutdown(ctx)
	})
	g.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) && parentCtx.Err() != nil {
				lg.Info("HTTP server closed gracefully")
				return nil
			}
			return errors.Wrap(err, "http server")
		}
		return nil
	})
	return g.Wait()
}

type services struct {
	ports map[string]service
}

func (s *services) addService(addr string, srv service) error {
	if s.ports == nil {
		s.ports = map[string]service{}
	}
	if existing, ok := s.ports[addr]; ok {
		return errors.Errorf("service conflict: %q and %q on %q", existing.name, srv.name, addr)
	}

	s.ports[addr] = srv
	return nil
}

func (s *services) Prometheus(m *app.Metrics) error {
	const (
		prefix      = "PROMETHEUS"
		defaultPort = ":9090"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := promapi.NewClient(upstreamURL,
		promapi.WithTracerProvider(m.TracerProvider()),
		promapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := promapi.NewServer(
		promproxy.NewServer(client),
		promapi.WithTracerProvider(m.TracerProvider()),
		promapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:    addr,
		name:    strings.ToLower(prefix),
		handler: server,
	})
}

func (s *services) Loki(m *app.Metrics) error {
	const (
		prefix      = "LOKI"
		defaultPort = ":3100"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := lokiapi.NewClient(upstreamURL,
		lokiapi.WithTracerProvider(m.TracerProvider()),
		lokiapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := lokiapi.NewServer(
		lokiproxy.NewServer(client),
		lokiapi.WithTracerProvider(m.TracerProvider()),
		lokiapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:    addr,
		name:    strings.ToLower(prefix),
		handler: server,
	})
}

var _ http.RoundTripper = (*pyroscopeTransport)(nil)

// pyroscopeTransport overrides Content-Type for some endpoints.
//
// See https://github.com/grafana/pyroscope/pull/1969.
type pyroscopeTransport struct {
	next http.RoundTripper
}

func (t *pyroscopeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if last := path.Base(req.URL.Path); last == "labels" || last == "label-values" {
		resp.Header.Set("Content-Type", "application/json")
	}
	return resp, nil
}

func (s *services) Pyroscope(m *app.Metrics) error {
	const (
		prefix      = "PYROSCOPE"
		defaultPort = ":4040"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := pyroscopeapi.NewClient(upstreamURL,
		pyroscopeapi.WithTracerProvider(m.TracerProvider()),
		pyroscopeapi.WithMeterProvider(m.MeterProvider()),
		pyroscopeapi.WithClient(&http.Client{
			Transport: &pyroscopeTransport{},
		}),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := pyroscopeapi.NewServer(
		pyroproxy.NewServer(client),
		pyroscopeapi.WithTracerProvider(m.TracerProvider()),
		pyroscopeapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:    addr,
		name:    strings.ToLower(prefix),
		handler: server,
	})
}

var _ http.RoundTripper = (*tempoTransport)(nil)

// tempoTransport sets Accept for some endpoints.
//
// FIXME(tdakkota): probably, we need to add an Accept header.
type tempoTransport struct {
	next http.RoundTripper
}

func (t *tempoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	if strings.Contains(req.URL.Path, "api/traces/") {
		if req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/protobuf")
		}
	}

	resp, err := next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *services) Tempo(m *app.Metrics) error {
	const (
		prefix      = "TEMPO"
		defaultPort = ":3200"
	)

	upstreamURL := os.Getenv(prefix + "_URL")
	if upstreamURL == "" {
		return nil
	}
	client, err := tempoapi.NewClient(upstreamURL,
		tempoapi.WithTracerProvider(m.TracerProvider()),
		tempoapi.WithMeterProvider(m.MeterProvider()),
		tempoapi.WithClient(&http.Client{
			Transport: &tempoTransport{},
		}),
	)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	server, err := tempoapi.NewServer(
		tempoproxy.NewServer(client),
		tempoapi.WithTracerProvider(m.TracerProvider()),
		tempoapi.WithMeterProvider(m.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "create server")
	}
	addr := os.Getenv(prefix + "_HTTP_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	return s.addService(addr, service{
		addr:    addr,
		name:    strings.ToLower(prefix),
		handler: server,
	})
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		s := services{}
		if err := s.Prometheus(m); err != nil {
			return errors.Wrapf(err, "setup Prometheus proxy")
		}
		if err := s.Loki(m); err != nil {
			return errors.Wrapf(err, "setup Loki proxy")
		}
		if err := s.Pyroscope(m); err != nil {
			return errors.Wrapf(err, "setup Pyroscope proxy")
		}
		if err := s.Tempo(m); err != nil {
			return errors.Wrapf(err, "setup Tempo proxy")
		}

		if len(s.ports) == 0 {
			return errors.New("no services to run")
		}
		g, ctx := errgroup.WithContext(ctx)
		for _, s := range s.ports {
			s := s
			lg := lg.Named(s.name)
			g.Go(func() error {
				return s.Run(ctx, lg)
			})
		}
		return g.Wait()
	})
}
