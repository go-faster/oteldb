package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

// App contains application dependencies and services.
type App struct {
	services map[string]func(context.Context) error

	logQuerier  logQuerier
	logInserter logstorage.Inserter

	traceQuerier  tracestorage.Querier
	traceInserter tracestorage.Inserter

	lg      *zap.Logger
	metrics Metrics
}

func newApp(ctx context.Context, lg *zap.Logger, metrics Metrics) (_ *App, err error) {
	var (
		storageType = strings.ToLower(os.Getenv("OTELDB_STORAGE"))
		m           = NewMetricsOverride(metrics)
		app         = &App{
			services: map[string]func(context.Context) error{},
			lg:       lg,
			metrics:  metrics,
		}
	)

	switch storageType {
	case "ch":
		inserter, querier, err := setupCH(ctx, os.Getenv("CH_DSN"), lg, m)
		if err != nil {
			return nil, errors.Wrapf(err, "create storage %q", storageType)
		}
		app.traceInserter = inserter
		app.traceQuerier = querier
	case "yt", "":
		inserter, querier, err := setupYT(ctx, lg)
		if err != nil {
			return nil, errors.Wrapf(err, "create storage %q", storageType)
		}
		app.traceInserter = inserter
		app.traceQuerier = querier

		app.logInserter = inserter
		app.logQuerier = querier
	default:
		return nil, errors.Errorf("unknown storage %q", storageType)
	}

	if err := app.setupReceiver(); err != nil {
		return nil, errors.Wrap(err, "otelreceiver")
	}
	if err := app.trySetupTempo(); err != nil {
		return nil, errors.Wrap(err, "tempo")
	}
	if err := app.trySetupLoki(); err != nil {
		return nil, errors.Wrap(err, "loki")
	}

	return app, nil
}

func addOgen[
	R interface {
		OperationID() string
	},
	Server interface {
		FindPath(string, *url.URL) (R, bool)
		http.Handler
	},
](
	app *App,
	name string,
	s Server,
	defaultPort string,
) {
	lg := app.lg.Named(name)

	addr := os.Getenv(strings.ToUpper(name) + "_ADDR")
	if addr == "" {
		addr = defaultPort
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           instrumentHTTP(s, s.FindPath, lg, app.metrics),
		ReadHeaderTimeout: 15 * time.Second,
	}

	app.services[name] = func(ctx context.Context) error {
		lg := lg.With(zap.String("addr", addr))
		lg.Info("Starting HTTP server")

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
}

func (app *App) trySetupTempo() error {
	q := app.traceQuerier
	if q == nil {
		return nil
	}

	tempo := tempohandler.NewTempoAPI(q)

	s, err := tempoapi.NewServer(tempo,
		tempoapi.WithTracerProvider(app.metrics.TracerProvider()),
		tempoapi.WithMeterProvider(app.metrics.MeterProvider()),
	)
	if err != nil {
		return err
	}

	addOgen[tempoapi.Route](app, "tempo", s, ":3200")
	return nil
}

func (app *App) trySetupLoki() error {
	q := app.logQuerier
	if q == nil {
		return nil
	}

	engine := logqlengine.NewEngine(q, logqlengine.Options{
		TracerProvider: app.metrics.TracerProvider(),
		ParseOptions: logql.ParseOptions{
			AllowDots: true,
		},
	})
	loki := lokihandler.NewLokiAPI(q, engine)

	s, err := lokiapi.NewServer(loki,
		lokiapi.WithTracerProvider(app.metrics.TracerProvider()),
		lokiapi.WithMeterProvider(app.metrics.MeterProvider()),
	)
	if err != nil {
		return err
	}

	addOgen[lokiapi.Route](app, "loki", s, ":3100")
	return nil
}

func (app *App) setupReceiver() error {
	var consumers otelreceiver.Consumers
	if i := app.traceInserter; i != nil {
		consumers.Traces = tracestorage.NewConsumer(i)
	}
	if i := app.logInserter; i != nil {
		consumers.Logs = logstorage.NewConsumer(i)
	}

	recv, err := otelreceiver.NewReceiver(
		consumers,
		otelreceiver.ReceiverConfig{
			Logger:         app.lg.Named("receiver"),
			TracerProvider: app.metrics.TracerProvider(),
			MeterProvider:  app.metrics.MeterProvider(),
		},
	)
	if err != nil {
		return errors.Wrap(err, "create OTEL receiver")
	}

	app.services["otelreceiver"] = func(ctx context.Context) error {
		return recv.Run(ctx)
	}
	return nil
}

// Run runs application.
func (app *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, s := range app.services {
		s := s
		g.Go(func() error {
			return s(ctx)
		})
	}
	return g.Wait()
}

type logQuerier interface {
	logstorage.Querier
	logqlengine.Querier
}
