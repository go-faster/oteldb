package main

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"golang.org/x/sync/errgroup"
)

func newApp(cfg Config, m *app.Telemetry) (*App, error) {
	cfg.setDefaults()
	a := &App{
		cfg:      cfg,
		metrics:  m,
		services: map[string]func(context.Context) error{},
	}
	a.setupHealthCheck()
	if err := a.setupAPI(); err != nil {
		return nil, errors.Wrap(err, "setup api")
	}
	return a, nil
}

type App struct {
	cfg      Config
	metrics  *app.Telemetry
	services map[string]func(context.Context) error
}

// Run runs application.
func (a *App) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, s := range a.services {
		s := s
		g.Go(func() error {
			return s(ctx)
		})
	}
	return g.Wait()
}
