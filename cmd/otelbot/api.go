package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelbench"
	"github.com/go-faster/oteldb/internal/otelbench/entdb"
	"github.com/go-faster/oteldb/internal/otelbotapi"
)

func (a *App) setupAPI() error {
	db, err := entdb.Open(os.Getenv("DATABASE_URL"))
	if err != nil {
		return errors.Wrap(err, "open database")
	}
	h := otelbench.NewHandler(db, a.metrics)
	apiServer, err := otelbotapi.NewServer(h, h,
		otelbotapi.WithTracerProvider(a.metrics.TracerProvider()),
		otelbotapi.WithMeterProvider(a.metrics.MeterProvider()),
	)
	if err != nil {
		return errors.Wrap(err, "setup api server")
	}
	srv := &http.Server{
		Addr:              a.cfg.API.Bind,
		Handler:           apiServer,
		ReadHeaderTimeout: time.Second,
	}
	a.services["api"] = func(ctx context.Context) error {
		go func() {
			<-ctx.Done()
			_ = srv.Shutdown(context.Background())
		}()
		return srv.ListenAndServe()
	}
	return nil
}
