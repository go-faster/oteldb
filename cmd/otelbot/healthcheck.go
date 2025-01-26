package main

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
)

func (a *App) setupHealthCheck() {
	mux := http.NewServeMux()
	mux.HandleFunc("/readiness", a.handleReadinessProbe)
	mux.HandleFunc("/liveness", a.handleLivenessProbe)
	mux.HandleFunc("/startup", a.handleStartupProbe)
	srv := &http.Server{
		Addr:              a.cfg.HealthCheck.Bind,
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
	}
	a.services["healthcheck"] = func(ctx context.Context) error {
		go func() {
			<-ctx.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		}()
		if err := srv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
				zctx.From(ctx).Info("Healthcheck HTTP server closed gracefully")
				return nil
			}
			return errors.Wrap(err, "healthcheck http server")
		}
		return nil
	}
}

func (a *App) handleReadinessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (a *App) handleLivenessProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (a *App) handleStartupProbe(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
