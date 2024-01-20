package main

import (
	"context"
	"net/http"
	"time"
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
			_ = srv.Shutdown(context.Background())
		}()
		return srv.ListenAndServe()
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
