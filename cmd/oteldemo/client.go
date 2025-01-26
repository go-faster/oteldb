package main

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
)

func client(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
	httpTransport := otelhttp.NewTransport(http.DefaultTransport,
		otelhttp.WithTracerProvider(m.TracerProvider()),
		otelhttp.WithMeterProvider(m.MeterProvider()),
	)
	meter := m.MeterProvider().Meter("oteldemo.client")
	sentRequestsCount, err := meter.Int64Counter("oteldemo.client.sent_requests")
	if err != nil {
		return errors.Wrap(err, "create counter")
	}
	httpClient := &http.Client{
		Transport: httpTransport,
		Timeout:   time.Second * 10,
	}
	tracer := m.TracerProvider().Tracer("client")
	sendRequest := func(ctx context.Context) {
		sentRequestsCount.Add(ctx, 1)
		ctx, cancel := context.WithTimeout(ctx, time.Second*2)
		defer cancel()

		ctx, span := tracer.Start(ctx, "sendRequest")
		defer span.End()

		time.Sleep(time.Millisecond * 40)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://server:8080/api/hello", http.NoBody)
		if err != nil {
			lg.Error("create request", zap.Error(err))
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			lg.Error("send request", zap.Error(err))
			return
		}
		_ = resp.Body.Close()

		zctx.From(ctx).Info("got response",
			zap.Int("status", resp.StatusCode),
			zap.String("url", req.URL.String()),
		)
		time.Sleep(time.Millisecond * 40)
	}
	sendRequest(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sendRequest(ctx)
		}
	}
}
