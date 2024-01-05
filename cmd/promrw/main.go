// Binary promrw implements prometheusremotewrite receiver that can record
// requests or send them to specified target.
package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func disableTelemetry() {
	for _, v := range []string{
		"OTEL_METRICS_EXPORTER",
		"OTEL_LOGS_EXPORTER",
		"OTEL_TRACES_EXPORTER",
	} {
		_ = os.Setenv(v, "none")
	}
}

func main() {
	disableTelemetry()
	var arg struct {
		Listen   bool
		Addr     string
		Data     string
		Duration time.Duration
	}
	flag.BoolVar(&arg.Listen, "listen", false, "Listen mode")
	flag.StringVar(&arg.Addr, "addr", ":8080", "Address")
	flag.StringVar(&arg.Data, "f", "rw.gob.zstd", "Data file")
	flag.DurationVar(&arg.Duration, "d", time.Minute, "Duration in seconds of recorded data")
	flag.Parse()

	if arg.Listen {
		app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) (rerr error) {
			f, err := os.Create(arg.Data)
			if err != nil {
				return errors.Wrap(err, "create file")
			}
			defer func() {
				if err := f.Close(); err != nil {
					rerr = multierr.Append(rerr, errors.Wrap(err, "close file"))
				} else {
					lg.Info("Saved", zap.String("file", arg.Data))
				}
			}()
			w, err := zstd.NewWriter(f)
			if err != nil {
				return errors.Wrap(err, "create encoder")
			}
			defer func() {
				if err := w.Close(); err != nil {
					rerr = multierr.Append(rerr, errors.Wrap(err, "close encoder"))
				}
			}()
			e := gob.NewEncoder(w)
			ctx, cancel := context.WithCancel(ctx)
			var start atomic.Pointer[time.Time]
			srv := &http.Server{
				Addr:              arg.Addr,
				ReadHeaderTimeout: time.Second,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					data, err := io.ReadAll(r.Body)
					if err != nil {
						lg.Error("read", zap.Error(err))
						return
					}
					now := time.Now()
					start.CompareAndSwap(nil, &now)
					duration := now.Sub(*start.Load())
					if duration > arg.Duration {
						cancel()
						w.WriteHeader(http.StatusAccepted)
						return
					}
					if err := e.Encode(data); err != nil {
						lg.Error("Write", zap.Error(err))
						w.WriteHeader(http.StatusInternalServerError)
						cancel()
						return
					}
					w.WriteHeader(http.StatusAccepted)
				}),
			}
			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				ticker := time.NewTicker(time.Second * 2)
				defer ticker.Stop()
				for {
					select {
					case now := <-ticker.C:
						if v := start.Load(); v != nil {
							duration := now.Sub(*v)
							fmt.Printf("d=%s of %s\n", duration.Round(time.Second), arg.Duration)
						} else {
							fmt.Println(`d="not started"`)
						}
					case <-ctx.Done():
						return nil
					}
				}
			})
			g.Go(func() error {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					return errors.Wrap(err, "listen and serve")
				}
				return nil
			})
			g.Go(func() error {
				<-ctx.Done()
				stopCtx := context.Background()
				return srv.Shutdown(stopCtx)
			})
			return g.Wait()
		})
	}
}
