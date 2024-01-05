// Binary promrw implements prometheusremotewrite receiver that can record
// requests or send them to specified target.
package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cheggaaa/pb/v3"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/prompb"
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

func decode(r *http.Request) ([]byte, error) {
	if t := r.Header.Get("Content-Type"); t != "application/x-protobuf" {
		return nil, errors.Errorf("unsupported content type %q", t)
	}
	switch e := r.Header.Get("Content-Encoding"); e {
	case "zstd":
		dec, err := zstd.NewReader(r.Body)
		if err != nil {
			return nil, errors.Wrap(err, "create decoder")
		}
		defer dec.Close()
		return io.ReadAll(dec)
	case "snappy":
		rd := snappy.NewReader(r.Body)
		return io.ReadAll(rd)
	default:
		return nil, errors.Errorf("unsupported encoding %q", e)
	}
}

func main() {
	disableTelemetry()
	var arg struct {
		Listen   bool
		Addr     string
		Data     string
		Duration time.Duration
		Workers  int
	}
	flag.BoolVar(&arg.Listen, "listen", false, "Listen mode")
	flag.StringVar(&arg.Addr, "addr", ":8080", "Address")
	flag.StringVar(&arg.Data, "f", "rw.gob.zstd", "Data file")
	flag.IntVar(&arg.Workers, "j", 8, "Workers")
	flag.DurationVar(&arg.Duration, "d", time.Minute, "Duration in seconds of recorded data")
	flag.Parse()

	if arg.Listen {
		// Write gob-encoded series of byte slices to zstd-compressed file.
		// Byte slice is protobuf-encoded prompb.WriteRequest.
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
					if r.Method == http.MethodGet {
						w.WriteHeader(http.StatusOK)
						return
					}
					data, err := decode(r)
					if err != nil {
						lg.Error("Read", zap.Error(err))
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					now := time.Now()
					start.CompareAndSwap(nil, &now)
					duration := now.Sub(*start.Load())
					if duration > arg.Duration {
						w.WriteHeader(http.StatusAccepted)
						cancel()
						return
					}
					// Check if we have a valid request.
					var writeRequest prompb.WriteRequest
					if err := writeRequest.Unmarshal(data); err != nil {
						lg.Error("Unmarshal", zap.Error(err))
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if err := e.Encode(data); err != nil {
						lg.Error("Write", zap.Error(err))
						w.WriteHeader(http.StatusInternalServerError)
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
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		f, err := os.Open(arg.Data)
		if err != nil {
			return errors.Wrap(err, "open file")
		}
		defer func() {
			if err := f.Close(); err != nil {
				lg.Error("close file", zap.Error(err))
			}
		}()
		stat, err := f.Stat()
		if err != nil {
			return errors.Wrap(err, "stat file")
		}

		b := pb.New64(stat.Size())
		b.Start()
		defer b.Finish()

		r, err := zstd.NewReader(b.NewProxyReader(f))
		if err != nil {
			return errors.Wrap(err, "create decoder")
		}
		defer r.Close()
		d := gob.NewDecoder(r)
		client := &http.Client{}

		fn := func(data []byte) error {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, arg.Addr, bytes.NewReader(data))
			if err != nil {
				return errors.Wrap(err, "create request")
			}
			res, err := client.Do(req)
			if err != nil {
				return errors.Wrap(err, "do request")
			}
			defer func() {
				_ = res.Body.Close()
			}()
			out, _ := io.ReadAll(io.LimitReader(res.Body, 512))
			if len(out) == 0 {
				out = []byte("empty")
			}
			if res.StatusCode != http.StatusAccepted {
				return errors.Errorf("%s: %s", res.Status, out)
			}
			return nil
		}

		g, ctx := errgroup.WithContext(ctx)
		inputs := make(chan []byte, arg.Workers)
		for i := 0; i < arg.Workers; i++ {
			g.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case data := <-inputs:
						bo := backoff.NewExponentialBackOff()
						bo.MaxElapsedTime = time.Second * 5
						notify := func(err error, duration time.Duration) {
							lg.Error("Retry", zap.Duration("duration", duration), zap.Error(err))
						}
						if err := backoff.RetryNotify(func() error {
							return fn(data)
						}, backoff.WithContext(bo, ctx), notify); err != nil {
							return errors.Wrap(err, "retry")
						}
					}
				}
			})
		}
		g.Go(func() error {
			defer close(inputs)
			for {
				var data []byte
				if err := d.Decode(&data); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return errors.Wrap(err, "decode")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case inputs <- data:
				}
			}
		})
		return g.Wait()
	})
}
