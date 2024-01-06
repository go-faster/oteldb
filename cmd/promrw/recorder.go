package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/prompb"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

type Recorder struct {
	Addr     string
	Duration time.Duration
	Output   string

	points   atomic.Uint64
	requests atomic.Uint64
	bytes    atomic.Uint64
}

func (r *Recorder) read(req *http.Request) ([]byte, error) {
	if t := req.Header.Get("Content-Type"); t != "application/x-protobuf" {
		return nil, errors.Errorf("unsupported content type %q", t)
	}
	if e := req.Header.Get("Content-Encoding"); e != "zstd" {
		return nil, errors.Errorf("unsupported encoding %q", e)
	}

	compressedData, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read compressed data")
	}

	d, err := zstd.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, errors.Wrap(err, "create decoder")
	}
	defer d.Close()

	data, err := io.ReadAll(d)
	if err != nil {
		return nil, errors.Wrap(err, "read data")
	}
	var writeRequest prompb.WriteRequest
	if err := writeRequest.Unmarshal(data); err != nil {
		return nil, errors.Wrap(err, "unmarshal request")
	}

	r.points.Add(uint64(len(writeRequest.Timeseries)))
	r.requests.Inc()
	r.bytes.Add(uint64(len(compressedData)))

	return compressedData, nil
}

func fmtInt(v int) string {
	s := humanize.SIWithDigits(float64(v), 0, "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func (r *Recorder) Run(ctx context.Context) (rerr error) {
	fmt.Println("listening on", "http://"+r.Addr)
	fmt.Println("writing to", r.Output, "for", r.Duration)
	f, err := os.Create(r.Output)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	defer func() {
		if err := f.Close(); err != nil {
			rerr = multierr.Append(rerr, errors.Wrap(err, "close file"))
		} else {
			fmt.Println("wrote", r.Output)
		}
	}()

	e := NewWriter(f)
	srv := &http.Server{
		Addr:              r.Addr,
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				return
			}
			compressedData, err := r.read(req)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if err := e.Encode(compressedData); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		}),
	}
	var (
		start = time.Now()
		until = start.Add(r.Duration)
		done  = make(chan struct{})
	)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				wr := r.requests.Load()
				wb := r.bytes.Load()
				wp := r.points.Load()
				fmt.Printf("req=%d bytes=%s points=%s (%s left)\n",
					wr, humanize.Bytes(wb), fmtInt(int(wp)), until.Sub(now).Round(time.Second),
				)
			case <-done:
				fmt.Println("done")
				return nil
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
		select {
		case <-ctx.Done():
		case <-done:
		}
		stopCtx := context.Background()
		return srv.Shutdown(stopCtx)
	})
	g.Go(func() error {
		defer close(done)
		select {
		case <-ctx.Done():
		case <-time.After(r.Duration):
		}
		return nil
	})
	return g.Wait()
}

func newRecorderCommand() *cobra.Command {
	var recorder Recorder
	cmd := &cobra.Command{
		Use:   "record",
		Args:  cobra.NoArgs,
		Short: "Listen for remote write requests and record them to file",
		RunE: func(cmd *cobra.Command, args []string) error {
			return recorder.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&recorder.Addr, "addr", "127.0.0.1:8080", "Address to listen on")
	cmd.Flags().DurationVarP(&recorder.Duration, "duration", "d", time.Minute*5, "Duration to record")
	cmd.Flags().StringVarP(&recorder.Output, "output", "o", "requests.rwq", "Output file")
	return cmd
}
