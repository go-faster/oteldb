package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/otelbench"
)

type Replay struct {
	Target   string
	Source   string
	Encoding string
	Workers  int
}

func (r *Replay) Run(ctx context.Context) error {
	f, err := os.Open(r.Source)
	if err != nil {
		return errors.Wrap(err, "open file")
	}
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(">> close:", err)
		}
	}()
	stat, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat file")
	}

	pb := progressbar.DefaultBytes(
		stat.Size(),
		"sending",
	)
	pbr := progressbar.NewReader(f, pb)
	d := otelbench.NewReader(&pbr)
	client := &http.Client{
		Timeout: time.Minute,
	}
	fn := func(data []byte) error {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Target, bytes.NewReader(data))
		if err != nil {
			return errors.Wrap(err, "create request")
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", r.Encoding)
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
		if !(res.StatusCode == http.StatusAccepted || res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusOK) {
			return errors.Errorf("%s: %s", res.Status, out)
		}
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	inputs := make(chan []byte)
	for i := 0; i < r.Workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case data, ok := <-inputs:
					if !ok {
						return nil
					}
					bo := backoff.NewExponentialBackOff()
					bo.MaxElapsedTime = time.Minute
					do := func() error { return fn(data) }
					if err := backoff.Retry(do, backoff.WithContext(bo, ctx)); err != nil {
						return errors.Wrap(err, "retry")
					}
				}
			}
		})
	}
	g.Go(func() error {
		defer close(inputs)
		for d.Decode() {
			data := d.Data()
			select {
			case <-ctx.Done():
				return nil
			case inputs <- slices.Clone(data):
				continue
			}
		}
		return d.Err()
	})
	if err := g.Wait(); err != nil {
		if errors.Is(err, ctx.Err()) {
			_ = pb.Exit()
			fmt.Println("Canceled")
			return nil
		}
		return err
	}
	_ = pb.Finish()
	fmt.Println("Done")
	return nil
}

func newReplayCommand() *cobra.Command {
	var replay Replay
	cmd := &cobra.Command{
		Use:     "replay",
		Aliases: []string{"send"},
		Short:   "Send recorded requests to remote write server",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return replay.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&replay.Target, "target", "http://127.0.0.1:19291", "Target server")
	cmd.Flags().StringVarP(&replay.Source, "input", "i", "requests.rwq", "Source file")
	cmd.Flags().IntVarP(&replay.Workers, "workers", "j", 8, "Number of workers")
	cmd.Flags().StringVar(&replay.Encoding, "encoding", "zstd", "Encoding")
	return cmd
}
