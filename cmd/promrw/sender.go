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
)

type Sender struct {
	Target  string
	Source  string
	Workers int
}

func (s *Sender) Run(ctx context.Context) error {
	f, err := os.Open(s.Source)
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
	d := NewReader(&pbr)
	client := &http.Client{
		Timeout: time.Minute,
	}
	fn := func(data []byte) error {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Target, bytes.NewReader(data))
		if err != nil {
			return errors.Wrap(err, "create request")
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "zstd")
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
	inputs := make(chan []byte)
	for i := 0; i < s.Workers; i++ {
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

func newSenderCommand() *cobra.Command {
	var sender Sender
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Send recorded requests to remote write server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return sender.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&sender.Target, "target", "http://127.0.0.1:19291", "Target server")
	cmd.Flags().StringVarP(&sender.Source, "input", "i", "requests.rwq", "Source file")
	cmd.Flags().IntVarP(&sender.Workers, "workers", "j", 8, "Number of workers")
	return cmd
}
