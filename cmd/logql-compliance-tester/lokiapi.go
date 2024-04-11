package main

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokicompliance"
)

func newLokiAPI(ctx context.Context, cfg lokicompliance.TargetConfig) (lokicompliance.LokiAPI, error) {
	c, err := lokiapi.NewClient(cfg.QueryURL)
	if err != nil {
		return nil, err
	}

	if err := waitForLoki(ctx, c, cfg); err != nil {
		return nil, errors.Wrap(err, "wait for loki")
	}

	return c, nil
}

func waitForLoki(ctx context.Context, c *lokiapi.Client, cfg lokicompliance.TargetConfig) error {
	var (
		b   = backoff.NewConstantBackOff(5 * time.Second)
		log = zctx.From(ctx)
	)
	if err := backoff.RetryNotify(
		func() error {
			_, err := c.Labels(ctx, lokiapi.LabelsParams{})
			if cerr := ctx.Err(); cerr != nil {
				return backoff.Permanent(cerr)
			}
			return err
		},
		b,
		func(err error, d time.Duration) {
			log.Debug("Retry ping request", zap.String("target", cfg.QueryURL))
		},
	); err != nil {
		return err
	}
	log.Info("Loki is ready", zap.String("target", cfg.QueryURL))
	return nil
}
