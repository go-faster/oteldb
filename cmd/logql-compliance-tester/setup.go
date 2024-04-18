package main

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokicompliance"
)

func setup(ctx context.Context, cfg Config) (*lokicompliance.Comparer, error) {
	log := zctx.From(ctx)

	var targets []string
	for _, cfg := range []lokicompliance.TargetConfig{
		cfg.ReferenceTarget,
		cfg.TestTarget,
	} {
		raw := cfg.PushURL
		if raw == "" {
			raw = cfg.QueryURL
		}

		u, err := url.Parse(raw)
		if err != nil {
			return nil, errors.Wrapf(err, "parse target %q", raw)
		}
		if u.Path == "" {
			u = u.JoinPath("loki", "api", "v1", "push")
		}
		raw = u.String()

		targets = append(targets, raw)
	}

	log.Info("Generating logs",
		zap.Strings("targets", targets),
		zap.Time("start", cfg.Start),
		zap.Time("end", cfg.End),
	)
	if err := lokicompliance.GenerateLogs(ctx, targets, lokicompliance.GenerateOptions{
		Start: cfg.Start.Truncate(time.Second).Add(-30 * time.Second),
		End:   cfg.End.Truncate(time.Second).Add(30 * time.Second),
		Step:  time.Second,
		Lines: 5,
		Streams: []string{
			"log1.json",
			"log2.json",
			"log3.json",
		},
	}); err != nil {
		return nil, errors.Wrap(err, "generate logs")
	}

	refAPI, err := newLokiAPI(ctx, cfg.Start, cfg.End, cfg.ReferenceTarget)
	if err != nil {
		return nil, errors.Wrap(err, "creating reference API")
	}
	testAPI, err := newLokiAPI(ctx, cfg.Start, cfg.End, cfg.TestTarget)
	if err != nil {
		return nil, errors.Wrap(err, "creating test API")
	}
	return lokicompliance.New(refAPI, testAPI), nil
}

func newLokiAPI(ctx context.Context, start, end time.Time, cfg lokicompliance.TargetConfig) (lokicompliance.LokiAPI, error) {
	c, err := lokiapi.NewClient(cfg.QueryURL)
	if err != nil {
		return nil, err
	}

	if err := waitForLoki(ctx, c, start, end, cfg); err != nil {
		return nil, errors.Wrap(err, "wait for loki")
	}

	return c, nil
}

func waitForLoki(ctx context.Context, c *lokiapi.Client, start, end time.Time, targetCfg lokicompliance.TargetConfig) error {
	check := func(ctx context.Context) error {
		q := targetCfg.ReadyQuery
		if q == "" {
			q = `{job="varlogs"}`
		}

		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: q,
			Start: lokiapi.NewOptLokiTime(getLokiTime(start)),
			End:   lokiapi.NewOptLokiTime(getLokiTime(end)),
		})
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return backoff.Permanent(cerr)
			}
			return err
		}

		streams, ok := resp.Data.GetStreamsResult()
		if !ok {
			// Ready query should be exactly a log query.
			err := errors.Errorf("unexpected result type %q", resp.Data.Type)
			return backoff.Permanent(err)
		}

		for _, s := range streams.Result {
			if len(s.Values) > 0 {
				return nil
			}
		}
		return errors.New("empty result")
	}

	var (
		b = backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(5*time.Second),
			backoff.WithMaxElapsedTime(time.Minute),
		)
		log = zctx.From(ctx)
	)
	if err := backoff.RetryNotify(
		func() error {
			return check(ctx)
		},
		b,
		func(err error, d time.Duration) {
			log.Debug("Retry ping request",
				zap.String("target", targetCfg.QueryURL),
				zap.Error(err),
			)
		},
	); err != nil {
		return err
	}

	log.Info("Target is ready", zap.String("target", targetCfg.QueryURL))
	return nil
}

func getLokiTime(t time.Time) lokiapi.LokiTime {
	ts := strconv.FormatInt(t.UnixNano(), 10)
	return lokiapi.LokiTime(ts)
}
