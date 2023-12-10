package main

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/autologs"
	"github.com/go-faster/oteldb/internal/autozpages"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		shutdown, err := autozpages.Setup(m.TracerProvider())
		if err != nil {
			return errors.Wrap(err, "setup zPages")
		}
		defer func() {
			_ = shutdown(context.Background())
		}()
		if ctx, err = autologs.Setup(ctx, m); err != nil {
			return errors.Wrap(err, "setup logs")
		}
		root, err := newApp(ctx, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}
		return root.Run(ctx)
	})
}
