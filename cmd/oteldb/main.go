package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	promodel "github.com/prometheus/common/model"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/autozpages"
)

func init() {
	// Match v3.0.0.
	promodel.NameValidationScheme = promodel.UTF8Validation
}

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Telemetry) error {
		ctx = zctx.WithOpenTelemetryZap(ctx)
		shutdown, err := autozpages.Setup(m.TracerProvider())
		if err != nil {
			return errors.Wrap(err, "setup zPages")
		}
		defer func() {
			_ = shutdown(context.Background())
		}()
		set := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		cfgPath := set.String("config", "", "Path to config (defaults to oteldb.yml)")
		if err := set.Parse(os.Args[1:]); err != nil {
			return err
		}

		cfg, err := loadConfig(*cfgPath)
		if err != nil {
			return errors.Wrap(err, "load config")
		}

		root, err := newApp(ctx, cfg, m)
		if err != nil {
			return errors.Wrap(err, "setup")
		}
		return root.Run(m.ShutdownContext())
	},
		app.WithServiceName("oteldb"),
	)
}
