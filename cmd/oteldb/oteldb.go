package main

import (
	"context"

	"go.uber.org/zap"
	ytzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"

	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/ytstore"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		yc, err := ythttp.NewClient(&yt.Config{
			Logger: &ytzap.Logger{L: zctx.From(ctx)},
		})
		if err != nil {
			return errors.Wrap(err, "yt")
		}

		tablePath := ypath.Path("//oteldb").Child("traces")
		s := ytstore.NewStore(yc, tablePath)
		if err := s.Migrate(ctx); err != nil {
			return errors.Wrap(err, "migrate")
		}

		recv, err := otelreceiver.NewReceiver(s, otelreceiver.ReceiverConfig{})
		if err != nil {
			return errors.Wrap(err, "create OTEL receiver")
		}
		return recv.Run(ctx)
	})
}
