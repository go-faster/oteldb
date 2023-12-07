package main

import (
	"context"

	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"
)

func main() {
	app.Run(func(ctx context.Context, lg *zap.Logger, m *app.Metrics) error {
		<-ctx.Done()
		return nil
	})
}
