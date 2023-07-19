package ytlocal

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"testing"

	"github.com/go-faster/sdk/gold"
)

var _interrupted context.Context

// interrupted returns a context that is canceled when the process receives an SIGINT.
//
// This is useful when test is started with `test2json`.
func interrupted() context.Context {
	return _interrupted
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancelCause(context.Background())
	_interrupted = ctx

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			cancel(errors.New("interrupted"))
		}
	}()

	gold.Init()
	os.Exit(m.Run())
}
