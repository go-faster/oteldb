package autopyro

import (
	"context"
	"os"
	"runtime"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/grafana/pyroscope-go"
)

func noop(_ context.Context) error { return nil }

// ShutdownFunc is a function that shuts down profiler.
type ShutdownFunc func(ctx context.Context) error

// Setup pyroscope profiler.
func Setup(ctx context.Context) (ShutdownFunc, error) {
	s := os.Getenv("PYROSCOPE_ENABLE")
	if v, _ := strconv.ParseBool(s); !v {
		return noop, nil
	}

	// https://grafana.com/docs/pyroscope/latest/configure-client/language-sdks/go_push/#configure-the-go-client
	// These 2 lines are only required if you're using mutex or block profiling
	// Read the explanation below for how to set these rates:
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)

	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName:   os.Getenv("PYROSCOPE_APP_NAME"),
		ServerAddress:     os.Getenv("PYROSCOPE_URL"),
		AuthToken:         os.Getenv("PYROSCOPE_TOKEN"),
		BasicAuthUser:     os.Getenv("PYROSCOPE_USER"),
		BasicAuthPassword: os.Getenv("PYROSCOPE_PASSWORD"),
		TenantID:          os.Getenv("PYROSCOPE_TENANT_ID"),

		Logger: zctx.From(ctx).Named("pyroscope").Sugar(),

		ProfileTypes: []pyroscope.ProfileType{
			// these profile types are enabled by default:
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,

			// these profile types are optional:
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})
	if err != nil {
		return noop, errors.Wrap(err, "start")
	}

	return func(ctx context.Context) error {
		return profiler.Stop()
	}, nil
}
