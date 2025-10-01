package chembed

import (
	"context"
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"
)

//go:embed clickhouse.users.xml
var usersCfg []byte

// New starts a new embedded ClickHouse server.
func New(ctx context.Context, lg *zap.Logger) error {
	p, err := exec.LookPath("clickhouse")
	if err != nil {
		return errors.Wrap(err, "lookup")
	}

	dir := "/clickhouse"
	cfgPath := filepath.Join(dir, "config.xml")
	userCfgPath := filepath.Join(dir, "users.xml")
	cfg := Config{
		Logger: Logger{
			Level:   "trace",
			Console: 1,
		},

		HTTP: 8123,
		TCP:  9000,

		Host: "127.0.0.1",

		Path:          filepath.Join(dir, "data"),
		TempPath:      filepath.Join(dir, "tmp"),
		UserFilesPath: filepath.Join(dir, "users"),

		MaxServerMemoryUsage: 1024 * 1024 * 1024 * 4, // 4GB

		MarkCacheSize: 5368709120,
		MMAPCacheSize: 1000,

		OpenTelemetrySpanLog: &OpenTelemetry{
			Table:    "opentelemetry_span_log",
			Database: "system",
			Engine: `engine MergeTree
            partition by toYYYYMM(finish_date)
            order by (finish_date, finish_time_us, trace_id)`,
		},

		UserDirectories: UserDir{
			UsersXML: UsersXML{
				Path: userCfgPath,
			},
		},
	}
	if err := writeXML(cfgPath, cfg); err != nil {
		return errors.Wrap(err, "write config")
	}

	for _, dir := range []string{
		cfg.Path,
		cfg.TempPath,
		cfg.UserFilesPath,
	} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return errors.Wrapf(err, "mkdir %q", dir)
		}
	}
	if err := os.WriteFile(userCfgPath, usersCfg, 0o600); err != nil {
		return errors.Wrap(err, "write users config")
	}

	// Setup command.
	cmd := exec.CommandContext(ctx, p, "server", "--config-file", cfgPath) // #nosec G204
	started := make(chan struct{})
	onAddr := func(info logInfo) {
		if info.Ready {
			close(started)
		}
	}
	cmd.Stdout = logProxy(lg, onAddr)
	cmd.Stderr = logProxy(lg, onAddr)
	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "start")
	}

	start := time.Now()

	wait := make(chan error)
	go func() {
		defer close(wait)
		wait <- cmd.Wait()
	}()

	startTimeout := time.Second * 10
	if runtime.GOARCH == "riscv64" {
		// RISC-V devboards are slow.
		startTimeout = time.Minute
	}

	select {
	case <-started:
		lg.Info(
			"Clickhouse started",
			zap.Duration("duration", time.Since(start).Round(time.Millisecond)),
		)
	case err := <-wait:
		lg.Info("Clickhouse exited", zap.Error(err))
		if err == nil {
			err = errors.New("clickhouse exited without error")
		}
		return err
	case <-time.After(startTimeout):
		return errors.New("clickhouse start timeout")
	}

	return nil
}
