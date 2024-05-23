package integration

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// Logger creates a new [zap.Logger] to use in tests.
func Logger(t *testing.T) *zap.Logger {
	return zaptest.NewLogger(t, zaptest.WrapOptions(zap.WrapCore(wrapTestLogger)))
}

func wrapTestLogger(core zapcore.Core) zapcore.Core {
	return &filterCore{core: core}
}

type filterCore struct {
	core zapcore.Core
}

var _ zapcore.Core = (*filterCore)(nil)

func (c *filterCore) Enabled(l zapcore.Level) bool {
	return c.core.Enabled(l)
}

func (c *filterCore) With(fields []zapcore.Field) zapcore.Core {
	return &filterCore{
		core: c.core.With(fields),
	}
}

func (c *filterCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if ce == nil || ce.LoggerName == "ch" {
		return ce
	}
	return c.core.Check(e, ce)
}

func (c *filterCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	return c.core.Write(e, fields)
}

func (c *filterCore) Sync() error {
	return c.core.Sync()
}
