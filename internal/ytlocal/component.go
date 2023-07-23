package ytlocal

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/yson"
	"golang.org/x/sync/errgroup"
)

// Component describes a component type.
type Component string

// Component types.
const (
	ComponentHTTPProxy Component = "http-proxy"
	ComponentMaster    Component = "master"
)

// Server describes a component server.
type Server[T any] struct {
	Type   Component
	Config T
	Binary string
	Dir    string
}

// Run runs a component server.
func (s *Server[T]) Run(ctx context.Context) error {
	lg := zctx.From(ctx).Named(string(s.Type))

	// Prepare configuration.
	cfgDir := filepath.Join(s.Dir, string(s.Type))
	// #nosec: G301
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return errors.Wrap(err, "mkdir all")
	}

	data, err := yson.Marshal(s.Config)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}
	// Save configuration.
	cfgPath := filepath.Join(cfgDir, "cfg.yson")
	// #nosec: G306
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		return errors.Wrap(err, "write config")
	}

	g, ctx := errgroup.WithContext(ctx)

	// Run binary.
	// #nosec: G204
	cmd := exec.CommandContext(ctx, s.Binary, "--config", cfgPath)
	r, w := io.Pipe()
	cmd.Stderr = w
	cmd.Dir = s.Dir

	g.Go(func() error {
		defer func() {
			_ = w.Close()
		}()
		return cmd.Run()
	})
	g.Go(func() error {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			text := sc.Text()
			if i := strings.IndexByte(text, '\t'); i != -1 {
				// Trim timestamp.
				text = strings.TrimSpace(text[i+1:])
			}
			lvl := zap.InfoLevel
			var txtLevel string
			if len(text) > 0 {
				txtLevel = text[:1]
			}
			switch txtLevel {
			case "W":
				lvl = zap.WarnLevel
			case "D":
				lvl = zap.DebugLevel
			case "I":
				lvl = zap.InfoLevel
			case "E":
				lvl = zap.ErrorLevel
			default:
			}
			if i := strings.IndexByte(text, '\t'); i != -1 {
				// Trim log level.
				text = strings.TrimSpace(text[i+1:])
			}
			lg.Check(lvl, text).Write()
		}
		return sc.Err()
	})
	return g.Wait()
}

// Options describes options for creating a new component server.
type Options struct {
	Binary *Binary
	Dir    string
}

// NewComponent creates a new component server.
func NewComponent[T any](opt Options, cfg T) Server[T] {
	var t Component
	switch any(cfg).(type) {
	case Master:
		t = ComponentMaster
	case HTTPProxy:
		t = ComponentHTTPProxy
	default:
		panic("unknown component")
	}
	bin, ok := opt.Binary.Components[t]
	if !ok || bin == "" {
		panic("unknown component path")
	}
	return Server[T]{
		Type:   t,
		Config: cfg,
		Binary: bin,
		Dir:    opt.Dir,
	}
}
