package ytlocal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapio"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func waitForNode(ctx context.Context, yc yt.Client, path ypath.Path) error {
	eb := backoff.NewExponentialBackOff()
	return backoff.Retry(func() error {
		_, err := yc.NodeExists(ctx, path, nil)
		if err != nil {
			if ctx.Err() != nil {
				return backoff.Permanent(err)
			}
			return err
		}
		return nil
	}, eb)
}

// SetupMaster sets master's config.
func SetupMaster(ctx context.Context, yc yt.Client, master Master) error {
	lg := zctx.From(ctx)
	const (
		user          = "admin"
		adminPassword = "admin"
	)
	adminPasswordSHA256 := fmt.Sprintf("%x", sha256.Sum256([]byte(adminPassword)))

	sys := ypath.Root.Child("sys")
	if err := waitForNode(ctx, yc, sys); err != nil {
		return errors.Wrap(err, "connect to proxy")
	}

	// Initialize world.
	//
	// See https://github.com/ytsaurus/ytsaurus/blob/main/yt/python/yt/environment/init_cluster.py#L128 for reference,
	// the `initialize_world` function.

	// yt remove //sys/@provision_lock -f
	if err := yc.RemoveNode(ctx, sys.Attr("provision_lock"), &yt.RemoveNodeOptions{Force: true}); err != nil {
		return errors.Wrap(err, "provision lock")
	}

	if _, err := yc.CreateObject(ctx, yt.NodeSchedulerPoolTree, &yt.CreateObjectOptions{
		IgnoreExisting: true,
		Attributes: map[string]any{
			"name": "default",
			"config": map[string]any{
				"nodes_filter": "!foo",
			},
		},
	}); err != nil {
		return errors.Wrap(err, "create scheduler pool tree")
	}
	if err := yc.SetNode(ctx, sys.Child("pool_trees").Attr("default_tree"), "default", &yt.SetNodeOptions{}); err != nil {
		return errors.Wrap(err, "set node")
	}
	if _, err := yc.CreateObject(ctx, yt.NodeSchedulerPool, &yt.CreateObjectOptions{
		IgnoreExisting: true,
		Attributes: map[string]any{
			"name":      "research",
			"pool_tree": "default",
		},
	}); err != nil {
		return errors.Wrap(err, "create scheduler pool")
	}

	if _, err := yc.CreateNode(ctx, ypath.Root.Child("home"), yt.NodeMap, &yt.CreateNodeOptions{IgnoreExisting: true}); err != nil {
		return errors.Wrap(err, "create node")
	}

	if _, err := yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": user,
		},
	}); err != nil {
		return errors.Wrap(err, "create user")
	}

	if _, err := yc.CreateNode(ctx, sys.Child("cypress_tokens").Child(adminPasswordSHA256), yt.NodeMap, &yt.CreateNodeOptions{
		IgnoreExisting: true,
		Attributes: map[string]any{
			"user": user,
		},
	}); err != nil {
		return errors.Wrap(err, "create cypress token")
	}

	if err := yc.AddMember(ctx, "superusers", user, nil); err != nil {
		return errors.Wrapf(err, "add member %q to group %q", user, "superusers")
	}

	for _, u := range []string{
		"odin", "cron", "cron_merge", "cron_compression", "cron_operations", "cron_tmp",
		"nightly_tester", "robot-yt-mon", "transfer_manager", "fennel", "robot-yt-idm",
		"robot-yt-hermes",
	} {
		if _, err := yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"name": u,
			},
		}); err != nil {
			return errors.Wrap(err, "create user")
		}
	}
	const everyoneGroup = "everyone"
	for _, group := range []string{
		"devs", "admins", "admin_snapshots", everyoneGroup,
	} {
		if _, err := yc.CreateObject(ctx, yt.NodeGroup, &yt.CreateObjectOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"name": group,
			},
		}); err != nil {
			return errors.Wrap(err, "create group")
		}
	}

	for _, cronUser := range []string{
		"cron", "cron_merge", "cron_compression", "cron_operations", "cron_tmp",
	} {
		if err := yc.AddMember(ctx, "superusers", cronUser, nil); err != nil {
			return errors.Wrapf(err, "add member %q to group %q", cronUser, "superusers")
		}

		const qSize = 500
		qSizePath := sys.
			Child("users").
			Child(cronUser).
			Attr("request_queue_size_limit")
		if err := yc.SetNode(ctx, qSizePath, qSize, &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "set req queue")
		}
	}
	if _, err := yc.CreateNode(ctx, sys.Child("cron"), yt.NodeMap, &yt.CreateNodeOptions{
		IgnoreExisting: true,
	}); err != nil {
		return errors.Wrap(err, "create cron node")
	}
	for _, add := range []struct {
		What string
		To   string
	}{
		{What: "devs", To: "admins"},
		{What: "robot-yt-mon", To: "admin_snapshots"},
		{What: "robot-yt-idm", To: "superusers"},
	} {
		if err := yc.AddMember(ctx, add.To, add.What, nil); err != nil {
			return errors.Wrapf(err, "add member %q to group %q", add.To, add.What)
		}
	}
	for _, dir := range []ypath.Path{
		sys, sys.Child("tokens"), "//tmp",
	} {
		if err := yc.SetNode(ctx, dir.Attr("opaque"), true, &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "set opaque")
		}
	}

	if err := yc.SetNode(ctx, sys.Attr("inherit_acl"), false, &yt.SetNodeOptions{}); err != nil {
		return errors.Wrap(err, "set opaque")
	}
	for _, acl := range []struct {
		Path ypath.Path
		ACL  map[string]any
	}{
		{
			Path: ypath.Root,
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{everyoneGroup},
				"permissions": []string{"read"},
			},
		},
		{
			Path: ypath.Root,
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"admins"},
				"permissions": []string{"write", "remove", "administer", "mount"},
			},
		},
		{
			Path: sys,
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"users"},
				"permissions": []string{"read"},
			},
		},
		{
			Path: sys,
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"admins"},
				"permissions": []string{"write", "remove", "administer", "mount"},
			},
		},
		{
			Path: sys.Child("accounts").Child("sys"),
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"root", "admins"},
				"permissions": []string{"use"},
			},
		},
		{
			Path: sys.Child("tokens"),
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"admins"},
				"permissions": []string{"read", "write", "remove"},
			},
		},
		{
			Path: sys.Child("tablet_cells"),
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"admins"},
				"permissions": []string{"read", "write", "remove", "administer"},
			},
		},
		{
			Path: sys.Child("tablet_cells"),
			ACL: map[string]any{
				"action":      "allow",
				"subjects":    []string{"odin"},
				"permissions": []string{"read"},
			},
		},
	} {
		// TODO: check if acl exists.
		if err := yc.SetNode(ctx, acl.Path.Attr("acl").Child("end"), acl.ACL, &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "set opaque")
		}
	}
	for _, p := range []ypath.Path{
		sys, sys.Child("tokens"), sys.Child("tablet_cells"),
	} {
		if err := yc.SetNode(ctx, p.Attr("inherit_acl"), false, &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "set opaque")
		}
	}

	// Check tablet cells.
	var (
		tabletBundle = sys.Child("tablet_cell_bundles").Child("default")
		tabletCells  []yt.NodeID
	)
	if err := yc.GetNode(
		ctx,
		tabletBundle.Attr("tablet_cell_ids"),
		&tabletCells,
		nil,
	); err != nil {
		return errors.Wrap(err, "get tablet cells")
	}

	if len(tabletCells) == 0 {
		lg.Info("Creating new tablet cell")

		if err := yc.SetNode(
			ctx,
			tabletBundle.Attr("options"),
			map[string]any{
				"changelog_replication_factor": 1,
				"changelog_read_quorum":        1,
				"changelog_write_quorum":       1,
				"changelog_account":            "sys",
				"snapshot_account":             "sys",
			},
			nil,
		); err != nil {
			return errors.Wrap(err, "setup tablet bundle")
		}

		tabletCellID, err := yc.CreateObject(ctx, yt.NodeTabletCell, &yt.CreateObjectOptions{})
		if err != nil {
			return errors.Wrap(err, "create tablet cell")
		}
		tabletCells = append(tabletCells, tabletCellID)
	}

	// Setup cluster connection.
	if err := yc.SetNode(ctx, ypath.Path("//sys/@cluster_connection"), master.ClusterConnection, nil); err != nil {
		return errors.Wrap(err, "set cluster connection")
	}

	return nil
}

// SetupQueryTracker setups query tracker.
func SetupQueryTracker(ctx context.Context, yc yt.Client, clusterName string) error {
	if _, err := yc.CreateObject(
		ctx,
		yt.NodeUser,
		&yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": "query_tracker",
			},
			IgnoreExisting: true,
		},
	); err != nil {
		return errors.Wrap(err, "create query tracker user")
	}

	if err := yc.AddMember(ctx, "superusers", "query_tracker", nil); err != nil {
		return errors.Wrap(err, "add query tracker user to superusers")
	}

	if _, err := yc.CreateNode(
		ctx,
		ypath.Path("//sys/query_tracker/config"),
		yt.NodeDocument,
		&yt.CreateNodeOptions{
			Attributes: map[string]any{
				"value": map[string]any{
					"query_tracker": map[string]any{
						"ql_engine": map[string]any{
							"default_cluster": clusterName,
						},
						"chyt_engine": map[string]any{
							"default_cluster": clusterName,
						},
					},
				},
			},
			Recursive:      true,
			IgnoreExisting: true,
		},
	); err != nil {
		return errors.Wrap(err, "create query tracker config")
	}

	if err := yc.SetNode(
		ctx,
		ypath.Path("//sys/@cluster_connection/query_tracker"),
		map[string]any{
			"stages": map[string]any{
				"production": map[string]any{
					"root": "//sys/query_tracker",
					"user": "query_tracker",
				},
				"testing": map[string]any{
					"root": "//sys/query_tracker",
					"user": "query_tracker",
				},
			},
		},
		nil,
	); err != nil {
		return errors.Wrap(err, "create query tracker cluster connection")
	}

	var cc any
	if err := yc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &cc, nil); err != nil {
		return errors.Wrap(err, "get cluster connection")
	}

	if err := yc.SetNode(
		ctx,
		ypath.Path(fmt.Sprintf("//sys/clusters/%s", clusterName)),
		cc,
		nil,
	); err != nil {
		return errors.Wrapf(err, "set cluster %q config", clusterName)
	}

	if err := yc.SetNode(
		ctx,
		ypath.Path("//sys/@cluster_name"),
		clusterName,
		nil,
	); err != nil {
		return errors.Wrap(err, "set cluster name")
	}

	return nil
}

// CHYTBinary contains paths to CHYT-related executables.
type CHYTBinary struct {
	Controller string
	Trampoline string
	Tailer     string
	Server     string
}

// NewCHYTBinary looks for CHYT-related executables
func NewCHYTBinary(p string) (b CHYTBinary, _ error) {
	for _, exe := range []struct {
		name string
		to   *string
	}{
		{"chyt-controller", &b.Controller},
		{"clickhouse-trampoline", &b.Trampoline},
		{"ytserver-log-tailer", &b.Tailer},
		{"ytserver-clickhouse", &b.Server},
	} {
		path, err := exec.LookPath(exe.name)
		if err != nil {
			return b, errors.Wrapf(err, "can't find %q", exe.name)
		}

		target := filepath.Join(p, exe.name)
		*exe.to = target
		if _, err := os.Stat(target); err == nil {
			// Binary exists.
			continue
		}
		// Create link.
		if err := os.Symlink(path, target); err != nil {
			return b, errors.Wrapf(err, "symlink %q to %q", path, target)
		}
	}

	return b, nil
}

// ControllerServer returns Server[T] for CHYT controller
func (e *CHYTBinary) ControllerServer(opt Options, cfg CHYTController) *Server[CHYTController] {
	return &Server[CHYTController]{
		Type:   ComponentCHYTContoller,
		Config: cfg,
		Binary: e.Controller,
		Dir:    opt.Dir,
	}
}

// Setup setups CHYT cluster.
func (e *CHYTBinary) Setup(ctx context.Context, yc yt.Client, controllerAddr, dir string, cfg CHYTInitCluster) error {
	if err := e.ensureExecutables(ctx, yc); err != nil {
		return errors.Wrap(err, "ensure executables")
	}

	if _, err := yc.CreateObject(
		ctx,
		yt.NodeAccessControlObjectNamespace,
		&yt.CreateObjectOptions{
			IgnoreExisting: true,
			Attributes:     map[string]any{"name": "chyt"},
		},
	); err != nil {
		return errors.Wrap(err, "create access control namespace")
	}

	if err := waitForNode(ctx, yc, ypath.Path("//sys/discovery_servers")); err != nil {
		return errors.Wrap(err, "wait for discovery server")
	}

	const alias = "ch_public"

	if err := e.createUsers(ctx, yc); err != nil {
		return errors.Wrap(err, "create users")
	}
	if _, err := yc.CreateObject(ctx, yt.NodeGroup, &yt.CreateObjectOptions{
		Attributes: map[string]any{
			"name": "/chyt/" + alias,
		},
	}); err != nil {
		return errors.Wrap(err, "create group")
	}

	if err := e.initCluster(ctx, dir, cfg); err != nil {
		return errors.Wrap(err, "init cluster")
	}

	if err := e.createClique(ctx, controllerAddr, cfg.Proxy, alias); err != nil {
		return errors.Wrap(err, "create clique")
	}

	// if err := e.runOperation(ctx, yc, operationConfig{
	// 	Alias:          alias,
	// 	ExecutablesDir: ypath.Path("//sys/bin"),
	// 	InstanceCount:  1,
	// 	Limits: CHLimits{
	// 		CPULimit:                   2,
	// 		Reader:                     100000000,
	// 		ChunkMetaCache:             100000000,
	// 		CompressedBlockCache:       100000000,
	// 		Clickhouse:                 100000000,
	// 		ClickhouseWatermark:        10,
	// 		Footprint:                  500000000,
	// 		LogTailer:                  100000000,
	// 		WatchdogOOMWatermark:       0,
	// 		WatchdogOOMWindowWatermark: 0,
	// 	},
	// }); err != nil {
	// 	return errors.Wrap(err, "run operation")
	// }

	return nil
}

func (e *CHYTBinary) ensureExecutables(ctx context.Context, yc yt.Client) error {
	uploadFile := func(ctx context.Context, name string, to ypath.Path) error {
		// #nosec: G304
		r, err := os.Open(name)
		if err != nil {
			return errors.Wrapf(err, "open file %q", name)
		}
		defer func() {
			_ = r.Close()
		}()

		if _, err := yc.CreateNode(ctx,
			to,
			yt.NodeFile,
			&yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true},
		); err != nil {
			return errors.Wrap(err, "create file")
		}

		w, err := yc.WriteFile(ctx, to, &yt.WriteFileOptions{})
		if err != nil {
			return errors.Wrap(err, "create writer")
		}
		defer func() {
			_ = w.Close()
		}()

		if _, err := io.Copy(w, r); err != nil {
			return errors.Wrap(err, "copy")
		}

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				ok, err := yc.NodeExists(ctx, to, &yt.NodeExistsOptions{})
				if err != nil {
					return errors.Wrapf(err, "check %q exists", to)
				}

				if ok {
					return nil
				}
			}
		}
	}
	ensureExe := func(ctx context.Context, name string, to ypath.Path) error {
		ok, err := yc.NodeExists(ctx, to, &yt.NodeExistsOptions{})
		if err != nil {
			return errors.Wrapf(err, "check %q exists", to)
		}

		if !ok {
			lg := zctx.From(ctx)
			lg.Info("File not found, upload", zap.Stringer("path", to))
			b := backoff.NewExponentialBackOff()
			b.MaxElapsedTime = time.Minute
			if err := backoff.RetryNotify(
				func() error {
					return uploadFile(ctx, name, to)
				},
				b,
				func(err error, d time.Duration) {
					lg.Error("File upload failed, retry", zap.Error(err), zap.Duration("after", d))
				},
			); err != nil {
				return errors.Wrap(err, "upload")
			}
		}

		if err := yc.SetNode(ctx, to.Attr("executable"), true, &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "mark as executable")
		}
		return nil
	}

	// See https://github.com/ytsaurus/yt-k8s-operator/blob/bf3a8b6025154b14ed9caf90390be3391381f62f/pkg/components/chyt.go#L118
	sysBin := ypath.Path("//sys/bin")
	for _, exe := range []struct {
		name      string
		localPath string
	}{
		{"clickhouse-trampoline", e.Trampoline},
		{"ytserver-log-tailer", e.Tailer},
		{"ytserver-clickhouse", e.Server},
	} {
		dir := sysBin.Child(exe.name)
		if _, err := yc.CreateNode(ctx, dir, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true}); err != nil {
			return errors.Wrapf(err, "create map node %q", dir)
		}

		target := dir.Child(exe.name)
		if err := ensureExe(ctx, exe.localPath, target); err != nil {
			return errors.Wrapf(err, "ensure executable %q", target)
		}
	}

	return nil
}

func (e *CHYTBinary) createUsers(ctx context.Context, yc yt.Client) error {
	for _, name := range []string{
		"yt-clickhouse",
		"robot-chyt-controller",
	} {
		if _, err := yc.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": name,
			},
		}); err != nil {
			return errors.Wrapf(err, "create user %q", name)
		}
	}

	return nil
}

func (e *CHYTBinary) initCluster(ctx context.Context, dir string, cfg CHYTInitCluster) error {
	data, err := yson.MarshalFormat(cfg, yson.FormatPretty)
	if err != nil {
		return errors.Wrap(err, "encode config")
	}

	configPath := filepath.Join(dir, "init-cluster-cfg.yson")
	// #nosec: G306
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		return errors.Wrap(err, "write config")
	}

	lg := zctx.From(ctx).Named("init-cluster")
	// #nosec: G204
	cmd := exec.CommandContext(ctx, e.Controller, "--config-path", configPath, "init-cluster")
	cmd.Stdout = &zapio.Writer{Log: lg.Named("stdout")}
	cmd.Stderr = &zapio.Writer{Log: lg.Named("stderr")}
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "init chyt cluster")
	}
	lg.Info("CHYT controller init-cluster is complete")

	return nil
}

// UserScript defines vanila task spec.
type UserScript struct {
	Command                       string            `yson:"command"`
	Environment                   map[string]string `yson:"environment,omitempty"`
	InterruptionSignal            string            `yson:"interruption_signal,omitempty"`
	JobCount                      int               `yson:"job_count,omitempty"`
	RestartCompletedJobs          bool              `yson:"restart_completed_jobs"`
	FilePaths                     []spec.File       `yson:"file_paths,omitempty"`
	MemoryLimit                   int64             `yson:"memory_limit,omitempty"`
	CPULimit                      float32           `yson:"cpu_limit,omitempty"`
	MaxStderrSize                 int64             `yson:"max_stderr_size,omitempty"`
	PortCount                     int               `yson:"port_count,omitempty"`
	UserJobMemoryDigestLowerBound float32           `yson:"user_job_memory_digest_lower_bound,omitempty"`
}

// VanilaOperation is a spec of Vanila operation.
type VanilaOperation struct {
	Alias string `yson:"alias"`
	Title string `yson:"title,omitempty"`

	Tasks map[string]UserScript `yson:"tasks"`

	MaxFailedJobCount int    `yson:"max_failed_job_count,omitempty"`
	MaxStderrCount    int    `yson:"max_failed_job_count,omitempty"`
	StderrTablePath   string `yson:"stderr_table_path,omitempty"`
	CoreTablePath     string `yson:"core_table_path,omitempty"`

	Description map[string]any `yson:"description,omitempty"`
	Annotations map[string]any `yson:"annotations,omitempty"`
}

// CHLimits defines Clickhouse engine resource limits.
type CHLimits struct {
	// CPULimit is a number of cores allowed to use.
	CPULimit int

	Reader                     int64
	UncompressedBlockCache     int64
	CompressedBlockCache       int64
	ChunkMetaCache             int64
	Clickhouse                 int64
	Footprint                  int64
	ClickhouseWatermark        int64
	WatchdogOOMWatermark       int64
	WatchdogOOMWindowWatermark int64
	LogTailer                  int64
}

// MaxServerMemoryUsage returns maximum memory usage.
func (l CHLimits) MaxServerMemoryUsage() int64 {
	return l.Reader +
		l.UncompressedBlockCache +
		l.CompressedBlockCache +
		l.Clickhouse +
		l.Footprint +
		l.ChunkMetaCache
}

// MemoryLimit returns total memory limit.
func (l CHLimits) MemoryLimit() int64 {
	return l.MaxServerMemoryUsage() + l.ClickhouseWatermark
}

type operationConfig struct {
	Alias          string
	ExecutablesDir ypath.Path
	InstanceCount  int
	Limits         CHLimits
}

func (e *CHYTBinary) runOperation(ctx context.Context, yc yt.Client, cfg operationConfig) error {
	var cc map[string]any
	if err := yc.GetNode(ctx, ypath.Path("//sys/@cluster_connection"), &cc, nil); err != nil {
		return errors.Wrap(err, "get cluster connection")
	}
	cc["block_cache"] = map[string]any{
		"block_cache": map[string]any{
			"uncompressed_data": map[string]any{
				"capacity": cfg.Limits.UncompressedBlockCache,
			},
		},
	}

	chCfg := map[string]any{
		"engine": map[string]any{
			"settings": map[string]any{
				"max_threads":                      cfg.Limits.CPULimit,
				"max_memory_usage_for_all_queries": cfg.Limits.Clickhouse,
				"log_queries":                      1,
				"queue_max_wait_ms":                30 * 1000,
			},
			"max_server_memory_usage": cfg.Limits.MaxServerMemoryUsage(),
		},
		"profile_manager": map[string]any{
			"global_tags": cfg.Alias,
		},
		"cluster_connection": cc,
		"discovery": map[string]any{
			"directory": "//sys/clickhouse/cliques",
		},
		"worker_thread_count": cfg.Limits.CPULimit,
		"cpu_limit":           cfg.Limits.CPULimit,
		"memory_watchdog": map[string]any{
			"memory_limit": cfg.Limits.MemoryLimit(),
		},
		"yt": map[string]any{
			"worker_thread_count": cfg.Limits.CPULimit,
			"cpu_limit":           cfg.Limits.CPULimit,
			"memory_watchdog": map[string]any{
				"memory_limit": cfg.Limits.MemoryLimit(),
			},
		},
		"launcher": map[string]any{},
		"memory": map[string]any{
			"clickhouse": cfg.Limits.Clickhouse,
		},
	}
	tailerCfg := map[string]any{
		"profile_manager": map[string]any{
			"global_tags": cfg.Alias,
		},
	}

	cfgDir := ypath.Path(`//sys/clickhouse/kolkhoz/tmp`)
	if _, err := yc.CreateNode(ctx, cfgDir, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true}); err != nil {
		return errors.Wrap(err, "create clickhouse config dir")
	}

	writeConfigFile := func(ctx context.Context, path ypath.Path, data any) error {
		zctx.From(ctx).Info("Write CHYT config", zap.Stringer("path", path))

		if _, err := yc.CreateNode(ctx,
			path,
			yt.NodeFile,
			&yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true},
		); err != nil {
			return errors.Wrap(err, "create file")
		}

		wc, err := yc.WriteFile(ctx, path, nil)
		if err != nil {
			return errors.Wrap(err, "create writer")
		}
		defer func() {
			_ = wc.Close()
		}()

		w := yson.NewEncoderWriter(yson.NewWriterFormat(wc, yson.FormatPretty))
		if err := w.Encode(data); err != nil {
			return errors.Wrap(err, "encode config")
		}

		return nil
	}

	var (
		configPath       = cfgDir.Child("config.yson")
		tailerConfigPath = cfgDir.Child("log_tailer_config.yson")

		serverPath     = cfg.ExecutablesDir.Child("ytserver-clickhouse").Child("ytserver-clickhouse")
		trampolinePath = cfg.ExecutablesDir.Child("clickhouse-trampoline").Child("clickhouse-trampoline")
		tailerPath     = cfg.ExecutablesDir.Child("ytserver-log-tailer").Child("ytserver-log-tailer")
	)

	if err := writeConfigFile(ctx, configPath, chCfg); err != nil {
		return errors.Wrap(err, "upload clickhouse config")
	}
	if err := writeConfigFile(ctx, tailerConfigPath, tailerCfg); err != nil {
		return errors.Wrap(err, "upload log tailer config")
	}

	// See https://github.com/ytsaurus/ytsaurus/blob/bc16cd61607dee55a526b8ffcbacaac5b5ad2308/yt/python/yt/clickhouse/spec_builder.py#L138.
	task := UserScript{
		JobCount: cfg.InstanceCount,
		Command:  fmt.Sprintf("%s %s", trampolinePath, serverPath),
		FilePaths: []spec.File{
			// Configs.
			{CypressPath: configPath},
			{CypressPath: tailerConfigPath},
			// Executables.
			{CypressPath: serverPath},
			{CypressPath: trampolinePath},
			{CypressPath: tailerPath},
		},
		MemoryLimit:                   cfg.Limits.MemoryLimit() + cfg.Limits.LogTailer,
		CPULimit:                      float32(cfg.Limits.CPULimit),
		MaxStderrSize:                 1024 * 1024 * 1024,
		PortCount:                     5,
		UserJobMemoryDigestLowerBound: 1.0,
		RestartCompletedJobs:          true,
		InterruptionSignal:            "SIGINT",
	}
	opSpec := VanilaOperation{
		Alias: "*" + cfg.Alias,
		Annotations: map[string]any{
			"is_clique": true,
			"expose":    true,
		},
		Tasks: map[string]UserScript{
			"instances": task,
		},
		MaxStderrCount: 150,
	}

	id, err := yc.StartOperation(ctx, yt.OperationVanilla, opSpec, nil)
	if err != nil {
		return errors.Wrap(err, "start cluster operation")
	}
	zctx.From(ctx).Info("Started CHYT operation", zap.Stringer("operation_id", id))
	return nil
}

func (e *CHYTBinary) createClique(ctx context.Context, controllerAddr, proxyAddr, alias string) error {
	// https://github.com/ytsaurus/yt-k8s-operator/blob/6ba7c6a57253e20dc9dd79ed6c74eabb3c4ea91a/pkg/components/chyt.go#L142-L148
	lg := zctx.From(ctx)
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = time.Minute

	if err := backoff.RetryNotify(func() error {
		return sendCHYTConrollerRequest(ctx, controllerAddr, proxyAddr, "create", map[string]any{
			"params": map[string]any{
				"alias": alias,
			},
		})
	}, b, func(err error, d time.Duration) {
		lg.Error("Retry CHYT clique creation", zap.Error(err), zap.Duration("after", d))
	}); err != nil {
		return errors.Wrap(err, "create CHYT clique")
	}
	lg.Info("Created clique", zap.String("alias", alias))

	for option, value := range map[string]any{
		"enable_geodata": false,
		"instance_cpu":   2,
		"instance_memory": map[string]any{
			"reader":                        100000000,
			"chunk_meta_cache":              100000000,
			"compressed_cache":              100000000,
			"clickhouse":                    100000000,
			"clickhouse_watermark":          10,
			"footprint":                     500000000,
			"log_tailer":                    100000000,
			"watchdog_oom_watermark":        0,
			"watchdog_oom_window_watermark": 0,
		},
		"instance_count": 1,
	} {
		if err := sendCHYTConrollerRequest(ctx, controllerAddr, proxyAddr, "set_option", map[string]any{
			"params": map[string]any{
				"alias": alias,
				"key":   option,
				"value": value,
			},
		}); err != nil {
			return errors.Wrapf(err, "set option %q", option)
		}
	}

	if err := backoff.RetryNotify(func() error {
		return sendCHYTConrollerRequest(ctx, controllerAddr, proxyAddr, "start", map[string]any{
			"params": map[string]any{
				"alias":     alias,
				"untracked": true,
			},
		})
	}, b, func(err error, d time.Duration) {
		lg.Error("Retry CHYT clique start", zap.Error(err), zap.Duration("after", d))
	}); err != nil {
		return errors.Wrap(err, "start CHYT clique")
	}
	lg.Info("Started clique", zap.String("alias", alias))

	return nil
}

func sendCHYTConrollerRequest(ctx context.Context, addr, proxy, command string, input any) error {
	data, err := yson.Marshal(input)
	if err != nil {
		return err
	}

	u, err := url.Parse("http://" + addr)
	if err != nil {
		return err
	}
	u = u.JoinPath(proxy, command)

	zctx.From(ctx).Info("Sending CHYT controller command",
		zap.Stringer("endpoint", u),
		zap.String("command", command),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("X-YT-TestUser", "root")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.Errorf("create clique error: code %d: %s", resp.StatusCode, body)
	}

	return nil
}
