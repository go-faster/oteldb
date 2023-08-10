package ytlocal

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"golang.org/x/sync/errgroup"
)

// RequireAllocate is helper for Allocate().
func (p *PortAllocator) RequireAllocate(tb testing.TB) int {
	tb.Helper()
	n, err := p.Allocate()
	require.NoError(tb, err)
	return n
}

func baseServerFactory(tpl BaseServer, ports *PortAllocator) func(tb testing.TB) BaseServer {
	return func(tb testing.TB) BaseServer {
		tb.Helper()
		return BaseServer{
			RPCPort:        ports.RequireAllocate(tb),
			MonitoringPort: ports.RequireAllocate(tb),

			TimestampProvider: tpl.TimestampProvider,
			ClusterConnection: tpl.ClusterConnection,
			Logging:           tpl.Logging,
			AddressResolver:   tpl.AddressResolver,
		}
	}
}

// TestComponent is a helper to run ytserver components.
//
// TODO: Replace with Server.
type TestComponent[T any] struct {
	Name    string
	Binary  string
	RunPath string
	RunDir  string
	Config  T
	TB      testing.TB
}

func (c TestComponent[T]) Go(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error {
		return errors.Wrap(c.Run(ctx), c.Name)
	})
}

// Run runs the component.
func (c TestComponent[T]) Run(ctx context.Context) error {
	data, err := yson.Marshal(c.Config)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}

	cfgName := c.Name + ".yson"
	cfgPath := filepath.Join(c.RunDir, cfgName)
	if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
		return errors.Wrap(err, "write config")
	}

	args := []string{
		"--config", cfgPath,
	}

	g, ctx := errgroup.WithContext(ctx)
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	r, w := io.Pipe()
	cmd.Stderr = w
	cmd.Env = []string{
		"PATH", os.Getenv("PATH") + ":" + c.RunPath,
	}

	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "start")
	}
	g.Go(func() error {
		defer func() { _ = r.Close() }()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "Leader active") {
				c.TB.Log("Leader active")
			}
			text := scanner.Text()
			firstTab := strings.IndexByte(text, '\t')
			if firstTab != -1 {
				// Trim timestamp.
				text = text[firstTab+1:]
			}
			c.TB.Logf("%-16s %s", c.Name, strings.TrimSpace(text))
		}
		return scanner.Err()
	})
	g.Go(func() error {
		defer func() { _ = w.Close() }()
		return cmd.Wait()
	})
	return g.Wait()
}

type ConsoleClient struct {
	binary    string
	cfgPath   string
	proxyAddr string
	token     string
	tb        testing.TB
}

type Params map[string]any

func (c *ConsoleClient) Run(ctx context.Context, args ...any) error {
	var strArgs []string
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			strArgs = append(strArgs, v)
		case Params:
			// Handle convenience helper for params.
			strArgs = append(strArgs, encodeParams(v))
		default:
			panic(fmt.Sprintf("unknown type: %T", arg))
		}
	}

	// Setup command.
	cmd := exec.CommandContext(ctx, c.binary, strArgs...)
	// Setup cluster config.
	env := cmd.Environ()
	for k, v := range map[string]string{
		"YT_PROXY": c.proxyAddr,
		"YT_TOKEN": c.token,
	} {
		if v == "" {
			continue
		}
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.tb.Logf("cli(%s): %s", strings.Join(strArgs, " "), string(out))
		_, _ = fmt.Fprintln(os.Stderr, string(out))
		return errors.Wrap(err, "run")
	}
	if output := strings.TrimSpace(string(out)); output != "" {
		c.tb.Logf("cli(%s): %s", strings.Join(strArgs, " "), output)
	} else {
		c.tb.Logf("cli(%s): ok", strings.Join(strArgs, " "))
	}
	return nil
}

func encodeParams(param map[string]any) string {
	data, err := yson.Marshal(param)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestRun(t *testing.T) {
	if ok, _ := strconv.ParseBool(os.Getenv("YT_LOCAL_TEST")); !ok {
		t.Skip("Set YT_LOCAL_TEST=1")
	}

	const localhost = "localhost"
	ports := &PortAllocator{
		Host: localhost,
		Net:  "tcp4",
	}

	// Setup client.
	const clientBinary = "yt"
	clientBinaryPath, err := exec.LookPath(clientBinary)
	if err != nil {
		t.Fatalf("Binary %q not found in $PATH", clientBinary)
	}
	t.Logf("Client binary: %s", clientBinaryPath)

	runDir := t.TempDir()

	// Ensure binaries.
	runPath := filepath.Join(runDir, "bin")
	require.NoError(t, os.MkdirAll(runPath, 0o755))
	bin, err := NewBinary(runPath)
	require.NoError(t, err)

	const clusterName = "test"
	cellID := GenerateCellID(1, clusterName)
	var (
		masterPort           = ports.RequireAllocate(t)
		masterMonitoringPort = ports.RequireAllocate(t)
		masterAddr           = net.JoinHostPort(localhost, strconv.Itoa(masterPort))
		cfgTimestampProvider = Connection{
			Addresses:       []string{masterAddr},
			SoftBackoffTime: 100,
			HardBackoffTime: 100,
			UpdatePeriod:    500,
		}
		cfgPrimaryMaster = Connection{
			Addresses:                 []string{masterAddr},
			CellID:                    cellID,
			RetryBackoffTime:          100,
			RetryAttempts:             100,
			RPCTimeout:                25_000,
			SoftBackoffTime:           100,
			HardBackoffTime:           100,
			EnablePeerPolling:         true,
			PeerPollingPeriod:         500,
			PeerPollingPeriodSplay:    100,
			PeerPollingRequestTimeout: 100,
			RediscoverPeriod:          5_000,
			RediscoverSplay:           500,
		}
		cfgAddressResolver = AddressResolver{
			Retries:       3,
			EnableIPv4:    true,
			EnableIPv6:    false,
			LocalhostFQDN: localhost,
		}
		cfgCellDirectory = CellDirectory{
			SoftBackoffTime:           100,
			HardBackoffTime:           100,
			EnablePeerPolling:         true,
			PeerPollingPeriod:         500,
			PeerPollingPeriodSplay:    100,
			PeerPollingRequestTimeout: 100,
			RediscoverPeriod:          5_000,
			RediscoverSplay:           500,
		}
		cfgDiscoveryConnection = Connection{
			Addresses: []string{masterAddr},
		}
		cfgLogging = Logging{
			AbortOnAlert:           true,
			CompressionThreadCount: 4,
			Writers: map[string]LoggingWriter{
				"stderr": {
					Format:     LogFormatPlainText,
					WriterType: LogWriterTypeStderr,
				},
			},
			Rules: []LoggingRule{
				{
					Writers:  []string{"stderr"},
					MinLevel: LogLevenWarning,
				},
			},
		}
		cfgClusterConnection = ClusterConnection{
			ClusterName:         clusterName,
			CellDirectory:       cfgCellDirectory,
			PrimaryMaster:       cfgPrimaryMaster,
			TimestampProvider:   cfgTimestampProvider,
			DiscoveryConnection: cfgDiscoveryConnection,
		}
		cfgBaseServer = BaseServer{
			RPCPort:           masterPort,
			MonitoringPort:    masterMonitoringPort,
			AddressResolver:   cfgAddressResolver,
			TimestampProvider: cfgTimestampProvider,
			ClusterConnection: cfgClusterConnection,
			Logging:           cfgLogging,
		}
		cfgTCPDispatcher = TCPDispatcher{
			ThreadPoolSize: 2,
		}
		cfgSolomonExporter = SolomonExporter{
			GridStep: 1_000,
		}
		cfgCypressAnnotations = CypressAnnotations{
			YTEnvIndex: 0,
		}
		cfgYPServiceDiscovery = YPServiceDiscovery{
			Enable: false,
		}
		cfgTimestampManager = TimestampManager{
			CommitAdvance:      3_000,
			RequestBackoffTime: 10,
			CalibrationPeriod:  10,
		}
		cfgCypressManager = CypressManager{
			DefaultJournalWriteQuorum:       1,
			DefaultJournalReadQuorum:        1,
			DefaultFileReplicationFactor:    1,
			DefaultJournalReplicationFactor: 1,
			DefaultTableReplicationFactor:   1,
		}
		cfgChunkClientDispatcher = ChunkClientDispatcher{
			ChunkReaderPoolSize: 1,
		}
		cfgRPCDispatcher = RPCDispatcher{
			CompressionPoolSize: 1,
			HeavyPoolSize:       1,
		}
		cfgHiveManager = HiveManager{
			PingPeriod:     1_000,
			IdlePostPeriod: 1_000,
		}
		cfgObjectService = ObjectService{
			EnableLocalReadExecutor: true,
			EnableLocalReadBusyWait: false,
		}
		cfgChunkManager = ChunkManger{
			AllowMultipleErasurePartsPerNode: true,
		}
		cfgDriver = Driver{
			ClusterName:            clusterName,
			EnableInternalCommands: true,
			PrimaryMaster:          cfgPrimaryMaster,
			CellDirectory:          cfgCellDirectory,
			TimestampProvider:      cfgTimestampProvider,
			DiscoveryConnections: []Connection{
				{
					Addresses: []string{masterAddr},
				},
			},
		}
		baseServer = baseServerFactory(cfgBaseServer, ports)
	)
	master := TestComponent[Master]{
		TB:      t,
		RunDir:  runDir,
		RunPath: runPath,
		Name:    "master",
		Binary:  bin.Master,
		Config: Master{
			BaseServer: cfgBaseServer,

			ChunkManger:           cfgChunkManager,
			ObjectService:         cfgObjectService,
			TimestampManager:      cfgTimestampManager,
			HiveManager:           cfgHiveManager,
			PrimaryMaster:         cfgPrimaryMaster,
			YPServiceDiscovery:    cfgYPServiceDiscovery,
			RPCDispatcher:         cfgRPCDispatcher,
			ChunkClientDispatcher: cfgChunkClientDispatcher,
			TCPDispatcher:         cfgTCPDispatcher,
			SolomonExporter:       cfgSolomonExporter,
			CypressAnnotations:    cfgCypressAnnotations,
			CypressManager:        cfgCypressManager,

			EnableRefCountedTrackerProfiling: false,
			EnableResourceTracker:            false,
			EnableProvisionLock:              false,
			UseNewHydra:                      true,
			EnableTimestampManager:           true,

			Snapshots: MasterSnapshots{
				Path: filepath.Join(runDir, "snapshots"),
			},
			HydraManager: HydraManager{
				SnapshotBackgroundThreadCount: 4,
				LeaderSyncDelay:               0,
				MinimizeCommitLatency:         true,
				LeaderLeaseCheckPeriod:        100,
				LeaderLeaseTimeout:            20_000,
				DisableLeaderLeaseGraceDelay:  true,
				InvariantsCheckProbability:    0.005,
				ResponseKeeper: ResponseKeeper{
					EnableWarmup:   false,
					ExpirationTime: 25_000,
					WarmupTime:     30_000,
				},
				MaxChangelogDataSize: 268435456,
			},
			Changelogs: MasterChangelogs{
				FlushPeriod: 10,
				EnableSync:  false,
				IOEngine: IOEngine{
					EnableSync: false,
				},
				Path: filepath.Join(runDir, "changelogs"),
			},
		},
	}
	scheduler := TestComponent[Scheduler]{
		TB:      t,
		RunDir:  runDir,
		RunPath: runPath,
		Name:    "scheduler",
		Binary:  bin.Scheduler,
		Config: Scheduler{
			BaseServer: baseServer(t),
		},
	}
	controllerAgent := TestComponent[ControllerAgent]{
		TB:      t,
		RunDir:  runDir,
		RunPath: runPath,
		Name:    "controller-agent",
		Binary:  bin.ControllerAgent,
		Config: ControllerAgent{
			BaseServer: baseServer(t),
			Options: ControllerAgentOptions{
				EnableTMPFS: true,
			},
		},
	}
	node := TestComponent[Node]{
		Name:    "data-node",
		Binary:  bin.Node,
		RunDir:  runDir,
		RunPath: runPath,
		TB:      t,
		Config: Node{
			BaseServer: baseServer(t),
			ResourceLimits: ResourceLimits{
				TotalCPU:    1,
				TotalMemory: 1024 * 1024 * 512,
			},
			Options: DataNodeOptions{
				StoreLocations: []StoreLocation{
					{
						Path:          filepath.Join(runDir, "chunk_store"),
						LowWatermark:  0,
						HighWatermark: 0,
						MediumName:    "default",
					},
				},
			},
		},
	}
	httpPort := 8080
	httpProxy := TestComponent[HTTPProxy]{
		TB:      t,
		RunDir:  runDir,
		RunPath: runPath,
		Name:    "http-proxy",
		Binary:  bin.HTTPProxy,
		Config: HTTPProxy{
			BaseServer: baseServer(t),
			Port:       httpPort,
			Coordinator: Coordinator{
				Enable:     true,
				Announce:   true,
				ShowPorts:  true,
				PublicFQDN: net.JoinHostPort(localhost, strconv.Itoa(httpPort)),
			},
			Driver: cfgDriver,
		},
	}

	// Setup client.
	proxyAddr := net.JoinHostPort(localhost, strconv.Itoa(httpPort))
	cc := &ConsoleClient{
		binary:    clientBinaryPath,
		proxyAddr: proxyAddr,
		tb:        t,
	}

	g, ctx := errgroup.WithContext(context.Background())
	master.Go(ctx, g)
	scheduler.Go(ctx, g)
	controllerAgent.Go(ctx, g)
	node.Go(ctx, g)
	httpProxy.Go(ctx, g)

	errDone := errors.New("found node")
	g.Go(func() error {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		client, err := ythttp.NewClient(&yt.Config{
			Proxy: proxyAddr,
		})
		if err != nil {
			return errors.Wrap(err, "create client")
		}

	Poll:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				ok, err := client.NodeExists(ctx, ypath.Path("//sys"), &yt.NodeExistsOptions{})
				if err != nil {
					t.Log("NodeExists failed:", err)
				} else {
					t.Log("NodeExists:", ok)
					break Poll
				}
			}
		}

		// Initialize world.
		//
		// See https://github.com/ytsaurus/ytsaurus/blob/main/yt/python/yt/environment/init_cluster.py#L128 for reference,
		// the `initialize_world` function.

		sys := ypath.Root.Child("sys")

		// yt remove //sys/@provision_lock -f
		if err := client.RemoveNode(ctx, sys.Attr("provision_lock"), &yt.RemoveNodeOptions{Force: true}); err != nil {
			return errors.Wrap(err, "provision lock")
		}
		if _, err := client.CreateObject(ctx, yt.NodeSchedulerPoolTree, &yt.CreateObjectOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"name": "default",
				"config": map[string]any{
					"nodes_filter": "",
				},
			},
		}); err != nil {
			return errors.Wrap(err, "create scheduler pool")
		}
		if err := client.SetNode(ctx, sys.Child("pool_trees").Attr("default_tree"), "default", &yt.SetNodeOptions{}); err != nil {
			return errors.Wrap(err, "set node")
		}
		if _, err := client.CreateNode(ctx, ypath.Root.Child("home"), yt.NodeMap, &yt.CreateNodeOptions{IgnoreExisting: true}); err != nil {
			return errors.Wrap(err, "create node")
		}
		adminPassword := "admin"
		adminPasswordSHA256 := fmt.Sprintf("%x", sha256.Sum256([]byte(adminPassword)))
		if _, err := client.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": "admin",
			},
		}); err != nil {
			return errors.Wrap(err, "create user")
		}

		const user = "admin"
		if err := cc.Run(ctx, "execute", "set_user_password", Params{
			"user":                user,
			"new_password_sha256": adminPasswordSHA256,
		}); err != nil {
			return errors.Wrap(err, "set user password")
		}
		if _, err := client.CreateNode(ctx, sys.Child("cypress_tokens").Child(adminPasswordSHA256), yt.NodeMap, &yt.CreateNodeOptions{
			IgnoreExisting: true,
			Attributes: map[string]any{
				"user": user,
			},
		}); err != nil {
			return errors.Wrap(err, "create cypress token")
		}

		if err := cc.Run(ctx, "add-member", user, "superusers"); err != nil {
			return errors.Wrap(err, "add member")
		}

		t.Log("Setting up users and groups")
		for _, u := range []string{
			"odin", "cron", "cron_merge", "cron_compression", "cron_operations", "cron_tmp",
			"nightly_tester", "robot-yt-mon", "transfer_manager", "fennel", "robot-yt-idm",
			"robot-yt-hermes",
		} {
			if _, err := client.CreateObject(ctx, yt.NodeUser, &yt.CreateObjectOptions{
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
			if _, err := client.CreateObject(ctx, yt.NodeGroup, &yt.CreateObjectOptions{
				IgnoreExisting: true,
				Attributes: map[string]any{
					"name": group,
				},
			}); err != nil {
				return errors.Wrap(err, "create group")
			}
		}

		t.Log("Setting up cron")
		for _, cronUser := range []string{
			"cron", "cron_merge", "cron_compression", "cron_operations", "cron_tmp",
		} {
			if err := cc.Run(ctx, "add-member", cronUser, "superusers"); err != nil {
				return errors.Wrap(err, "add member")
			}
			const qSize = 500
			qSizePath := sys.
				Child("users").
				Child(cronUser).
				Attr("request_queue_size_limit")
			if err := client.SetNode(ctx, qSizePath, qSize, &yt.SetNodeOptions{}); err != nil {
				return errors.Wrap(err, "set req queue")
			}
		}
		if _, err := client.CreateNode(ctx, sys.Child("cron"), yt.NodeMap, &yt.CreateNodeOptions{
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
			if err := cc.Run(ctx, "add-member", add.What, add.To); err != nil {
				return errors.Wrap(err, "add member")
			}
		}
		for _, dir := range []ypath.Path{
			sys, sys.Child("tokens"), "//tmp",
		} {
			if err := client.SetNode(ctx, dir.Attr("opaque"), true, &yt.SetNodeOptions{}); err != nil {
				return errors.Wrap(err, "set opaque")
			}
		}

		if err := client.SetNode(ctx, sys.Attr("inherit_acl"), false, &yt.SetNodeOptions{}); err != nil {
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
			if err := client.SetNode(ctx, acl.Path.Attr("acl").Child("end"), acl.ACL, &yt.SetNodeOptions{}); err != nil {
				return errors.Wrap(err, "set opaque")
			}
		}
		for _, p := range []ypath.Path{
			sys, sys.Child("tokens"), sys.Child("tablet_cells"),
		} {
			if err := client.SetNode(ctx, p.Attr("inherit_acl"), false, &yt.SetNodeOptions{}); err != nil {
				return errors.Wrap(err, "set opaque")
			}
		}

		return errDone
	})
	require.ErrorIs(t, g.Wait(), errDone)
}
