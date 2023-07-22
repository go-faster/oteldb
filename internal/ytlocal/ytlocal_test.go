package ytlocal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
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

// Component is a helper to run ytserver components.
type Component[T any] struct {
	Name   string
	Binary string
	RunDir string
	Config T
	TB     testing.TB
}

func (c Component[T]) Go(ctx context.Context, g *errgroup.Group) {
	g.Go(func() error {
		return errors.Wrap(c.Run(ctx), c.Name)
	})
}

// Run runs the component.
func (c Component[T]) Run(ctx context.Context) error {
	data, err := yson.Marshal(c.Config)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}

	cfgName := c.Name + ".yson"
	cfgPath := filepath.Join(c.RunDir, cfgName)
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		return errors.Wrap(err, "write config")
	}

	args := []string{
		"--config", cfgPath,
	}

	g, ctx := errgroup.WithContext(ctx)
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	r, w := io.Pipe()
	cmd.Stderr = io.MultiWriter(w, os.Stderr)
	if err := cmd.Start(); err != nil {
		return errors.Wrap(err, "start")
	}
	errFound := errors.New("found")

	g.Go(func() error {
		defer func() { _ = r.Close() }()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "Leader active") {
				c.TB.Log("Leader active")
			}
		}
		return scanner.Err()
	})
	g.Go(func() error {
		defer func() { _ = w.Close() }()
		if err := cmd.Wait(); err != nil {
			if errors.Is(context.Cause(ctx), errFound) {
				return nil
			}
			return err
		}
		return nil
	})

	return g.Wait()
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

	// Search for ytserver-all in $PATH.
	singleBinary := "ytserver-all"
	singleBinaryPath, err := exec.LookPath(singleBinary)
	if err != nil {
		t.Fatalf("Binary %q not found in $PATH", singleBinary)
	}

	// Ensure that all binaries are available.
	//
	// See TryProgram here for list:
	// https://github.com/ytsaurus/ytsaurus/blob/d8cc9c52b6fd94b352a4264579dd89a75aae9b38/yt/yt/server/all/main.cpp#L49-L74
	//
	// If not available, create a symlink to ytserver-all.
	runDir := t.TempDir()
	binaries := map[string]string{}
	for _, name := range []string{
		"master",
		"clock",
		"http-proxy",
		"node",
		"job-proxy",
		"exec",
		"tools",
		"scheduler",
		"controller-agent",
		"log-tailer",
		"discovery",
		"timestamp-provider",
		"master-cache",
		"cell-balancer",
		"queue-agent",
		"tablet-balancer",
		"cypress-proxy",
		"query-tracker",
		"tcp-proxy",
	} {
		binaryPath := "ytserver-" + name
		p, err := exec.LookPath(binaryPath)
		if err == nil {
			binaries[name] = p
			continue
		}
		// Create link.
		binaryPath = filepath.Join(runDir, binaryPath)
		if err := os.Symlink(singleBinaryPath, binaryPath); err != nil {
			t.Fatalf("failed to create link: %v", err)
		}
		binaries[name] = binaryPath
	}

	const clusterName = "test"
	cellID := GenerateCellID(1, clusterName)
	var (
		masterPort           = ports.RequireAllocate(t)
		masterMonitoringPort = ports.RequireAllocate(t)
		masterAddr           = fmt.Sprintf("%s:%d", localhost, masterPort)
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
					MinLevel: LogLevelInfo,
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
		baseServer = baseServerFactory(cfgBaseServer, ports)
	)
	master := Component[Master]{
		TB:     t,
		RunDir: runDir,
		Name:   "master",
		Binary: binaries["master"],
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
	scheduler := Component[Scheduler]{
		TB:     t,
		RunDir: runDir,
		Name:   "scheduler",
		Binary: binaries["scheduler"],
		Config: Scheduler{
			BaseServer: baseServer(t),
		},
	}
	controllerAgent := Component[ControllerAgent]{
		TB:     t,
		RunDir: runDir,
		Name:   "controller-agent",
		Binary: binaries["controller-agent"],
		Config: ControllerAgent{
			BaseServer: baseServer(t),
			Options: ControllerAgentOptions{
				EnableTMPFS: true,
			},
		},
	}
	dataNode := Component[DataNode]{
		Name:   "data-node",
		Binary: binaries["node"],
		RunDir: runDir,
		TB:     t,
		Config: DataNode{
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

	g, ctx := errgroup.WithContext(context.Background())
	master.Go(ctx, g)
	scheduler.Go(ctx, g)
	controllerAgent.Go(ctx, g)
	dataNode.Go(ctx, g)
	require.NoError(t, g.Wait())
}
