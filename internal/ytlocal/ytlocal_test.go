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
	"time"

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

	// Try running master.
	masterPort := ports.RequireAllocate(t)
	masterMonitoringPort := ports.RequireAllocate(t)
	const cellID = "a3c51a55-ffffffff-259-ffffffff"

	masterAddr := fmt.Sprintf("%s:%d", localhost, masterPort)
	cfg := Master{
		BaseServer: BaseServer{
			RPCPort:        masterPort,
			MonitoringPort: masterMonitoringPort,
			AddressResolver: AddressResolver{
				Retries:       3,
				EnableIPv4:    true,
				EnableIPv6:    false,
				LocalhostFQDN: localhost,
			},
			TimestampProvider: Connection{
				Addresses:       []string{masterAddr},
				SoftBackoffTime: 100,
				HardBackoffTime: 100,
				UpdatePeriod:    500,
			},
			ClusterConnection: ClusterConnection{
				ClusterName: "test",
				CellDirectory: CellDirectory{
					SoftBackoffTime:           100,
					HardBackoffTime:           100,
					EnablePeerPolling:         true,
					PeerPollingPeriod:         500,
					PeerPollingPeriodSplay:    100,
					PeerPollingRequestTimeout: 100,
					RediscoverPeriod:          5_000,
					RediscoverSplay:           500,
				},
				DiscoveryConnection: Connection{
					Addresses: []string{masterAddr},
				},
				TimestampProvider: Connection{
					Addresses:       []string{masterAddr},
					SoftBackoffTime: 100,
					HardBackoffTime: 100,
					UpdatePeriod:    500,
				},
				PrimaryMaster: Connection{
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
				},
			},
			Logging: Logging{
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
			},
		},
		EnableProvisionLock: false,
		UseNewHydra:         true,
		ChunkManger: ChunkManger{
			AllowMultipleErasurePartsPerNode: true,
		},
		Changelogs: MasterChangelogs{
			FlushPeriod: 10,
			EnableSync:  false,
			IOEngine: IOEngine{
				EnableSync: false,
			},
			Path: filepath.Join(runDir, "changelogs"),
		},
		ObjectService: ObjectService{
			EnableLocalReadExecutor: true,
			EnableLocalReadBusyWait: false,
		},
		EnableTimestampManager: true,
		TimestampManager: TimestampManager{
			CommitAdvance:      3_000,
			RequestBackoffTime: 10,
			CalibrationPeriod:  10,
		},
		HiveManager: HiveManager{
			PingPeriod:     1_000,
			IdlePostPeriod: 1_000,
		},
		PrimaryMaster: Connection{
			Addresses: []string{masterAddr},
			CellID:    cellID,
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
		YPServiceDiscovery: YPServiceDiscovery{
			Enable: false,
		},
		RPCDispatcher: RPCDispatcher{
			CompressionPoolSize: 1,
			HeavyPoolSize:       1,
		},
		ChunkClientDispatcher: ChunkClientDispatcher{
			ChunkReaderPoolSize: 1,
		},
		TCPDispatcher: TCPDispatcher{
			ThreadPoolSize: 4,
		},
		SolomonExporter: SolomonExporter{
			GridStep: 1_000,
		},
		CypressAnnotations: CypressAnnotations{
			YTEnvIndex: 0,
		},
		CypressManager: CypressManager{
			DefaultJournalWriteQuorum:       1,
			DefaultJournalReadQuorum:        1,
			DefaultFileReplicationFactor:    1,
			DefaultJournalReplicationFactor: 1,
			DefaultTableReplicationFactor:   1,
		},
		EnableRefCountedTrackerProfiling: false,
		EnableResourceTracker:            false,
		Snapshots: MasterSnapshots{
			Path: filepath.Join(runDir, "snapshots"),
		},
	}
	data, err := yson.Marshal(cfg)
	require.NoError(t, err)

	cfgPath := filepath.Join(runDir, "master.yson")
	require.NoError(t, os.WriteFile(cfgPath, data, 0644))

	args := []string{
		"--config", cfgPath,
	}

	ctx, timeoutCancel := context.WithTimeout(interrupted(), time.Second*10)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(ctx)
	defer func() {
		cancel(context.Canceled)
	}()

	g, ctx := errgroup.WithContext(ctx)
	cmd := exec.CommandContext(ctx, binaries["master"], args...)
	r, w := io.Pipe()
	cmd.Stderr = w
	require.NoError(t, cmd.Start())
	errFound := errors.New("found")

	g.Go(func() error {
		defer func() { _ = r.Close() }()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "Leader active") {
				t.Log("Leader active")
				cancel(errFound)
				return nil
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

	require.NoError(t, g.Wait())
}
