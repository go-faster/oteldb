// Binary ytlocal runs local ytsaurus clusters.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/ytlocal"
)

// DeltaEncoder colorfully encodes delta from start in seconds and milliseconds.
func DeltaEncoder(now time.Time) zapcore.TimeEncoder {
	return func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		duration := t.Sub(now)
		seconds := duration / time.Second
		milliseconds := (duration % time.Second) / time.Millisecond
		secColor := color.New(color.Faint)
		msecColor := color.New(color.FgHiBlack)
		enc.AppendString(secColor.Sprintf("%03d", seconds) + msecColor.Sprintf(".%03d", milliseconds))
	}
}

func main() {
	root := cobra.Command{
		Use:           "ytlocal",
		Short:         "ytlocal runs local ytsaurus cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			pa := ytlocal.PortAllocator{
				Host: "localhost",
				Net:  "tcp",
			}
			port, err := pa.Allocate()
			if err != nil {
				return errors.Wrap(err, "allocate")
			}
			zctx.From(ctx).Info("allocated port", zap.Int("port", port))

			dir, err := os.MkdirTemp("", "ytlocal-*")
			if err != nil {
				return errors.Wrap(err, "mkdir temp")
			}
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			bin, err := ytlocal.NewBinary(dir)
			if err != nil {
				return errors.Wrap(err, "new binary")
			}
			zctx.From(ctx).Info("new binary",
				zap.String("all", bin.All),
				zap.String("master", bin.Master),
			)

			const clusterName = "test"
			cellID := ytlocal.GenerateCellID(1, clusterName)

			masterPort, err := pa.Allocate()
			if err != nil {
				return errors.Wrap(err, "allocate")
			}
			masterMonitoringPort, err := pa.Allocate()
			if err != nil {
				return errors.Wrap(err, "allocate")
			}
			const localhost = "localhost"

			var (
				masterAddr           = fmt.Sprintf("%s:%d", localhost, masterPort)
				cfgTimestampProvider = ytlocal.Connection{
					Addresses:       []string{masterAddr},
					SoftBackoffTime: 100,
					HardBackoffTime: 100,
					UpdatePeriod:    500,
				}
				cfgPrimaryMaster = ytlocal.Connection{
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
				cfgAddressResolver = ytlocal.AddressResolver{
					Retries:       3,
					EnableIPv4:    true,
					EnableIPv6:    false,
					LocalhostFQDN: localhost,
				}
				cfgCellDirectory = ytlocal.CellDirectory{
					SoftBackoffTime:           100,
					HardBackoffTime:           100,
					EnablePeerPolling:         true,
					PeerPollingPeriod:         500,
					PeerPollingPeriodSplay:    100,
					PeerPollingRequestTimeout: 100,
					RediscoverPeriod:          5_000,
					RediscoverSplay:           500,
				}
				cfgDiscoveryConnection = ytlocal.Connection{
					Addresses: []string{masterAddr},
				}
				cfgLogging = ytlocal.Logging{
					AbortOnAlert:           true,
					CompressionThreadCount: 4,
					Writers: map[string]ytlocal.LoggingWriter{
						"stderr": {
							Format:     ytlocal.LogFormatPlainText,
							WriterType: ytlocal.LogWriterTypeStderr,
						},
					},
					Rules: []ytlocal.LoggingRule{
						{
							Writers:  []string{"stderr"},
							MinLevel: ytlocal.LogLevenWarning,
						},
					},
				}
				cfgClusterConnection = ytlocal.ClusterConnection{
					ClusterName:         clusterName,
					CellDirectory:       cfgCellDirectory,
					PrimaryMaster:       cfgPrimaryMaster,
					TimestampProvider:   cfgTimestampProvider,
					DiscoveryConnection: cfgDiscoveryConnection,
				}
				cfgBaseServer = ytlocal.BaseServer{
					RPCPort:           masterPort,
					MonitoringPort:    masterMonitoringPort,
					AddressResolver:   cfgAddressResolver,
					TimestampProvider: cfgTimestampProvider,
					ClusterConnection: cfgClusterConnection,
					Logging:           cfgLogging,
				}
				cfgTCPDispatcher = ytlocal.TCPDispatcher{
					ThreadPoolSize: 2,
				}
				cfgSolomonExporter = ytlocal.SolomonExporter{
					GridStep: 1_000,
				}
				cfgCypressAnnotations = ytlocal.CypressAnnotations{
					YTEnvIndex: 0,
				}
				cfgYPServiceDiscovery = ytlocal.YPServiceDiscovery{
					Enable: false,
				}
				cfgTimestampManager = ytlocal.TimestampManager{
					CommitAdvance:      3_000,
					RequestBackoffTime: 10,
					CalibrationPeriod:  10,
				}
				cfgCypressManager = ytlocal.CypressManager{
					DefaultJournalWriteQuorum:       1,
					DefaultJournalReadQuorum:        1,
					DefaultFileReplicationFactor:    1,
					DefaultJournalReplicationFactor: 1,
					DefaultTableReplicationFactor:   1,
				}
				cfgChunkClientDispatcher = ytlocal.ChunkClientDispatcher{
					ChunkReaderPoolSize: 1,
				}
				cfgRPCDispatcher = ytlocal.RPCDispatcher{
					CompressionPoolSize: 1,
					HeavyPoolSize:       1,
				}
				cfgHiveManager = ytlocal.HiveManager{
					PingPeriod:     1_000,
					IdlePostPeriod: 1_000,
				}
				cfgObjectService = ytlocal.ObjectService{
					EnableLocalReadExecutor: true,
					EnableLocalReadBusyWait: false,
				}
				cfgChunkManager = ytlocal.ChunkManger{
					AllowMultipleErasurePartsPerNode: true,
				}
				cfgDriver = ytlocal.Driver{
					ClusterName:            clusterName,
					EnableInternalCommands: true,
					PrimaryMaster:          cfgPrimaryMaster,
					CellDirectory:          cfgCellDirectory,
					TimestampProvider:      cfgTimestampProvider,
					DiscoveryConnections: []ytlocal.Connection{
						{
							Addresses: []string{masterAddr},
						},
					},
				}
				cfgMaster = ytlocal.Master{
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

					Snapshots: ytlocal.MasterSnapshots{
						Path: filepath.Join(dir, "snapshots"),
					},
					HydraManager: ytlocal.HydraManager{
						SnapshotBackgroundThreadCount: 4,
						LeaderSyncDelay:               0,
						MinimizeCommitLatency:         true,
						LeaderLeaseCheckPeriod:        100,
						LeaderLeaseTimeout:            20_000,
						DisableLeaderLeaseGraceDelay:  true,
						InvariantsCheckProbability:    0.005,
						ResponseKeeper: ytlocal.ResponseKeeper{
							EnableWarmup:   false,
							ExpirationTime: 25_000,
							WarmupTime:     30_000,
						},
						MaxChangelogDataSize: 268435456,
					},
					Changelogs: ytlocal.MasterChangelogs{
						FlushPeriod: 10,
						EnableSync:  false,
						IOEngine: ytlocal.IOEngine{
							EnableSync: false,
						},
						Path: filepath.Join(dir, "changelogs"),
					},
				}
			)
			_ = cfgDriver
			opt := ytlocal.Options{
				Binary: bin,
				Dir:    dir,
			}
			master := ytlocal.NewComponent(opt, cfgMaster)
			zctx.From(ctx).Info("new component",
				zap.String("type", string(master.Type)),
				zap.String("binary", master.Binary),
			)
			if err := master.Run(ctx); err != nil {
				return errors.Wrap(err, "run master")
			}
			return nil
		},
	}
	lgCfg := zap.NewDevelopmentConfig()
	lgCfg.DisableStacktrace = true
	lgCfg.DisableCaller = true
	lgCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	lgCfg.EncoderConfig.EncodeTime = DeltaEncoder(time.Now())
	lg, err := lgCfg.Build()
	if err != nil {
		panic(err)
	}
	ctx := zctx.Base(context.Background(), lg)
	if err := root.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(1)
	}
}
