// Binary ytlocal runs local ytsaurus clusters.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/yson"
	"golang.org/x/sync/errgroup"

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
		enc.AppendString(secColor.Sprintf("%03d", seconds) + msecColor.Sprintf(".%02d", milliseconds/10))
	}
}

// initBaseConfigs allocates ports and fills base configs.
func initBaseConfigs(pa *ytlocal.PortAllocator, base ytlocal.BaseServer, targets ...*ytlocal.BaseServer) error {
	for _, target := range targets {
		port, err := pa.Allocate()
		if err != nil {
			return errors.Wrap(err, "allocate")
		}
		rpcPort, err := pa.Allocate()
		if err != nil {
			return errors.Wrap(err, "allocate")
		}
		b := ytlocal.BaseServer{
			RPCPort:           port,
			MonitoringPort:    rpcPort,
			Logging:           base.Logging,
			AddressResolver:   base.AddressResolver,
			TimestampProvider: base.TimestampProvider,
			ClusterConnection: base.ClusterConnection,
		}
		*target = b
	}
	return nil
}

// ColorLevelEncoder is single-character color encoder for zapcore.Level.
func ColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString(color.New(color.FgCyan).Sprint("D"))
	case zapcore.InfoLevel:
		enc.AppendString(color.New(color.FgBlue).Sprint("I"))
	case zapcore.WarnLevel:
		enc.AppendString(color.New(color.FgYellow).Sprint("W"))
	case zapcore.ErrorLevel:
		enc.AppendString(color.New(color.FgRed).Sprint("E"))
	default:
		enc.AppendString("U")
	}
}

func main() {
	var arg struct {
		ProxyPort int
		Clean     bool
	}
	root := cobra.Command{
		Use:           "ytlocal",
		Short:         "ytlocal runs local ytsaurus cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			pa := &ytlocal.PortAllocator{
				Host: "localhost",
				Net:  "tcp",
			}
			dir, err := os.MkdirTemp("", "ytlocal-*")
			if err != nil {
				return errors.Wrap(err, "mkdir temp")
			}
			defer func() {
				zctx.From(ctx).Info("Stopped",
					zap.String("dir", dir),
					zap.Bool("clean", arg.Clean),
				)
			}()
			if arg.Clean {
				defer func() { _ = os.RemoveAll(dir) }()
			}
			bin, err := ytlocal.NewBinary(dir)
			if err != nil {
				return errors.Wrap(err, "new binary")
			}
			zctx.From(ctx).Info("Using binaries",
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
				cfgScheduler       = ytlocal.Scheduler{}
				cfgControllerAgent = ytlocal.ControllerAgent{
					Options: ytlocal.ControllerAgentOptions{
						EnableTMPFS: true,
					},
				}
				cfgNode = ytlocal.Node{
					ResourceLimits: ytlocal.ResourceLimits{
						TotalCPU:    1,
						TotalMemory: 1024 * 1024 * 512,
					},
					Options: ytlocal.DataNodeOptions{
						StoreLocations: []ytlocal.StoreLocation{
							{
								Path:       filepath.Join(dir, "chunk_store"),
								MediumName: "default",
							},
						},
					},
				}
				cfgHTTPProxy = ytlocal.HTTPProxy{
					Port: arg.ProxyPort,
					Coordinator: ytlocal.Coordinator{
						Enable:     true,
						Announce:   true,
						ShowPorts:  true,
						PublicFQDN: net.JoinHostPort(localhost, strconv.Itoa(arg.ProxyPort)),
					},
					Driver: cfgDriver,
				}
			)
			// Allocate ports and fill base config for all components, using
			// cfgBaseServer as a template.
			if err := initBaseConfigs(pa, cfgBaseServer,
				&cfgScheduler.BaseServer,
				&cfgControllerAgent.BaseServer,
				&cfgNode.BaseServer,
				&cfgHTTPProxy.BaseServer,
			); err != nil {
				return errors.Wrap(err, "init base configs")
			}
			var (
				opt       = ytlocal.Options{Binary: bin, Dir: dir}
				master    = ytlocal.NewComponent(opt, cfgMaster)
				scheduler = ytlocal.NewComponent(opt, cfgScheduler)
				agent     = ytlocal.NewComponent(opt, cfgControllerAgent)
				node      = ytlocal.NewComponent(opt, cfgNode)
				proxy     = ytlocal.NewComponent(opt, cfgHTTPProxy)
			)
			zctx.From(ctx).Info("Starting cluster",
				zap.Int("master.rpc_port", master.Config.RPCPort),
				zap.Int("master.monitoring_port", master.Config.MonitoringPort),
			)
			g, ctx := errgroup.WithContext(ctx)
			ytlocal.Go(ctx, g,
				master,
				scheduler,
				agent,
				node,
				proxy,
			)
			return g.Wait()
		},
	}

	{
		f := root.Flags()
		f.IntVar(&arg.ProxyPort, "port", 8080, "HTTP proxy port")
		f.BoolVar(&arg.Clean, "clean", true, "Clean temporary directory")
	}
	{
		python := &cobra.Command{
			Use:   "python",
			Short: "Run python yt_local mode",
			RunE: func(cmd *cobra.Command, args []string) error {
				// Generate new temporary directory.
				dir, err := os.MkdirTemp("", "ytlocal-python-*")
				if err != nil {
					return errors.Wrap(err, "mkdir temp")
				}
				const allBinary = "ytserver-all"
				allPath, err := exec.LookPath(allBinary)
				if err != nil {
					return errors.Wrap(err, "look path")
				}
				// Generate resolver configuration and save it to file.
				data, err := yson.Marshal(ytlocal.AddressResolver{
					EnableIPv4: true,
					EnableIPv6: false,
				})
				if err != nil {
					return errors.Wrap(err, "marshal")
				}
				cfgResolverPath := filepath.Join(dir, "resolver.yson")
				// #nosec G306
				if err := os.WriteFile(cfgResolverPath, data, 0644); err != nil {
					return errors.Wrap(err, "write file")
				}

				ctx := cmd.Context()
				// #nosec: G204
				c := exec.CommandContext(ctx, "yt_local", "start",
					"--proxy-por", "8080",
					"--master-config", cfgResolverPath,
					"--node-config", cfgResolverPath,
					"--scheduler-config", cfgResolverPath,
					"--controller-agent-config", cfgResolverPath,
					"--rpc-proxy-config", cfgResolverPath,
					"--local-cypress-dir", cfgResolverPath,
					"--fqdn", "localhost",
					"--ytserver-all-path", allPath,
					"--sync",
				)
				c.Stderr = os.Stderr
				c.Stdout = os.Stdout
				c.Dir = dir

				return c.Run()
			},
		}
		root.AddCommand(python)
	}

	lgCfg := zap.NewDevelopmentConfig()
	lgCfg.DisableStacktrace = true
	lgCfg.DisableCaller = true
	lgCfg.EncoderConfig.EncodeLevel = ColorLevelEncoder
	lgCfg.EncoderConfig.EncodeTime = DeltaEncoder(time.Now())
	lgCfg.EncoderConfig.ConsoleSeparator = " "
	lgCfg.EncoderConfig.EncodeName = func(s string, encoder zapcore.PrimitiveArrayEncoder) {
		name := s
		const maxChars = 6
		if len(name) > maxChars {
			name = name[:maxChars]
		}
		format := "%-" + strconv.Itoa(maxChars) + "s"
		encoder.AppendString(color.New(color.FgHiBlue).Sprintf(format, name))
	}
	lg, err := lgCfg.Build()
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := root.ExecuteContext(zctx.Base(ctx, lg)); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(1)
	}
}
