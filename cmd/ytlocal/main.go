// Binary ytlocal runs local ytsaurus clusters.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/go-faster/tcpproxy"
	"github.com/spf13/cobra"
	"github.com/testcontainers/testcontainers-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
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

// zapErr is non-verbose zap field for error.
func zapErr(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("err", err.Error())
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
		skynetPort, err := pa.Allocate()
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
			// Every node would try to bind 10080 port, set some unused port to avoid
			// "already used" errors.
			SkynetHTTPPort: skynetPort,
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

const alphabet = "1234567890abcdefghijklmnpqrstuvwxyz"

// GenerateID generates a random ID with the given length from alphabet.
func GenerateID(reader io.Reader, length int) string {
	// Generate a random ID with the given length from alphabet.
	// The alphabet is chosen to avoid characters that can be confused
	// with each other, such as 0/O and 1/I/l.

	buf := make([]byte, length)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		panic(err)
	}

	for i, b := range buf {
		buf[i] = alphabet[b%byte(len(alphabet))]
	}
	return string(buf)
}

func main() {
	var arg struct {
		ProxyPort    int
		FrontendPort int
		Clean        bool
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

			chytExe, err := ytlocal.NewCHYTBinary(dir)
			if err != nil {
				return errors.Wrap(err, "new CHYT binary")
			}
			zctx.From(ctx).Info("Using CHYT binaries",
				zap.String("controller", chytExe.Controller),
				zap.String("trampoline", chytExe.Trampoline),
				zap.String("tailer", chytExe.Tailer),
				zap.String("server", chytExe.Server),
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
			chytControllerPort, err := pa.Allocate()
			if err != nil {
				return errors.Wrap(err, "allocate")
			}

			const localhost = "localhost"
			var (
				masterAddr         = net.JoinHostPort(localhost, strconv.Itoa(masterPort))
				httpProxyAddr      = net.JoinHostPort(localhost, strconv.Itoa(arg.ProxyPort))
				chytControllerAddr = net.JoinHostPort(localhost, strconv.Itoa(chytControllerPort))
			)

			zctx.From(ctx).Info(
				"Using addrs",
				zap.String("master", masterAddr),
				zap.String("http_proxy", httpProxyAddr),
				zap.String("chyt_controller", chytControllerAddr),
			)

			var (
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
						cfgDiscoveryConnection,
					},
				}
				cfgMaster = ytlocal.Master{
					BaseServer: cfgBaseServer,

					ChunkManger:        cfgChunkManager,
					ObjectService:      cfgObjectService,
					TimestampManager:   cfgTimestampManager,
					HiveManager:        cfgHiveManager,
					PrimaryMaster:      cfgPrimaryMaster,
					YPServiceDiscovery: cfgYPServiceDiscovery,
					DiscoveryServer: ytlocal.DiscoveryConfig{
						Addresses: cfgDiscoveryConnection.Addresses,
					},
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

				execTotalCPU = 4
				cfgExecNode  = ytlocal.ExecNode{
					ExecAgent: ytlocal.ExecAgent{
						SlotManager: ytlocal.SlotManager{
							JobEnvironment: ytlocal.JobEnvironment{
								StartUID: 19500,
								Type:     ytlocal.JobEnvironmentTypeSimple,
							},
							Locations: []ytlocal.SlotLocation{
								{Path: filepath.Join(dir, "slots")},
							},
						},
						JobController: ytlocal.JobController{
							ResourceLimits: ytlocal.JobControllerResourceLimits{
								UserSlots: execTotalCPU * 2,
							},
						},
					},
					ResourceLimits: ytlocal.ResourceLimits{
						TotalCPU:    float64(execTotalCPU),
						TotalMemory: 4 * 1024 * 1024 * 1024,
					},
					DataNode: ytlocal.DataNodeOptions{
						CacheLocations: []ytlocal.DiskLocation{
							{Path: filepath.Join(dir, "chunk_cache")},
						},
					},
					Flavors: []string{"exec"},
				}

				cfgHTTPProxy = ytlocal.HTTPProxy{
					Port: arg.ProxyPort,
					Coordinator: ytlocal.Coordinator{
						Enable:     true,
						Announce:   true,
						ShowPorts:  true,
						PublicFQDN: httpProxyAddr,
					},
					Driver: cfgDriver,
				}

				cfgQueryTracker = ytlocal.QueryTracker{
					User:                       "query_tracker",
					CreateStateTablesOnStartup: true,
				}

				cfgCHYTContoller = ytlocal.CHYTController{
					Strawberry: ytlocal.Strawberry{
						Root:          "//sys/clickhouse/strawberry",
						Stage:         "production",
						RobotUsername: "robot-chyt-controller",
					},
					LocationProxies: []string{
						httpProxyAddr,
					},
					Controller: struct {
						Resolver ytlocal.AddressResolver `yson:"address_resolver"`
					}{
						Resolver: cfgAddressResolver,
					},
					HTTPAPIEndpoint: chytControllerAddr,
					DisableAPIAuth:  true,
				}
			)
			// Allocate ports and fill base config for all components, using
			// cfgBaseServer as a template.
			if err := initBaseConfigs(pa, cfgBaseServer,
				&cfgScheduler.BaseServer,
				&cfgControllerAgent.BaseServer,
				&cfgNode.BaseServer,
				&cfgHTTPProxy.BaseServer,
				&cfgQueryTracker.BaseServer,
				&cfgExecNode.BaseServer,
			); err != nil {
				return errors.Wrap(err, "init base configs")
			}
			var (
				opt           = ytlocal.Options{Binary: bin, Dir: dir}
				master        = ytlocal.NewComponent(opt, cfgMaster)
				scheduler     = ytlocal.NewComponent(opt, cfgScheduler)
				agent         = ytlocal.NewComponent(opt, cfgControllerAgent)
				node          = ytlocal.NewComponent(opt, cfgNode)
				proxy         = ytlocal.NewComponent(opt, cfgHTTPProxy)
				queryTracker  = ytlocal.NewComponent(opt, cfgQueryTracker)
				execNode      = ytlocal.NewComponent(opt, cfgExecNode)
				chytContoller = chytExe.ControllerServer(opt, cfgCHYTContoller)
				chytWaiter    = &waiterServer[ytlocal.CHYTController]{
					done:   make(chan struct{}),
					server: chytContoller,
				}
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
				queryTracker,
				execNode,
				chytWaiter,
			)
			g.Go(func() error {
				yc, err := ythttp.NewClient(&yt.Config{
					Proxy: httpProxyAddr,
				})
				if err != nil {
					return errors.Wrap(err, "create client")
				}

				lg := zctx.From(ctx)
				if err := ytlocal.SetupMaster(ctx, yc, cfgMaster); err != nil {
					return errors.Wrap(err, "setup master")
				}
				lg.Info("Master setup is complete")

				if err := ytlocal.SetupQueryTracker(ctx, yc, clusterName); err != nil {
					return errors.Wrap(err, "setup query trackers")
				}
				lg.Info("Query tracker setup is complete")

				if _, err := yc.CreateNode(ctx,
					ypath.Path(`//sys/clickhouse/strawberry/chyt`),
					yt.NodeMap,
					&yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true},
				); err != nil {
					return errors.Wrap(err, "create clickhouse dir")
				}
				chytWaiter.Ready()
				lg.Info("Run CHYT controller")

				if err := chytExe.Setup(ctx, yc, chytControllerAddr, dir, ytlocal.CHYTInitCluster{Proxy: httpProxyAddr}); err != nil {
					return errors.Wrap(err, "setup chyt")
				}
				lg.Info("CHYT cluster setup is complete")

				return nil
			})
			return g.Wait()
		},
	}

	{
		f := root.Flags()
		root.PersistentFlags().IntVar(&arg.ProxyPort, "port", 8080, "HTTP proxy port")
		root.PersistentFlags().IntVar(&arg.FrontendPort, "frontend-port", 8081, "Frontend port")
		f.BoolVar(&arg.Clean, "clean", true, "Clean temporary directory")
	}
	{
		python := &cobra.Command{
			Use:   "python",
			Short: "Run with yt_local",
			Long: `Run with python yt_local.

The yt_local should be in PATH.
Also, docker is required to run UI.
`,
			RunE: func(cmd *cobra.Command, args []string) error {
				defer func() {
					zctx.From(cmd.Context()).Info("Stopped")
				}()
				const allBinary = "ytserver-all"
				allPath, err := exec.LookPath(allBinary)
				if err != nil {
					return errors.Wrap(err, "look path")
				}

				// Generate random name for cluster.
				name := GenerateID(rand.Reader, 6)
				// Create new temporary directory.
				dir := filepath.Join(os.TempDir(), "ytlocal-python-"+name)
				if err := os.Mkdir(dir, 0o700); err != nil {
					return errors.Wrap(err, "mkdir all")
				}

				// Generate resolver configuration patch and save it to file.
				data, err := yson.Marshal(struct {
					AddressResolver ytlocal.AddressResolver `yson:"address_resolver"`
				}{
					AddressResolver: ytlocal.AddressResolver{
						EnableIPv4:    true,
						EnableIPv6:    false,
						LocalhostFQDN: "localhost",
					},
				})
				if err != nil {
					return errors.Wrap(err, "marshal")
				}
				cfgResolverPath := filepath.Join(dir, "resolver.yson")
				// #nosec G306
				if err := os.WriteFile(cfgResolverPath, data, 0o644); err != nil {
					return errors.Wrap(err, "write file")
				}

				ctx := cmd.Context()
				lg := zctx.From(ctx)
				lg.Info("Setting up",
					zap.String("dir", dir),
					zap.String("name", name),
					zap.Int("proxy.port", arg.ProxyPort),
					zap.Int("ui.port", arg.FrontendPort),
				)

				g, ctx := errgroup.WithContext(ctx)
				const fqdn = "localhost"
				proxyPort := strconv.Itoa(arg.ProxyPort)
				{
					lg := lg.Named("local")
					// #nosec: G204
					c := exec.CommandContext(ctx, "yt_local", "start",
						"--proxy-port", proxyPort,
						"--master-config", cfgResolverPath,
						"--node-config", cfgResolverPath,
						"--scheduler-config", cfgResolverPath,
						"--controller-agent-config", cfgResolverPath,
						"--rpc-proxy-config", cfgResolverPath,
						"--local-cypress-dir", cfgResolverPath,
						"--fqdn", fqdn,
						"--ytserver-all-path", allPath,
						"--id", name,
						"--sync",
					)
					logPath := filepath.Join(dir, "yt_local.log")
					// #nosec: G304
					out, err := os.Create(logPath)
					if err != nil {
						return errors.Wrap(err, "create")
					}
					defer func() { _ = out.Close() }()
					r, w := io.Pipe()

					c.Stderr = io.MultiWriter(out, w)
					c.Stdout = io.MultiWriter(out, w)
					defer func() {
						// Ensure that container is stopped.
						if p := c.Process; p != nil {
							_ = p.Kill()
						}
					}()
					g.Go(func() error {
						defer func() {
							_ = r.Close()
						}()
						s := bufio.NewScanner(r)
						for s.Scan() {
							text := s.Text()
							if strings.Contains(text, "Waiting") {
								idx := strings.Index(text, "Waiting")
								lg.Info(text[idx:])
								continue
							}
							if strings.Contains(text, "Local YT started") {
								lg.Info("Started", zap.Int("proxy_port", arg.ProxyPort))
								continue
							}
							// ["2023-07-25", "16:37:44,704", "INFO", "Watcher started"]
							elems := strings.SplitN(text, " ", 4)
							if len(elems) != 4 {
								continue
							}
							var lvl zapcore.Level
							switch elems[2][0] {
							case 'I':
								lvl = zapcore.InfoLevel
							case 'W', 'C':
								lvl = zapcore.WarnLevel
							case 'E':
								lvl = zapcore.ErrorLevel
							case 'D', 'T':
								lvl = zapcore.DebugLevel
							}
							lg.Check(lvl, elems[3]).Write()
						}
						return nil
					})
					c.Dir = dir
					defer func() {
						// Ensure that yt_local is stopped.
						if p := c.Process; p != nil {
							_ = p.Kill()
						}
					}()
					g.Go(func() error {
						defer func() { _ = w.Close() }()
						lg.Info("Starting", zap.String("log", logPath))
						if err := c.Run(); err != nil {
							// Check if was stopped by signal and context is done.
							if errors.Is(ctx.Err(), context.Canceled) {
								lg.Info("Stopped by context")
								return nil
							}
							lg.Error("Failed", zapErr(err))
							return errors.Wrap(err, "ui")
						}
						return nil
					})
				}
				{
					// Run UI with docker.
					const image = "ytsaurus/ui:stable"
					lg := lg.Named("ui")

					docker, err := testcontainers.NewDockerClientWithOpts(ctx)
					if err != nil {
						return errors.Wrap(err, "docker client")
					}
					// Create network.
					lg.Info("Creating network")
					network := "ytlocal." + name + ".net"
					res, err := docker.NetworkCreate(ctx, network, types.NetworkCreate{
						CheckDuplicate: true,
						Labels: map[string]string{
							"go-faster.oteldb.ytlocal": "true",
						},
					})
					if err != nil {
						return errors.Wrap(err, "network create")
					}
					defer func() {
						// Cleanup network.
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						defer cancel()

						if err := docker.NetworkRemove(ctx, res.ID); err != nil {
							lg.Error("Failed to remove network",
								zapErr(err),
								zap.String("id", res.ID),
							)
						} else {
							lg.Info("Removed network")
						}
					}()
					// Get gateway address.
					networkRes, err := docker.NetworkInspect(ctx, res.ID, types.NetworkInspectOptions{})
					if err != nil {
						return errors.Wrap(err, "network inspect")
					}
					var gateway string
					for _, ipam := range networkRes.IPAM.Config {
						gateway = ipam.Gateway
					}
					if gateway == "" {
						return errors.New("gateway not found")
					}

					pa := &ytlocal.PortAllocator{
						Host: gateway,
						Net:  "tcp",
					}

					gatewayProxyPort, err := pa.Allocate()
					if err != nil {
						return errors.Wrap(err, "allocate")
					}

					gatewayProxy := net.JoinHostPort(gateway, strconv.Itoa(gatewayProxyPort))
					containerName := "ytlocal." + name + ".ui"
					// #nosec: G204
					{
						logPath := filepath.Join(dir, "ui.log")
						// #nosec: G304
						out, err := os.Create(logPath)
						if err != nil {
							return errors.Wrap(err, "create")
						}
						defer func() { _ = out.Close() }()
						c := exec.CommandContext(ctx, "docker", "run", "--rm",
							"--network", network,
							"--name", containerName,
							"-p", fmt.Sprintf("%d:80", arg.FrontendPort),
							"-e", "PROXY="+gatewayProxy,
							"-e", "APP_ENV=local",
							image,
						)
						r, w := io.Pipe()

						c.Stderr = io.MultiWriter(out, w)
						c.Stdout = io.MultiWriter(out, w)
						defer func() {
							// Ensure that container is stopped.
							if p := c.Process; p != nil {
								_ = p.Kill()
							}
						}()
						g.Go(func() error {
							defer func() {
								_ = r.Close()
							}()
							s := bufio.NewScanner(r)
							for s.Scan() {
								text := s.Text()
								if strings.Contains(text, "nginx: started") {
									lg.Info("Started", zap.Int("port", arg.FrontendPort))
									continue
								}
								// ["2023-07-25", "16:37:44,704", "INFO", "Watcher started"]
								elems := strings.SplitN(text, " ", 4)
								if len(elems) != 4 {
									continue
								}
								var lvl zapcore.Level
								switch elems[2][0] {
								case 'I':
									lvl = zapcore.InfoLevel
								case 'W', 'C':
									lvl = zapcore.WarnLevel
								case 'E':
									lvl = zapcore.ErrorLevel
								case 'D', 'T':
									lvl = zapcore.DebugLevel
								default:
									continue
								}
								lg.Check(lvl, elems[3]).Write()
							}
							return nil
						})
						g.Go(func() error {
							defer func() { _ = w.Close() }()
							lg.Info("Start", zap.String("log", logPath))
							defer lg.Info("Stop")
							if err := c.Run(); err != nil {
								// Check if was stopped by signal and context is done.
								if errors.Is(ctx.Err(), context.Canceled) {
									lg.Info("Stopped by context")
									return nil
								}
								lg.Error("Failed", zapErr(err))
								return errors.Wrap(err, "ui")
							}
							return nil
						})
					}
					defer func() {
						lg.Info("Ensuring that container is removed")
						// Ensure that container is removed.
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, time.Second*5)
						defer cancel()

						containers, err := docker.ContainerList(ctx, types.ContainerListOptions{})
						if err != nil {
							lg.Error("Failed to list containers", zapErr(err))
							return
						}
						for _, container := range containers {
							for _, name := range container.Names {
								if name != "/"+containerName {
									lg.Info("Skip container", zap.String("name", name))
									continue
								}
								if err := docker.ContainerRemove(ctx, containerName, types.ContainerRemoveOptions{
									Force: true,
								}); err != nil {
									lg.Error("Failed to remove container", zapErr(err))
								}
							}
						}
					}()
					{
						// Run proxy for UI on container network gateway, so it
						// can be accessed from container.
						var p tcpproxy.Proxy
						target := net.JoinHostPort(fqdn, proxyPort)
						p.AddRoute(gatewayProxy, tcpproxy.To(target))
						g.Go(func() error {
							<-ctx.Done()
							if err := p.Close(); err != nil {
								return errors.Wrap(err, "close proxy")
							}
							return nil
						})
						g.Go(func() error {
							lg := lg.Named("pxy")
							lg.Info("Routing",
								zap.String("from", gatewayProxy),
								zap.String("to", target),
							)
							defer lg.Info("Stop")
							if err := p.Run(); err != nil && !errors.Is(err, net.ErrClosed) {
								lg.Error("Failed", zapErr(err))
								return errors.Wrap(err, "proxy")
							}
							return nil
						})
					}
				}
				if err := g.Wait(); err != nil {
					lg.Error("Wait", zapErr(err))
					return errors.Wrap(err, "wait")
				}
				return nil
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
		lg.Error("Failed", zapErr(err))
		os.Exit(1)
	}
}

type waiterServer[T any] struct {
	done   chan struct{}
	server *ytlocal.Server[T]
}

func (s *waiterServer[T]) Ready() {
	close(s.done)
}

func (s *waiterServer[T]) String() string {
	return s.server.String()
}

func (s *waiterServer[T]) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.server.Run(ctx)
	}
}
