package ytlocal

import (
	"fmt"
	"path"
	"testing"
)

func TestMaster(t *testing.T) {
	const masterPort = 23741
	const masterMonitoringPort = 23742
	const cellID = "a3c51a55-ffffffff-259-ffffffff"
	const localhost = "localhost"
	runDir := "/tmp"
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
						MinLevel: LogLevelDebug,
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
			Path: path.Join(runDir, "changelogs"),
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
			Path: path.Join(runDir, "snapshots"),
		},
	}
	encode(t, "master", cfg)
}
