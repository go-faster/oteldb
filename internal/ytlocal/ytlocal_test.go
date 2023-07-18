package ytlocal

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestRun(t *testing.T) {
	runDir := t.TempDir()

	singleBinary := "ytserver-all"
	singleBinaryPath, err := exec.LookPath(singleBinary)
	if os.Getenv("YT_LOCAL_TEST") == "" {
		t.Skip("YT_LOCAL_TEST not set")
	}
	if err != nil {
		t.Skipf("Binary %q not found in $PATH", singleBinary)
	}

	// Ensure that all binaries are available.
	//
	// See TryProgram here for list:
	// https://github.com/ytsaurus/ytsaurus/blob/d8cc9c52b6fd94b352a4264579dd89a75aae9b38/yt/yt/server/all/main.cpp#L49-L74
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

	// Arguments:
	// --config <path>

	// Test all binaries.
	for _, binary := range binaries {
		cmd := exec.Command(binary, "--help")
		cmd.Dir = runDir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(out), fmt.Sprintf("Usage: %s [OPTIONS]", binary))
	}

	// Try running master.
	const masterPort = 23741
	const masterMonitoringPort = 23742
	const cellID = "a3c51a55-ffffffff-259-ffffffff"
	const localhost = "localhost"
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
				Addresses: []string{masterAddr},
			},
			ClusterConnection: ClusterConnection{
				ClusterName: "test",
				DiscoveryConnection: Connection{
					Addresses: []string{masterAddr},
				},
				PrimaryMaster: Connection{
					Addresses: []string{masterAddr},
					CellID:    cellID,
				},
			},
			Logging: Logging{
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
		UseNewHydra: true,
		PrimaryMaster: Connection{
			Addresses: []string{masterAddr},
			CellID:    cellID,
		},
		CypressManager: CypressManager{
			DefaultJournalWriteQuorum:       1,
			DefaultJournalReadQuorum:        1,
			DefaultFileReplicationFactor:    1,
			DefaultJournalReplicationFactor: 1,
			DefaultTableReplicationFactor:   1,
		},
	}
	data, err := yson.Marshal(cfg)
	require.NoError(t, err)

	cfgPath := filepath.Join(runDir, "master.yson")
	require.NoError(t, os.WriteFile(cfgPath, data, 0644))

	cmd := exec.Command(binaries["master"], "--config", cfgPath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	require.NoError(t, cmd.Run())
}
