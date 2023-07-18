package ytlocal

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRun(t *testing.T) {
	runDir := t.TempDir()

	singleBinary := "ytserver-all"
	singleBinaryPath, err := exec.LookPath(singleBinary)
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
}
