package ytlocal

import (
	"os"
	"os/exec"
	"path/filepath"

	"github.com/go-faster/errors"
)

// NewBinary initializes a set of ytsaurus binaries, using path as a directory
// for symlinked binaries.
func NewBinary(path string) (*Binary, error) {
	b := &Binary{
		Components: map[Component]string{},
	}
	if err := b.ensure(path); err != nil {
		return nil, errors.Wrap(err, "ensure")
	}
	return b, nil
}

// Binary is helper for dealing with ytsaurus binaries.
type Binary struct {
	All               string
	Master            string
	Clock             string
	HTTPProxy         string
	JobProxy          string
	ControllerAgent   string
	Node              string
	TCPProxy          string
	MasterCache       string
	CellBalancer      string
	QueueAgent        string
	TimestampProvider string
	Discovery         string
	LogTailer         string
	Scheduler         string
	Tools             string
	Exec              string
	CypressProxy      string
	TabletBalancer    string
	QueryTracker      string

	// Components is a map of component name to binary path.
	Components map[Component]string
}

func (b *Binary) ensure(dir string) error {
	// Search for ytserver-all in $PATH.
	const allBinary = "ytserver-all"
	allPath, err := exec.LookPath(allBinary)
	if err != nil {
		return errors.Wrap(err, "look path")
	}
	b.All = allPath

	// Ensure that all binaries are available.
	//
	// See TryProgram here for list:
	// https://github.com/ytsaurus/ytsaurus/blob/d8cc9c52b6fd94b352a4264579dd89a75aae9b38/yt/yt/server/all/main.cpp#L49-L74
	//
	// If not available, create a symlink to ytserver-all.
	for name, ptr := range map[Component]*string{
		ComponentMaster:      &b.Master,
		"clock":              &b.Clock,
		"http-proxy":         &b.HTTPProxy,
		"node":               &b.Node,
		"job-proxy":          &b.JobProxy,
		"exec":               &b.Exec,
		"tools":              &b.Tools,
		"scheduler":          &b.Scheduler,
		"controller-agent":   &b.ControllerAgent,
		"log-tailer":         &b.LogTailer,
		"discovery":          &b.Discovery,
		"timestamp-provider": &b.TimestampProvider,
		"master-cache":       &b.MasterCache,
		"cell-balancer":      &b.CellBalancer,
		"queue-agent":        &b.QueueAgent,
		"tablet-balancer":    &b.TabletBalancer,
		"cypress-proxy":      &b.CypressProxy,
		"query-tracker":      &b.QueryTracker,
		"tcp-proxy":          &b.TCPProxy,
	} {
		binaryName := "ytserver-" + string(name)
		{
			p, err := exec.LookPath(binaryName)
			if err == nil {
				// Binary in $PATH.
				*ptr = p
				b.Components[name] = p
				continue
			}
		}
		binaryPath := filepath.Join(dir, binaryName)
		*ptr = binaryPath
		b.Components[name] = binaryPath
		if _, err := os.Stat(binaryPath); err == nil {
			// Binary exists.
			continue
		}
		// Create link.
		if err := os.Symlink(allPath, binaryPath); err != nil {
			return errors.Wrap(err, "symlink")
		}
	}
	return nil
}
