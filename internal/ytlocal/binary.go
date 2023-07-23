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
		ComponentMaster:            &b.Master,
		ComponentClock:             &b.Clock,
		ComponentHTTPProxy:         &b.HTTPProxy,
		ComponentNode:              &b.Node,
		ComponentJobProxy:          &b.JobProxy,
		ComponentExec:              &b.Exec,
		ComponentTools:             &b.Tools,
		ComponentScheduler:         &b.Scheduler,
		ComponentControllerAgent:   &b.ControllerAgent,
		ComponentLogTailer:         &b.LogTailer,
		ComponentDiscovery:         &b.Discovery,
		ComponentTimestampProvider: &b.TimestampProvider,
		ComponentMasherCache:       &b.MasterCache,
		ComponentCellBalancer:      &b.CellBalancer,
		ComponentQueueAgent:        &b.QueueAgent,
		ComponentTabletBalancer:    &b.TabletBalancer,
		ComponentCypressProxy:      &b.CypressProxy,
		ComponentQueryTracker:      &b.QueryTracker,
		ComponentTCPProxy:          &b.TCPProxy,
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
