// Package cliversion provides the version of the binary.
package cliversion

import (
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

func getVersion(modulePath string, m *debug.Module) (string, bool) {
	if m == nil || m.Path != modulePath {
		return "", false
	}
	return m.Version, true
}

func getInfo(modulePath string) (info Info, _ bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info, ok
	}
	info.GoVersion = bi.GoVersion

	var isDep bool
	if v, ok := getVersion(modulePath, &bi.Main); ok {
		info.Version = v
	} else {
		isDep = true
		for _, m := range bi.Deps {
			if v, ok := getVersion(modulePath, m); ok {
				info.Version = v
				break
			}
		}
	}

	if !isDep {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.Commit = s.Value
			case "vcs.time":
				if t, err := time.Parse(time.RFC3339Nano, s.Value); err == nil {
					info.Time = t
				}
			}
		}
	}
	return info, true
}

// Info is the build information.
type Info struct {
	// Version is the version of the module.
	Version string
	// GoVersion is the version of the Go that produced this binary.
	GoVersion string

	// Commit is the current commit hash.
	Commit string
	// Time is the time of the build.
	Time time.Time
}

// GetInfo returns the build information.
func GetInfo(modulePath string) (Info, bool) {
	return getInfo(modulePath)
}

// String returns string representation of the build information.
func (i Info) String() string {
	var s strings.Builder
	s.WriteString("version ")
	if v := i.Version; v != "" {
		s.WriteString(v)
	} else {
		s.WriteString("unknown")
	}
	if commit := i.Commit; commit != "" {
		s.WriteByte('-')
		s.WriteString(commit)
	}

	if t, v := i.Time, i.GoVersion; v != "" || !t.IsZero() {
		s.WriteString(" (built")
		if v != "" {
			s.WriteString(" with ")
			s.WriteString(v)
		}
		if !t.IsZero() {
			s.WriteString(" at ")
			s.WriteString(t.Format(time.RFC1123))
		}
		s.WriteByte(')')
	}
	const osArch = " " + runtime.GOOS + "/" + runtime.GOARCH
	s.WriteString(osArch)
	return s.String()
}
