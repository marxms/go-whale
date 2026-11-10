// Package version reports the build identity of the gowhale binary.
package version

import (
	"fmt"
	"runtime/debug"
)

// The linker overrides these values in release builds. The "devel" default keeps
// `go run` honest and lets `go install @vX.Y.Z` fall back to the module version.
//
//nolint:gochecknoglobals // set by -ldflags in release builds.
var (
	Version = "devel"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info is the build identity printed by `gowhale version`.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Current returns the link-time values and fills any gap from the Go build
// information. A module installed with `go install ...@vX.Y.Z` therefore
// reports the module version even when the linker flags were not used.
func Current() Info {
	info := Info{Version: Version, Commit: Commit, Date: Date}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	if info.Version == "devel" && build.Main.Version != "" && build.Main.Version != "(devel)" {
		info.Version = build.Main.Version
	}
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs.revision":
			if info.Commit == "unknown" {
				info.Commit = setting.Value
			}
		case "vcs.time":
			if info.Date == "unknown" {
				info.Date = setting.Value
			}
		}
	}
	return info
}

// String renders the identity in one line.
func (i Info) String() string {
	return fmt.Sprintf("gowhale %s (commit %s, built %s)", i.Version, i.Commit, i.Date)
}
