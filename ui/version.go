package ui

import (
	"runtime/debug"
	"sync"
)

// moduleName is the import path of this module. It must match the value in
// go.mod so that detectVersion can locate the right entry in build info.
const moduleName = "github.com/yakser/asynqpg/ui"

const devVersion = "(devel)"

var (
	versionOnce  sync.Once
	versionValue string
)

// detectVersion returns the module version of asynqpg/ui as recorded in the
// host binary's build info. The value is populated by the Go toolchain when
// the module is consumed as a dependency of a tagged release; for local
// builds (replace directives, go.work, plain `go build`) it is "(devel)".
//
// Callers that need a custom string (e.g. a binary built with
// `-ldflags '-X my/pkg.Version=v1.2.3'`) should set HandlerOpts.Version
// instead of relying on this fallback.
func detectVersion() string {
	versionOnce.Do(func() {
		versionValue = readModuleVersion()
	})
	return versionValue
}

func readModuleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return devVersion
	}

	if info.Main.Path == moduleName {
		if info.Main.Version != "" {
			return info.Main.Version
		}
		return devVersion
	}

	for _, dep := range info.Deps {
		if dep.Path != moduleName {
			continue
		}
		if dep.Replace != nil {
			return devVersion
		}
		if dep.Version == "" {
			return devVersion
		}
		return dep.Version
	}

	return devVersion
}
