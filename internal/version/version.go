/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package version reports what build this is.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Version is set at build time with -ldflags, and falls back to the module
// version or "dev".
var Version = ""

// String renders the version, the commit it came from, and the toolchain.
func String() string {
	version, revision, dirty := build()
	if dirty {
		revision += "-dirty"
	}
	return fmt.Sprintf("%s (%s) %s %s/%s", version, revision, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func build() (version, revision string, dirty bool) {
	version, revision = Version, "unknown"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		if version == "" {
			version = "dev"
		}
		return version, revision, false
	}
	if version == "" {
		version = info.Main.Version
		if version == "" || version == "(devel)" {
			version = "dev"
		}
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if len(setting.Value) >= 12 {
				revision = setting.Value[:12]
			} else {
				revision = setting.Value
			}
		case "vcs.modified":
			dirty = setting.Value == "true"
		}
	}
	return version, revision, dirty
}
