//go:build !darwin

package rerankd

import "runtime"

// effectiveArch is the plain GOARCH outside macOS (no Rosetta equivalent to
// account for).
func effectiveArch() string { return runtime.GOARCH }
