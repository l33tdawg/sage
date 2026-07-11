package web

// SupportsInProcessRestart reports whether this platform can replace the
// current executable after the coordinated serve lifecycle has fully drained.
func SupportsInProcessRestart() bool { return restartInProcessSupported() }

// RestartProcess replaces the current process image. Callers must first stop
// accepting traffic and close all stores, listeners, consensus and sidecars.
func RestartProcess(execPath string) error { return restartSelf(execPath) }
