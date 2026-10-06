//go:build !darwin || !cgo

package nativeidentity

import "net"

// Supported reports whether this build can authenticate native socket peers.
func Supported() bool { return false }

// Verify fails closed when the kernel audit token and Security framework cannot
// both be checked. There is deliberately no PID, executable-path, or UID fallback.
func Verify(conn net.Conn, requirement string) (Identity, error) {
	return Identity{}, ErrUnsupported
}
