//go:build !windows && !linux && !darwin

package shellcontrol

import "net"

// Endpoint access is restricted by a 0700 parent and 0600 socket. Linux and
// Darwin add exact peer-UID checks; remaining Unix platforms retain the
// filesystem ownership boundary where Go exposes no portable peer UID API.
func verifyPeerPlatform(*net.UnixConn) error { return nil }
