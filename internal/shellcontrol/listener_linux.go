//go:build linux

package shellcontrol

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func verifyPeerPlatform(conn *net.UnixConn) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if err != nil {
			credentialErr = err
			return
		}
		if int(cred.Uid) != os.Geteuid() {
			credentialErr = fmt.Errorf("native-shell peer uid does not own this daemon")
		}
	}); err != nil {
		return err
	}
	return credentialErr
}
