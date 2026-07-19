//go:build !windows

package shellcontrol

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func listenEndpoint(sageHome string) (net.Listener, string, func() error, error) {
	runDir := filepath.Join(sageHome, "run")
	if info, err := os.Lstat(runDir); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return nil, "", nil, fmt.Errorf("native-shell runtime path is not a real directory")
	}
	if err := os.MkdirAll(runDir, 0700); err != nil {
		return nil, "", nil, fmt.Errorf("create native-shell runtime directory: %w", err)
	}
	if err := os.Chmod(runDir, 0700); err != nil {
		return nil, "", nil, fmt.Errorf("protect native-shell runtime directory: %w", err)
	}
	endpoint := filepath.Join(runDir, "shell-control.sock")
	if info, err := os.Lstat(endpoint); err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if info.Mode()&os.ModeSocket == 0 || !ok || int(stat.Uid) != os.Geteuid() {
			return nil, "", nil, fmt.Errorf("refuse unsafe existing native-shell control endpoint")
		}
		if err := os.Remove(endpoint); err != nil {
			return nil, "", nil, fmt.Errorf("remove stale native-shell control socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, "", nil, fmt.Errorf("inspect native-shell control endpoint: %w", err)
	}
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, "", nil, fmt.Errorf("listen on native-shell control socket: %w", err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		listener.Close()
		_ = os.Remove(endpoint)
		return nil, "", nil, fmt.Errorf("native-shell control endpoint is not a Unix listener")
	}
	// Cleanup below verifies that the path still identifies our socket. The
	// standard library's default unlink-on-close behavior would bypass that
	// check and can remove a replacement endpoint on Unix.
	unixListener.SetUnlinkOnClose(false)
	if err := os.Chmod(endpoint, 0600); err != nil {
		listener.Close()
		_ = os.Remove(endpoint)
		return nil, "", nil, fmt.Errorf("protect native-shell control socket: %w", err)
	}
	ownedInfo, err := os.Lstat(endpoint)
	if err != nil {
		listener.Close()
		return nil, "", nil, fmt.Errorf("record native-shell control socket identity: %w", err)
	}
	cleanup := func() error {
		current, err := os.Lstat(endpoint)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.Mode()&os.ModeSocket == 0 || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(ownedInfo, current) {
			return fmt.Errorf("refuse to remove replaced native-shell endpoint")
		}
		return os.Remove(endpoint)
	}
	return unixListener, endpoint, cleanup, nil
}

func verifyPeer(conn net.Conn) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("native-shell peer is not a Unix socket")
	}
	return verifyPeerPlatform(unixConn)
}
