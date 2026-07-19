//go:build !windows

package shellcontrol

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
)

func listenEndpoint(sageHome string) (net.Listener, string, func() error, error) {
	runDir := filepath.Join(sageHome, "run")
	if info, statErr := os.Lstat(runDir); statErr == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return nil, "", nil, fmt.Errorf("native-shell runtime path is not a real directory")
	}
	if mkdirErr := os.MkdirAll(runDir, 0700); mkdirErr != nil {
		return nil, "", nil, fmt.Errorf("create native-shell runtime directory: %w", mkdirErr)
	}
	if chmodErr := os.Chmod(runDir, 0700); chmodErr != nil {
		return nil, "", nil, fmt.Errorf("protect native-shell runtime directory: %w", chmodErr)
	}
	endpoint := filepath.Join(runDir, "shell-control.sock")
	if info, statErr := os.Lstat(endpoint); statErr == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if info.Mode()&os.ModeSocket == 0 || !ok || int(stat.Uid) != os.Geteuid() {
			return nil, "", nil, fmt.Errorf("refuse unsafe existing native-shell control endpoint")
		}
		if removeErr := os.Remove(endpoint); removeErr != nil {
			return nil, "", nil, fmt.Errorf("remove stale native-shell control socket: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, "", nil, fmt.Errorf("inspect native-shell control endpoint: %w", statErr)
	}
	listener, listenErr := (&net.ListenConfig{}).Listen(context.Background(), "unix", endpoint)
	if listenErr != nil {
		return nil, "", nil, fmt.Errorf("listen on native-shell control socket: %w", listenErr)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		_ = os.Remove(endpoint)
		return nil, "", nil, fmt.Errorf("native-shell control endpoint is not a Unix listener")
	}
	// Cleanup below verifies that the path still identifies our socket. The
	// standard library's default unlink-on-close behavior would bypass that
	// check and can remove a replacement endpoint on Unix.
	unixListener.SetUnlinkOnClose(false)
	if chmodErr := os.Chmod(endpoint, 0600); chmodErr != nil {
		_ = listener.Close()
		_ = os.Remove(endpoint)
		return nil, "", nil, fmt.Errorf("protect native-shell control socket: %w", chmodErr)
	}
	ownedInfo, statErr := os.Lstat(endpoint)
	if statErr != nil {
		_ = listener.Close()
		return nil, "", nil, fmt.Errorf("record native-shell control socket identity: %w", statErr)
	}
	cleanup := func() error {
		current, currentErr := os.Lstat(endpoint)
		if os.IsNotExist(currentErr) {
			return nil
		}
		if currentErr != nil {
			return currentErr
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
