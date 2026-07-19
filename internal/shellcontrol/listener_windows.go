//go:build windows

package shellcontrol

import (
	"crypto/sha256"
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func currentUserSID() (string, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", err
	}
	return user.User.Sid.String(), nil
}

func windowsHomeIdentity(sageHome string) (string, error) {
	absolute, err := filepath.Abs(sageHome)
	if err != nil {
		return "", err
	}
	return normalizeWindowsHomeIdentity(filepath.Clean(absolute)), nil
}

func normalizeWindowsHomeIdentity(path string) string {
	identity := strings.ReplaceAll(path, "/", `\`)
	if strings.HasPrefix(strings.ToUpper(identity), `\\?\UNC\`) {
		identity = `\\` + identity[len(`\\?\UNC\`):]
	} else {
		identity = strings.TrimPrefix(identity, `\\?\`)
	}
	for len(identity) > 3 && strings.HasSuffix(identity, `\`) {
		identity = strings.TrimSuffix(identity, `\`)
	}
	return strings.ToLower(identity)
}

func windowsPipeSuffix(sid, canonicalHome string) string {
	digest := sha256.Sum256([]byte(sid + "\x00" + canonicalHome))
	return fmt.Sprintf("%x", digest[:8])
}

func listenEndpoint(sageHome string) (net.Listener, string, func() error, error) {
	sid, err := currentUserSID()
	if err != nil {
		return nil, "", nil, fmt.Errorf("resolve native-shell owner SID: %w", err)
	}
	homeIdentity, err := windowsHomeIdentity(sageHome)
	if err != nil {
		return nil, "", nil, fmt.Errorf("resolve native-shell profile identity: %w", err)
	}
	endpoint := fmt.Sprintf(`\\.\pipe\sage-shell-control-%s`, windowsPipeSuffix(sid, homeIdentity))
	listener, err := winio.ListenPipe(endpoint, &winio.PipeConfig{
		SecurityDescriptor: "D:P(A;;GA;;;" + sid + ")",
		InputBufferSize:    maxFrameBytes, OutputBufferSize: maxFrameBytes,
	})
	if err != nil {
		return nil, "", nil, fmt.Errorf("listen on native-shell named pipe: %w", err)
	}
	return listener, endpoint, func() error { return nil }, nil
}

// The named-pipe DACL is the peer identity boundary. The protocol never opens
// a permissive pipe and never relies on a process-supplied username.
func verifyPeer(net.Conn) error { return nil }
