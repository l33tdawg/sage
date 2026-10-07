//go:build darwin && cgo

package nativeidentity

/*
#cgo LDFLAGS: -framework Security -framework CoreFoundation -lbsm
#include "verify_darwin.h"
#include <stdlib.h>
*/
import "C"

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"unsafe"
)

// Supported reports whether this build can authenticate native socket peers.
func Supported() bool { return true }

// Verify authenticates an accepted Unix stream socket using its kernel-supplied
// audit token and the Security framework's dynamic code validation. The caller
// must supply a trusted, compiled-in signing requirement, never one from the peer
// or an environment variable. A production requirement must pin the intended
// signing authority and identifier; an exact cdhash may pin a disposable fixture.
//
// Every peer must have the daemon's effective UID and a valid, hardened runtime
// signature without debug or runtime-relaxation entitlements. Nothing reads a
// client-provided PID, path, bundle identifier, or audit token. The result is a
// point-in-time admission check: use a fresh check for each credential issuance,
// then bind that issuance to a short-lived, single-use challenge.
func Verify(conn net.Conn, requirement string) (Identity, error) {
	if len(requirement) == 0 || len(requirement) > 8192 || strings.TrimSpace(requirement) == "" || strings.ContainsRune(requirement, '\x00') {
		return Identity{}, ErrInvalidRequirement
	}
	un, ok := conn.(*net.UnixConn)
	if !ok || un == nil {
		return Identity{}, fmt.Errorf("%w: expected an accepted Unix stream socket", ErrPeerRejected)
	}
	raw, err := un.SyscallConn()
	if err != nil {
		return Identity{}, fmt.Errorf("%w: socket unavailable: %v", ErrPeerRejected, err)
	}
	var token C.sage_peer_token
	var tokenErr C.int
	if err := raw.Control(func(fd uintptr) {
		tokenErr = C.sage_peer_token_read(C.int(fd), &token)
	}); err != nil {
		return Identity{}, fmt.Errorf("%w: socket unavailable: %v", ErrPeerRejected, err)
	}
	if tokenErr != 0 {
		return Identity{}, fmt.Errorf("%w: kernel socket credentials (%d)", ErrPeerRejected, int(tokenErr))
	}

	policy := C.CString(requirement)
	defer C.free(unsafe.Pointer(policy))
	var result C.sage_peer_identity
	var detail C.int32_t
	stage := C.sage_peer_verify(&token, policy, &result, &detail)
	if stage != 0 {
		if stage == C.SAGE_PEER_REQUIREMENT {
			return Identity{}, fmt.Errorf("%w: Security status %d", ErrInvalidRequirement, int32(detail))
		}
		return Identity{}, fmt.Errorf("%w: %s (%d)", ErrPeerRejected, C.GoString(C.sage_peer_stage_name(stage)), int32(detail))
	}
	// LOCAL_PEERTOKEN is a fresh kernel lookup of the socket peer's task. It is
	// not an immutable connect-time credential: exec or descriptor transfer can
	// change it. Fail if it changed while Security inspected the first token.
	var after C.sage_peer_token
	if err := raw.Control(func(fd uintptr) {
		tokenErr = C.sage_peer_token_read(C.int(fd), &after)
	}); err != nil || tokenErr != 0 || token != after {
		return Identity{}, fmt.Errorf("%w: socket peer changed during verification", ErrPeerRejected)
	}
	digest := sha256.Sum256(C.GoBytes(unsafe.Pointer(&token), C.int(C.sizeof_sage_peer_token)))
	return Identity{
		Identifier:     C.GoString(&result.identifier[0]),
		TeamID:         C.GoString(&result.team_id[0]),
		CDHash:         hex.EncodeToString(C.GoBytes(unsafe.Pointer(&result.cdhash[0]), C.int(result.cdhash_len))),
		ProcessBinding: hex.EncodeToString(digest[:]),
		UID:            uint32(result.uid),
		PID:            int32(result.pid),
	}, nil
}
