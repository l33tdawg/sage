// Package nativeidentity authenticates the process at the other end of an
// accepted local socket. It grants no vault, dashboard, or governance authority.
package nativeidentity

import "errors"

var (
	ErrUnsupported        = errors.New("native peer identity verification is unsupported")
	ErrPeerRejected       = errors.New("native peer identity rejected")
	ErrInvalidRequirement = errors.New("native peer signing requirement is invalid")
)

// Identity describes code verified against the caller's explicit signing
// requirement. ProcessBinding hashes the kernel's complete audit token, including
// its process generation. PID is diagnostic only; it must not authorize anything.
// An identity is a point-in-time result, not proof the process remains alive.
type Identity struct {
	Identifier     string
	TeamID         string
	CDHash         string
	ProcessBinding string
	UID            uint32
	PID            int32
}
