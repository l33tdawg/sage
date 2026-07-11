package p2p

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// LoadOrCreateIdentity loads a dedicated libp2p identity or creates it with
// owner-only permissions. The key is intentionally separate from the SAGE
// agent, validator, and federation-CA keys: it authenticates connectivity,
// while federation mTLS and request signatures remain the trust boundary.
func LoadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	if path == "" {
		return nil, errors.New("p2p identity path is required")
	}
	if key, err := loadIdentity(path); err == nil {
		return key, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create p2p identity directory: %w", err)
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate p2p identity: %w", err)
	}
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal p2p identity: %w", err)
	}

	// O_EXCL makes concurrent first boots safe and refuses to follow an
	// attacker-planted symlink. If another process won the race, load the key it
	// committed instead of replacing it and changing this node's peer ID.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- operator-owned configured path
	if errors.Is(err, os.ErrExist) {
		return loadIdentity(path)
	}
	if err != nil {
		return nil, fmt.Errorf("create p2p identity: %w", err)
	}
	committed := false
	defer func() {
		_ = f.Close()
		if !committed {
			_ = os.Remove(path)
		}
	}()
	if _, err = f.Write(raw); err != nil {
		return nil, fmt.Errorf("write p2p identity: %w", err)
	}
	if err = f.Sync(); err != nil {
		return nil, fmt.Errorf("sync p2p identity: %w", err)
	}
	if err = f.Close(); err != nil {
		return nil, fmt.Errorf("close p2p identity: %w", err)
	}
	committed = true
	return priv, nil
}

func loadIdentity(path string) (crypto.PrivKey, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("p2p identity %s must not be a symbolic link", path)
	}
	f, err := os.Open(path) // #nosec G304 -- operator-owned configured path
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat p2p identity: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("p2p identity %s is not a regular file", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("p2p identity %s permissions are %o; require 600", path, info.Mode().Perm())
	}
	raw, err := io.ReadAll(io.LimitReader(f, 16<<10))
	if err != nil {
		return nil, fmt.Errorf("read p2p identity: %w", err)
	}
	if len(raw) == 0 || len(raw) >= 16<<10 {
		return nil, fmt.Errorf("p2p identity %s has invalid size", path)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("decode p2p identity: %w", err)
	}
	return priv, nil
}
