package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerateKeyConcurrentFirstLaunchUsesOneIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.key")
	const callers = 24
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := loadOrGenerateKey(path)
			if err == nil {
				ids <- hex.EncodeToString(key.Public().(ed25519.PublicKey))
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	require.Len(t, unique, 1)
}
