//go:build v119testfixture

package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootStateSyncPrePublishHookPersistsExactValidatedTuple(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pre-publish.json")
	t.Setenv("SAGE_V119_STATE_SYNC_PRE_PUBLISH_PAUSE_FILE", marker)
	wantHash := sha256.Sum256([]byte("validated Comet state"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runBootStateSyncPrePublishHook(ctx, 97, wantHash[:], 20)
	}()

	var evidence bootStateSyncPrePublishEvidence
	require.Eventually(t, func() bool {
		encoded, err := os.ReadFile(marker) //nolint:gosec // test-owned path
		if err != nil {
			return false
		}
		return json.Unmarshal(encoded, &evidence) == nil
	}, time.Second, time.Millisecond)
	assert.Equal(t, int64(97), evidence.Height)
	assert.Equal(t, hex.EncodeToString(wantHash[:]), evidence.AppHash)
	assert.Equal(t, uint64(20), evidence.AppVersion)

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestBootStateSyncPrePublishHookRejectsInvalidEvidence(t *testing.T) {
	t.Setenv("SAGE_V119_STATE_SYNC_PRE_PUBLISH_PAUSE_FILE", filepath.Join(t.TempDir(), "pre-publish.json"))
	err := runBootStateSyncPrePublishHook(context.Background(), 0, nil, 0)
	require.Error(t, err)
	assert.False(t, errors.Is(err, context.Canceled))
}
