//go:build v119testfixture

package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type bootStateSyncPrePublishEvidence struct {
	Height     int64  `json:"height"`
	AppHash    string `json:"app_hash"`
	AppVersion uint64 `json:"app_version"`
}

// runBootStateSyncPrePublishHook creates a deterministic wire-test boundary
// after the activation journal/config/directories are complete but before the
// runtime publishes Sealed and releases consensus serving. The marker records
// the exact tuple already validated against the live Comet state store, seen
// commit, block-sync handoff, and activated application. Production builds
// compile a no-op and do not recognize this environment variable.
func runBootStateSyncPrePublishHook(ctx context.Context, height int64, appHash []byte, appVersion uint64) error {
	marker := os.Getenv("SAGE_V119_STATE_SYNC_PRE_PUBLISH_PAUSE_FILE")
	if marker == "" {
		return nil
	}
	if height <= 0 || len(appHash) != sha256.Size || appVersion == 0 {
		return errors.New("pre-publication evidence requires a positive height, canonical AppHash, and positive app version")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create pre-publication marker: %w", err)
	}
	evidence := bootStateSyncPrePublishEvidence{
		Height:     height,
		AppHash:    hex.EncodeToString(appHash),
		AppVersion: appVersion,
	}
	if err = json.NewEncoder(f).Encode(evidence); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("persist pre-publication marker: %w", err)
	}
	<-ctx.Done()
	return ctx.Err()
}
