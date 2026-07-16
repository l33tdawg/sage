//go:build v119testfixture

package abci

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	v119CommitCrashWorkerEnv = "SAGE_V119_COMMIT_CRASH_WORKER"
	v119CommitCrashStoreEnv  = "SAGE_V119_COMMIT_CRASH_STORE"
	v119CommitCrashMarkerEnv = "SAGE_V119_COMMIT_CRASH_MARKER"
	v119CommitCrashStageEnv  = "SAGE_V119_COMMIT_CRASH_STAGE"
)

// TestV119CommitStateCrashWorker is launched by the boundary matrix below.
// The hook fsyncs a readiness marker and then blocks until the parent sends a
// real SIGKILL, so no Go defer or Badger Close can mask a crash-window defect.
func TestV119CommitStateCrashWorker(t *testing.T) {
	if os.Getenv(v119CommitCrashWorkerEnv) != "1" {
		t.Skip("subprocess helper")
	}
	path := os.Getenv(v119CommitCrashStoreEnv)
	marker := os.Getenv(v119CommitCrashMarkerEnv)
	targetStage := store.AtomicStateWriteStage(os.Getenv(v119CommitCrashStageEnv))
	if path == "" || marker == "" || targetStage == "" {
		t.Fatal("commit crash worker environment is incomplete")
	}

	bs, err := store.NewBadgerStore(path)
	require.NoError(t, err)
	restore := store.SetAtomicStateWriteFaultHookForTest(func(stage store.AtomicStateWriteStage) error {
		if stage != targetStage {
			return nil
		}
		file, openErr := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if openErr != nil {
			return fmt.Errorf("create crash marker: %w", openErr)
		}
		if _, writeErr := file.WriteString(string(stage)); writeErr != nil {
			_ = file.Close()
			return fmt.Errorf("write crash marker: %w", writeErr)
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			return fmt.Errorf("sync crash marker: %w", syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("close crash marker: %w", closeErr)
		}
		select {}
	})
	defer restore()

	target := &AppState{Height: 42, AppHash: bytes.Repeat([]byte{0x42}, 32), EpochNum: 6}
	if err := SaveState(bs, target); err != nil {
		t.Fatalf("save state before requested crash stage: %v", err)
	}
	t.Fatalf("requested crash stage %q did not fire", targetStage)
}

func TestV119CommitStateCrashBoundariesReopenAsCoherentHandshakeTuple(t *testing.T) {
	baseline := AppState{Height: 41, AppHash: bytes.Repeat([]byte{0x31}, 32), EpochNum: 5}
	target := AppState{Height: 42, AppHash: bytes.Repeat([]byte{0x42}, 32), EpochNum: 6}
	tests := []struct {
		stage     store.AtomicStateWriteStage
		committed bool
	}{
		{stage: store.AtomicStateWriteBeforeTransaction},
		{stage: store.AtomicStateWriteBeforeCommit},
		{stage: store.AtomicStateWriteAfterCommit, committed: true},
		{stage: store.AtomicStateWriteAfterSync, committed: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.stage), func(t *testing.T) {
			root := t.TempDir()
			badgerPath := filepath.Join(root, "badger")
			seed, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			require.NoError(t, SaveState(seed, &baseline))
			require.NoError(t, seed.CloseBadger())

			marker := filepath.Join(root, "crash-ready")
			cmd := exec.Command(os.Args[0], "-test.run=^TestV119CommitStateCrashWorker$", "-test.v")
			cmd.Env = append(os.Environ(),
				v119CommitCrashWorkerEnv+"=1",
				v119CommitCrashStoreEnv+"="+badgerPath,
				v119CommitCrashMarkerEnv+"="+marker,
				v119CommitCrashStageEnv+"="+string(tt.stage),
			)
			var childOutput bytes.Buffer
			cmd.Stdout = &childOutput
			cmd.Stderr = &childOutput
			require.NoError(t, cmd.Start())

			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, statErr := os.Stat(marker); statErr == nil {
					break
				} else if !os.IsNotExist(statErr) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("inspect crash marker: %v\n%s", statErr, childOutput.String())
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("worker did not reach %s\n%s", tt.stage, childOutput.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.NoError(t, cmd.Process.Kill())
			require.Error(t, cmd.Wait(), "SIGKILLed worker must not exit successfully")

			reopened, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(root, "projection.db"))
			require.NoError(t, err)
			app, err := NewSageAppWithStores(reopened, projection, zerolog.Nop())
			require.NoError(t, err)

			expected := baseline
			if tt.committed {
				expected = target
			}
			loaded, err := LoadState(reopened)
			require.NoError(t, err)
			assert.Equal(t, expected.Height, loaded.Height, "height must come from one complete commit")
			assert.Equal(t, expected.AppHash, loaded.AppHash, "AppHash must match the persisted height")
			assert.Equal(t, expected.EpochNum, loaded.EpochNum, "epoch must match the persisted height")

			// Info is the exact ABCI surface consumed by CometBFT's startup
			// handshaker. It must expose either the complete old tuple (real replay)
			// or the complete new tuple (mock replay), never torn bookkeeping.
			info, err := app.Info(context.Background(), nil)
			require.NoError(t, err)
			assert.Equal(t, expected.Height, info.LastBlockHeight)
			assert.Equal(t, expected.AppHash, info.LastBlockAppHash)
			require.NoError(t, app.Close())
		})
	}
}
