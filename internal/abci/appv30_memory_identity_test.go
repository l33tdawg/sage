package abci

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

// This is a dispatch unit test only. Signed CheckTx/FinalizeBlock/Commit,
// replay, public-index, serving, and SQL transport qualification remain required.
func TestAppV30RawMemoryDisclosureUsesStrictHPlusOne(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	hash := sha256.Sum256([]byte("canonical whitespace record"))
	require.NoError(t, s.SetMemoryHash(" ", hash[:], "committed"))
	app := &SageApp{badgerStore: s, appV30AppliedHeight: 20}
	for _, tc := range []struct {
		name    string
		height  int64
		allowed bool
	}{{"before", 19, false}, {"activation", 20, false}, {"after", 21, true}} {
		t.Run(tc.name, func(t *testing.T) {
			state, err := app.memoryDisclosureStateForConsensus(" ", tc.height)
			if !tc.allowed {
				require.Error(t, err)
				require.Nil(t, state)
				return
			}
			require.NoError(t, err)
			require.Equal(t, hash[:], state.ContentHash)
			require.Equal(t, "committed", state.Status)
		})
	}
	app.appV30AppliedHeight = 0
	_, err = app.memoryDisclosureStateForConsensus(" ", 21)
	require.Error(t, err, "height alone never activates app-v30")
}

// Historical reads/status writes are not new-write admission. In particular,
// post-fork dispatch must not reject older over-cap non-whitespace identities.
func TestAppV30IdentityDispatchPreservesHistoricalReadAndStatus(t *testing.T) {
	s, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	hash := sha256.Sum256([]byte("historical identity dispatch"))
	identities := []struct {
		name          string
		id            string
		legacyAllowed bool
		afterAllowed  bool
	}{
		{"space-one", " ", false, true},
		{"invalid-utf8-one", "\xff", true, true},
		{"space-512", strings.Repeat(" ", 512), false, true},
		{"nonspace-512", strings.Repeat("x", 512), true, true},
		{"historical-nonspace-513", strings.Repeat("x", 513), true, true},
		{"historical-space-513", strings.Repeat(" ", 513), false, false},
	}
	phases := []struct {
		name          string
		height        int64
		appliedHeight int64
		after         bool
	}{
		{"before", 19, 20, false},
		{"activation", 20, 20, false},
		{"after", 21, 20, true},
		{"unactivated", 21, 0, false},
	}
	for _, identity := range identities {
		for _, phase := range phases {
			t.Run(identity.name+"/"+phase.name, func(t *testing.T) {
				require.NoError(t, s.SetMemoryHash(identity.id, hash[:], "proposed"))
				app := &SageApp{
					badgerStore: s, appV24AppliedHeight: 10,
					appV30AppliedHeight: phase.appliedHeight,
				}
				allowed := identity.legacyAllowed
				if phase.after {
					allowed = identity.afterAllowed
				}
				state, readErr := app.memoryDisclosureStateForConsensus(identity.id, phase.height)
				statusErr := app.setTerminalMemoryStatus(identity.id, "committed", phase.height)
				if allowed {
					require.NoError(t, readErr)
					require.NotNil(t, state)
					require.Equal(t, hash[:], state.ContentHash)
					require.Equal(t, "proposed", state.Status)
					require.NoError(t, statusErr)
				} else {
					require.Error(t, readErr)
					require.Nil(t, state)
					require.Error(t, statusErr)
				}
				storedHash, storedStatus, getErr := s.GetMemoryHash(identity.id)
				require.NoError(t, getErr)
				require.Equal(t, hash[:], storedHash)
				if allowed {
					require.Equal(t, "committed", storedStatus)
				} else {
					require.Equal(t, "proposed", storedStatus)
				}
			})
		}
	}
}
