package store

import (
	"encoding/binary"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func identityTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	s, err := OpenBadgerStoreWithoutMigrations(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	return s
}

func identityTestHeight(t *testing.T, s *BadgerStore, height uint64) {
	t.Helper()
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, height)
	require.NoError(t, s.SetState("height", value))
}

func identityTestMode(t *testing.T, s *BadgerStore) memoryIdentityMode {
	t.Helper()
	var mode memoryIdentityMode
	require.NoError(t, s.db.View(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn, taintServingReadOnly: true}
		var err error
		mode, err = reader.memoryIdentityModeInSnapshot()
		return err
	}))
	return mode
}

func TestMemoryIdentityModeUsesFinalizedHeight(t *testing.T) {
	for _, test := range []struct {
		name   string
		height uint64
		raw    bool
	}{{"pre-boundary", 9, false}, {"at-boundary", 10, false}, {"first-post-boundary", 11, true}} {
		t.Run(test.name, func(t *testing.T) {
			s := identityTestStore(t)
			require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
			identityTestHeight(t, s, test.height)
			mode := identityTestMode(t, s)
			require.Equal(t, test.raw, mode.raw)
			require.Equal(t, test.raw, mode.useRaw(" "))
			require.False(t, mode.useRaw(strings.Repeat("x", 513)))
		})
	}
	s := identityTestStore(t)
	require.False(t, identityTestMode(t, s).raw)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	var mode memoryIdentityMode
	err := s.db.View(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn}
		var err error
		mode, err = reader.memoryIdentityModeInSnapshot()
		return err
	})
	require.Error(t, err)
	require.False(t, mode.raw)
	require.NoError(t, s.SetState("height", []byte{0}))
	err = s.db.View(func(txn *badger.Txn) error {
		_, err := (&BadgerStore{db: s.db, txn: txn}).memoryIdentityModeInSnapshot()
		return err
	})
	require.Error(t, err)
}

func TestMemoryIdentityModeRemainsInHeldSnapshot(t *testing.T) {
	s := identityTestStore(t)
	identityTestHeight(t, s, 9)
	txn := s.db.NewTransaction(false)
	defer txn.Discard()
	reader := &BadgerStore{db: s.db, txn: txn, taintServingReadOnly: true}
	before, err := reader.memoryIdentityModeInSnapshot()
	require.NoError(t, err)
	require.False(t, before.raw)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	after, err := reader.memoryIdentityModeInSnapshot()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.True(t, identityTestMode(t, s).raw)
}

func TestMemoryIdentityModeQualifiedFreshGenesis(t *testing.T) {
	for _, app := range []uint64{29, 30} {
		t.Run(map[uint64]string{29: "fresh29", 30: "fresh30"}[app], func(t *testing.T) {
			s := identityTestStore(t)
			origin := FreshGenesisActivation{Version: FreshGenesisVersion, AppVersion: app, Scope: strings.Repeat("11", 32), BootstrapDigest: strings.Repeat("22", 32), RootID: strings.Repeat("33", 32), Validators: []FreshGenesisValidator{{ValidatorID: strings.Repeat("44", 32), Power: 1}}}
			require.NoError(t, s.BootstrapFreshGenesis(origin))
			mode := identityTestMode(t, s)
			require.False(t, mode.raw)
			require.False(t, mode.heightPresent)
			identityTestHeight(t, s, 0)
			require.False(t, identityTestMode(t, s).raw)
			identityTestHeight(t, s, 1)
			require.Equal(t, app == 30, identityTestMode(t, s).raw)
			if app == 29 {
				require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
				identityTestHeight(t, s, 11)
				require.True(t, identityTestMode(t, s).raw)
			}
			// A valid marker without its canonical scope binding is insufficient.
			require.NoError(t, s.SetState("governance_delegation_domain_v20", make([]byte, 32)))
			err := s.db.View(func(txn *badger.Txn) error {
				_, err := (&BadgerStore{db: s.db, txn: txn}).memoryIdentityModeInSnapshot()
				return err
			})
			require.Error(t, err)
		})
	}
}

func TestMemoryProjectionInventoryPreservesRawAndHistoricalIDs(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	require.NoError(t, s.SetState("app_hash", make([]byte, 32)))
	ids := []string{"\x00", " ", strings.Repeat("x", 513), string([]byte{0xff})}
	for _, id := range ids {
		require.NoError(t, s.SetMemoryHash(id, make([]byte, 32), "committed"))
	}
	inventory, err := s.MemoryProjectionInventory(10, 4096)
	require.NoError(t, err)
	require.Equal(t, ids, inventory.MemoryIDs)
	require.True(t, inventory.RawIdentityRulesAt(11))
	require.False(t, inventory.RawIdentityRulesAt(10))
	reader := &BadgerStore{db: s.db, txn: s.db.NewTransaction(false)}
	defer reader.txn.Discard()
	_, err = reader.MemoryProjectionInventory(10, 4096)
	require.Error(t, err)
	_, err = s.MemoryProjectionInventory(3, 4096)
	require.Error(t, err)
	_, err = s.MemoryProjectionInventory(10, 512)
	require.Error(t, err)
	// Historical nonblank IDs stay readable, while overcap whitespace keeps
	// the historical refusal; admission's512 limit is never a blanket read ban.
	state, err := s.GetMemoryDisclosureState(strings.Repeat("x", 513))
	require.NoError(t, err)
	require.NotNil(t, state)
	require.NoError(t, s.SetMemoryHash(strings.Repeat(" ", 513), make([]byte, 32), "committed"))
	_, err = s.MemoryProjectionInventory(10, 4096)
	require.Error(t, err)
}

func TestMemoryProjectionInventoryDoesNotInferRawModeFromIDs(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.SetMemoryHash(" ", make([]byte, 32), "committed"))
	_, err := s.MemoryProjectionInventory(10, 4096)
	require.Error(t, err)
	identityTestHeight(t, s, 11)
	_, err = s.MemoryProjectionInventory(10, 4096)
	require.Error(t, err)
}
