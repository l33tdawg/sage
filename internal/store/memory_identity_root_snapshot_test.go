package store

import (
	"encoding/json"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func identityRootFixture(t *testing.T) (*BadgerStore, FreshGenesisActivation) {
	t.Helper()
	s := identityTestStore(t)
	origin := FreshGenesisActivation{Version: FreshGenesisVersion, AppVersion: 30, Scope: strings.Repeat("11", 32), BootstrapDigest: strings.Repeat("22", 32), RootID: strings.Repeat("33", 32), Validators: []FreshGenesisValidator{{ValidatorID: strings.Repeat("44", 32), Power: 1}}}
	require.NoError(t, s.BootstrapFreshGenesis(origin))
	identityTestHeight(t, s, 1)
	return s, origin
}

func identityRootWriteFixture(t *testing.T, s *BadgerStore, key, value []byte) {
	t.Helper()
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error { return txn.Set(key, value) }))
}

func identityRootModeError(t *testing.T, s *BadgerStore) {
	t.Helper()
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := (&BadgerStore{db: s.db, txn: txn, taintServingReadOnly: true}).memoryIdentityModeInSnapshot()
		return err
	})
	require.Error(t, err)
}

func TestMemoryIdentityRootCoreRejectsMalformedState(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*AppV23RootState)
	}{
		{"zero-generation", func(root *AppV23RootState) { root.Generation = 0 }},
		{"empty-credential", func(root *AppV23RootState) { root.CredentialID = "" }},
		{"uppercase-credential", func(root *AppV23RootState) { root.CredentialID = strings.Repeat("AB", 32) }},
		{"short-history-digest", func(root *AppV23RootState) { root.HistoryDigest = "00" }},
		{"nonhex-history-digest", func(root *AppV23RootState) { root.HistoryDigest = strings.Repeat("z", 64) }},
		{"uppercase-history-digest", func(root *AppV23RootState) { root.HistoryDigest = strings.Repeat("AB", 32) }},
		{"wrong-first-history-digest", func(root *AppV23RootState) { root.HistoryDigest = strings.Repeat("00", 32) }},
		{"mismatched-principal", func(root *AppV23RootState) { root.PrincipalID = strings.Repeat("66", 32) }},
		{"first-generation-different-credential", func(root *AppV23RootState) { root.CredentialID = strings.Repeat("66", 32) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := identityRootFixture(t)
			root, err := s.GetAppV23Root()
			require.NoError(t, err)
			test.mutate(root)
			encoded, err := json.Marshal(root)
			require.NoError(t, err)
			identityRootWriteFixture(t, s, appV23RootKey(), encoded)
			identityRootModeError(t, s)
		})
	}
}

func TestMemoryIdentityRootMarkersRejectMissingMalformedOrStale(t *testing.T) {
	for _, test := range []struct {
		name      string
		principal bool
		value     []byte
		remove    bool
	}{
		{"missing-current", false, nil, true},
		{"short-current", false, []byte{1}, false},
		{"oversized-current", false, make([]byte, 9), false},
		{"zero-current", false, appV23RootCredentialGenerationValue(0), false},
		{"stale-current", false, appV23RootCredentialGenerationValue(1), false},
		{"missing-principal", true, nil, true},
		{"wrong-principal-generation", true, appV23RootCredentialGenerationValue(2), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, origin := identityRootFixture(t)
			credential := strings.Repeat("55", 32)
			require.NoError(t, s.RotateAppV23RootCredential(1, credential, 1))
			id := credential
			if test.principal {
				id = origin.RootID
			}
			key := appV23RootCredentialKey(id)
			if test.remove {
				require.NoError(t, s.db.Update(func(txn *badger.Txn) error { return txn.Delete(key) }))
			} else {
				identityRootWriteFixture(t, s, key, test.value)
			}
			identityRootModeError(t, s)
		})
	}
}

func TestMemoryIdentityRootPreservesValidCredentialRotation(t *testing.T) {
	s, origin := identityRootFixture(t)
	require.True(t, identityTestMode(t, s).raw)
	credential := strings.Repeat("55", 32)
	require.NoError(t, s.RotateAppV23RootCredential(1, credential, 1))
	require.NoError(t, s.ValidateAppV23State())
	root, err := s.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, origin.RootID, root.PrincipalID)
	require.Equal(t, credential, root.CredentialID)
	require.Equal(t, uint64(2), root.Generation)
	require.True(t, identityTestMode(t, s).raw)
	// Fixed presence checks preserve the ordinary-principal collision guard.
	identityRootWriteFixture(t, s, appV23EnrollmentKey(credential), []byte(`{}`))
	identityRootModeError(t, s)
}

func TestMemoryIdentityOriginRecordsRejectInvalidSchemaAndBounds(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*FreshGenesisActivation)
	}{
		{"schema", func(origin *FreshGenesisActivation) { origin.Version = FreshGenesisVersion + 1 }},
		{"app-version-above-range", func(origin *FreshGenesisActivation) { origin.AppVersion = FreshGenesisMaxAppVersion + 1 }},
		{"app-version-below-range", func(origin *FreshGenesisActivation) { origin.AppVersion = FreshGenesisMinAppVersion - 1 }},
		{"noncanonical-scope", func(origin *FreshGenesisActivation) { origin.Scope = strings.Repeat("AB", 32) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, origin := identityRootFixture(t)
			test.mutate(&origin)
			encoded, err := json.Marshal(origin)
			require.NoError(t, err)
			identityRootWriteFixture(t, s, freshGenesisActivationKey(), encoded)
			identityRootModeError(t, s)
		})
	}
	for _, test := range []struct {
		name string
		key  func() []byte
		size int
	}{
		{"root-JSON-cap", appV23RootKey, (8 << 10) + 1},
		{"origin-JSON-cap", freshGenesisActivationKey, (128 << 10) + 1},
		{"applied-JSON-cap", func() []byte { return upgradeAppliedKey("app-v30") }, 1025},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := identityRootFixture(t)
			identityRootWriteFixture(t, s, test.key(), []byte(strings.Repeat(" ", test.size)))
			identityRootModeError(t, s)
		})
	}
}

func TestMemoryIdentityAppliedRecordRequiresExactCanonicalUpgrade(t *testing.T) {
	for _, record := range []AppliedUpgradeRecord{
		{Name: "other-upgrade", TargetAppVersion: 30, AppliedHeight: 10},
		{Name: "app-v30", TargetAppVersion: 31, AppliedHeight: 10},
		{Name: "app-v30", TargetAppVersion: 30, AppliedHeight: 0},
		{Name: "app-v30", TargetAppVersion: 30, AppliedHeight: -1},
		{Name: "app-v30", TargetAppVersion: 30, AppliedHeight: 13},
	} {
		s := identityTestStore(t)
		identityTestHeight(t, s, 11)
		encoded, err := json.Marshal(record)
		require.NoError(t, err)
		identityRootWriteFixture(t, s, upgradeAppliedKey("app-v30"), encoded)
		identityRootModeError(t, s)
	}
}
