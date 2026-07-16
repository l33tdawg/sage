package abci

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// TestAppV20ForkGateAndVersionLockstep covers the dormant v11.9 fork gate,
// activation boundary, canonical name, and advertised binary ceiling. The
// higher v20 gate must subsume the additive lower rules but never light up the
// independent app-v18 administrator override on a direct skip.
func TestAppV20ForkGateAndVersionLockstep(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV20Fork(100))
	assert.False(t, app.IsAppV20ActiveForNextTx())
	assert.Equal(t, uint64(1), app.currentAppVersion())
	assert.Equal(t, tx.CanonicalUpgradeName(20), appV20UpgradeName)
	assert.Equal(t, uint64(20), MaxSupportedAppVersion())

	app.appV20AppliedHeight = 100
	app.state.Height = 100
	assert.False(t, app.postAppV20Fork(100), "activation block remains pre-fork")
	assert.True(t, app.postAppV20Fork(101), "v20 starts strictly after activation")
	assert.True(t, app.IsAppV20ActiveForNextTx(), "operator surfaces may construct the first post-activation scope action")
	assert.True(t, app.postAppV8Rules(101), "v20 subsumes additive v8 rules")
	assert.True(t, app.postAppV17Rules(101), "v20 subsumes delegated-proof rules")
	assert.True(t, app.postAppV19Rules(101), "v20 subsumes the v19 readiness rule")
	assert.False(t, app.postAppV18Rules(101), "v20 must not enable the independent admin override")
	assert.Equal(t, uint64(20), app.currentAppVersion())
}

// TestReplayAppV20BootRefreshAndDormantAppHash protects the two properties that
// make shipping an unactivated fork safe: the persisted audit record restores
// the gate on boot, and a chain with no such record still produces identical
// AppHashes across replicas.
func TestReplayAppV20BootRefreshAndDormantAppHash(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv20-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
	seedTestGovernanceDelegationDomain(t, bs)
	require.NoError(t, SaveState(bs, &AppState{Height: 4199}))
	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(4200), app.appV20AppliedHeight)
	assert.True(t, app.postAppV20Fork(4201))

	blockTime := time.Now().Truncate(time.Second)
	regTx := encodeSelfSignedRegister(t, deterministicAgentKey(t), "operator")
	commit := func() []byte {
		candidate := setupTestApp(t)
		require.Zero(t, candidate.appV20AppliedHeight)
		resp, err := candidate.FinalizeBlock(context.Background(), &types.RequestFinalizeBlock{
			Height: 5, Time: blockTime, Txs: [][]byte{regTx},
		})
		require.NoError(t, err)
		require.Len(t, resp.TxResults, 1)
		require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
		return resp.AppHash
	}
	assert.Equal(t, commit(), commit(), "dormant app-v20 must replay byte-identically")
}

// TestReplayLegacyEmptyDomainTarget20OriginMainFixture freezes the target-20
// transaction shape that origin/main (v11.8) could commit before the v11.9
// GovernanceDomain tail existed. Numeric target 20 alone was not reserved: on
// a post-app-v8 chain it created and, with one validator, executed a normal
// upgrade ballot. The empty domain must therefore keep the old payload,
// proposal, plan, TxResult, and AppHash forever; only a non-empty canonical
// domain tags the new v11.9 ceremony.
func TestReplayLegacyEmptyDomainTarget20OriginMainFixture(t *testing.T) {
	const (
		fixtureHeight         = int64(2)
		fixtureTime           = int64(1_700_000_020)
		wantWireHex           = "1b00000063000000076170702d763230000000000000001400000000000000403739623535363265386665363534663934303738623131326538613938626137393031663835336165363935626564376530653339313062616430343936363400000000000000c83beb2c85fb0fe29fbb8f06e53a30dd7f1b8796ddf579064ee0feeed4a1d9e7b717b2c0facc4662becfbc3459a1a6ea0f94a242957ec8aba8d3bc0ee8fbfde70f79b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664000000000000000717979d02de41c80079b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664b70d16bdc93f34afc32d5a4ffd81919a9d87aad0bfffa34b6a7832a7953f560c161959222992350c370859ef1bd3f90bc16df0e18965218bb44567c4603eef0b000000006553f114dba64fd12b0bb7a2925807c76b724cb3fd3cbf061a99a840be15d0dc6f2b7d1000000000"
		wantProposalAppHash   = "e110df7c06f35c995639c9368318e34af0bd38097c188ea3d1e8bb74983236d4"
		wantActivationAppHash = "1f581532eb11367e2eec56476bbab0ed5549409f9a87cf07fd0c66eac5545b05"
	)

	admin := deterministicAgentKey(t)
	bodyHash := sha256.Sum256([]byte(appV20UpgradeName))
	var timestampBytes [8]byte
	binary.BigEndian.PutUint64(timestampBytes[:], uint64(fixtureTime))
	agentMessage := append(append([]byte(nil), bodyHash[:]...), timestampBytes[:]...)
	parsed := &tx.ParsedTx{
		Type:      tx.TxTypeUpgradePropose,
		Nonce:     7,
		Timestamp: time.Unix(fixtureTime, 0).UTC(),
		UpgradePropose: &tx.UpgradePropose{
			Name:               appV20UpgradeName,
			TargetAppVersion:   20,
			ProposerID:         admin.id,
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
			// GovernanceDomain intentionally absent: this is the old wire form.
		},
		AgentPubKey:    append([]byte(nil), admin.pub...),
		AgentSig:       ed25519.Sign(admin.priv, agentMessage),
		AgentTimestamp: fixtureTime,
		AgentBodyHash:  append([]byte(nil), bodyHash[:]...),
	}
	require.NoError(t, tx.SignTx(parsed, admin.priv))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	assert.Equal(t, wantWireHex, hex.EncodeToString(raw), "frozen v11.8 target-20 wire bytes changed")

	app := setupTestApp(t)
	registerAgent(t, app, admin, "legacy-admin", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: admin.id, PublicKey: admin.pub, Power: 10,
	}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10}))
	app.appV19AppliedHeight = 1
	assert.False(t, app.requiresAppV20AtomicFinalizeAt(fixtureHeight, [][]byte{raw}), "empty legacy domain must not select v11.9 atomic finalize")

	resp, err := app.FinalizeBlock(context.Background(), &types.RequestFinalizeBlock{
		Height: fixtureHeight,
		Time:   time.Unix(fixtureTime, 0).UTC(),
		Txs:    [][]byte{raw},
	})
	require.NoError(t, err)
	require.Len(t, resp.TxResults, 1)
	proposalID := governance.ComputeProposalID(admin.id, fixtureHeight, governance.OpUpgrade, appV20UpgradeName)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)
	assert.Equal(t, fmt.Sprintf(
		"upgrade proposal created (awaiting 2/3 quorum): proposal_id=%s name=app-v20 target_app_version=20",
		proposalID,
	), resp.TxResults[0].Log)
	assert.Equal(t, wantProposalAppHash, hex.EncodeToString(resp.AppHash), "origin/main-compatible proposal AppHash changed")
	assert.Nil(t, app.pendingAppV20Finalize, "legacy proposal must retain the incremental pre-v11.9 finalize path")

	storedProposal, err := app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	assert.Equal(t, governance.StatusExecuted, storedProposal.Status, "single-validator origin/main ballot executed in its proposal block")
	assert.JSONEq(t,
		`{"name":"app-v20","target_app_version":20,"upgrade_delay_blocks":200}`,
		string(storedProposal.Payload),
	)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, appV20UpgradeName, plan.Name)
	assert.Equal(t, uint64(20), plan.TargetAppVersion)
	assert.Equal(t, fixtureHeight+defaultUpgradeDelayBlocks, plan.ActivationHeight)
	assert.Empty(t, plan.GovernanceDomain)
	assert.False(t, app.appV20PendingPlanFreezesValidatorReconfiguration())
	assert.Zero(t, app.appV20AppliedHeight)
	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker)
	domain, err := app.badgerStore.GetState(governanceDelegationDomainStateKey)
	require.NoError(t, err)
	assert.Nil(t, domain)

	assert.False(t, app.requiresAppV20AtomicFinalizeAt(plan.ActivationHeight, nil), "legacy plan activation must remain incremental")
	activationResp, err := app.FinalizeBlock(context.Background(), &types.RequestFinalizeBlock{
		Height: plan.ActivationHeight,
		Time:   time.Unix(fixtureTime+600, 0).UTC(),
	})
	require.NoError(t, err)
	require.NotNil(t, activationResp.ConsensusParamUpdates)
	require.NotNil(t, activationResp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(20), activationResp.ConsensusParamUpdates.Version.App, "origin/main emitted the unsupported version bump")
	assert.Equal(t, wantActivationAppHash, hex.EncodeToString(activationResp.AppHash), "origin/main-compatible activation AppHash changed")
	assert.Zero(t, app.appV20AppliedHeight, "legacy activation must not arm the live v11.9 gate")
	assert.Equal(t, uint64(19), app.currentAppVersion(), "the next handshake retains origin/main's unsupported-version mismatch")
	assert.False(t, app.requiresAppV20AtomicFinalizeAt(plan.ActivationHeight+1, nil), "legacy applied record must not select v11.9 atomic rules")
	applied, err := app.badgerStore.GetAppliedUpgrade(appV20UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied)
	assert.Equal(t, uint64(20), applied.TargetAppVersion)
	assert.Equal(t, plan.ActivationHeight, applied.AppliedHeight)
	_, err = app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, err, store.ErrNoUpgradePlan)
	domain, err = app.badgerStore.GetState(governanceDelegationDomainStateKey)
	require.NoError(t, err)
	assert.Nil(t, domain)
	marker, err = app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker)
}

func TestAppV20CeremonyDiscriminatorRequiresExactTriple(t *testing.T) {
	canonicalDomain := strings.Repeat("ab", sha256.Size)
	for _, tt := range []struct {
		name             string
		upgradeName      string
		targetAppVersion uint64
		domain           string
		want             bool
	}{
		{name: "exact ceremony", upgradeName: appV20UpgradeName, targetAppVersion: 20, domain: canonicalDomain, want: true},
		{name: "empty legacy domain", upgradeName: appV20UpgradeName, targetAppVersion: 20},
		{name: "uppercase legacy tail", upgradeName: appV20UpgradeName, targetAppVersion: 20, domain: strings.ToUpper(canonicalDomain)},
		{name: "invalid legacy tail", upgradeName: appV20UpgradeName, targetAppVersion: 20, domain: strings.Repeat("g", 64)},
		{name: "different legacy name", upgradeName: "future-v20", targetAppVersion: 20, domain: canonicalDomain},
		{name: "different version", upgradeName: appV20UpgradeName, targetAppVersion: 21, domain: canonicalDomain},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isAppV20CeremonyUpgrade(tt.upgradeName, tt.targetAppVersion, tt.domain))
		})
	}
}

func TestAppliedAppV20ConstructorRejectsInvalidCeremonyStateWithoutTakingStoreOwnership(t *testing.T) {
	validDomain := bytes.Repeat([]byte{0x5a}, sha256.Size)
	tests := []struct {
		name    string
		domain  []byte
		marker  []byte
		wantErr []string
	}{
		{
			name:   "malformed domain",
			domain: bytes.Repeat([]byte{0x5a}, sha256.Size-1),
			marker: appV20LegacyResourceAuditValue,
			wantErr: []string{
				"invalid governance delegation domain",
				fmt.Sprintf("length %d, want %d", sha256.Size-1, sha256.Size),
			},
		},
		{
			name:    "missing retained marker",
			domain:  validDomain,
			wantErr: []string{"missing its retained legacy resource audit marker"},
		},
		{
			name:    "retained marker without domain",
			marker:  appV20LegacyResourceAuditValue,
			wantErr: []string{"retains its ceremony marker but has no governance delegation domain"},
		},
		{
			name:    "malformed retained marker",
			domain:  validDomain,
			marker:  []byte("not-complete"),
			wantErr: []string{"invalid retained ceremony marker", "resource audit marker is malformed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			badgerPath := filepath.Join(t.TempDir(), "badger")
			seed, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			require.NoError(t, seed.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
			if tt.domain != nil {
				require.NoError(t, seed.SetState(governanceDelegationDomainStateKey, tt.domain))
			}
			if tt.marker != nil {
				require.NoError(t, seed.SetState(appV20LegacyResourceAuditStateKey, tt.marker))
			}
			require.NoError(t, seed.CloseBadger())

			bs, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = bs.CloseBadger() })

			sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "projection.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlite.Close() })

			app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
			require.Nil(t, app)
			for _, wantErr := range tt.wantErr {
				require.ErrorContains(t, err, wantErr)
			}

			// NewSageAppWithStores borrows both handles. A failed invariant check
			// must leave ownership and liveness with its caller.
			require.NoError(t, bs.SetState("constructor-ownership-probe", []byte("alive")))
			require.NoError(t, sqlite.Ping(context.Background()))
		})
	}
}

func TestAppliedLegacyTarget20WithoutDomainDoesNotEnableAppV20(t *testing.T) {
	badgerPath := filepath.Join(t.TempDir(), "badger")
	seed, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	require.NoError(t, seed.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	require.NoError(t, seed.CloseBadger())

	bs, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlite.Close() })

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, app)
	assert.Zero(t, app.appV20AppliedHeight, "a legacy applied target-20 record has no v11.9 ceremony tag")
	assert.Equal(t, uint64(1), app.currentAppVersion(), "legacy unsupported activation retains the old handshake mismatch")
	assert.Empty(t, app.GovernanceDelegationDomain())
}

func TestAppliedAppV20ConstructorRejectsMalformedAppliedRecord(t *testing.T) {
	tests := []struct {
		name    string
		record  string
		wantErr string
	}{
		{name: "invalid JSON", record: `{`, wantErr: "read applied app-v20 record"},
		{
			name:    "wrong embedded name",
			record:  `{"name":"app-v999","target_app_version":20,"applied_height":11}`,
			wantErr: `record has name "app-v999"`,
		},
		{
			name:    "wrong target version",
			record:  `{"name":"app-v20","target_app_version":19,"applied_height":11}`,
			wantErr: "target app version 19, want 20",
		},
		{
			name:    "non-positive height",
			record:  `{"name":"app-v20","target_app_version":20,"applied_height":0}`,
			wantErr: "non-positive height 0",
		},
		{
			name:    "impossible future height",
			record:  `{"name":"app-v20","target_app_version":20,"applied_height":2}`,
			wantErr: "ahead of persisted app height 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			badgerPath := filepath.Join(t.TempDir(), "badger")
			seed, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			require.NoError(t, seed.DB().Update(func(txn *badger.Txn) error {
				return txn.Set([]byte("upgrade:applied:"+appV20UpgradeName), []byte(tt.record))
			}))
			seedTestGovernanceDelegationDomain(t, seed)
			require.NoError(t, seed.CloseBadger())

			bs, err := store.NewBadgerStore(badgerPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = bs.CloseBadger() })
			sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "projection.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlite.Close() })

			app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
			require.Nil(t, app)
			require.ErrorContains(t, err, tt.wantErr)
			require.NoError(t, bs.SetState("malformed-record-ownership-probe", []byte("alive")))
			require.NoError(t, sqlite.Ping(context.Background()))
		})
	}
}
