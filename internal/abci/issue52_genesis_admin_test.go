package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// issue #52 — genesis chain-admin seed. These are the consensus-safety gate: the
// replay/state-sync byte-identical invariant, cross-validator determinism, the
// single-validator (quorum-fork-negative) guard, lowercase canonicalization, and
// idempotency across a re-InitChain.

func i52Validator(t *testing.T) abcitypes.ValidatorUpdate {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return abcitypes.ValidatorUpdate{
		Power:  10,
		PubKey: cryptoproto.PublicKey{Sum: &cryptoproto.PublicKey_Ed25519{Ed25519: pub}},
	}
}

func i52InitHash(t *testing.T, vals []abcitypes.ValidatorUpdate, appState []byte) (string, *SageApp) {
	t.Helper()
	app := setupTestApp(t)
	_, err := app.InitChain(context.Background(), &abcitypes.RequestInitChain{
		Validators: vals, AppStateBytes: appState,
	})
	require.NoError(t, err)
	h, err := app.badgerStore.ComputeAppHash()
	require.NoError(t, err)
	return hex.EncodeToString(h), app
}

func i52AdminID(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return hex.EncodeToString(pub)
}

// Test 1 (THE GATE): every "no real admin" app_state variant must yield a
// byte-identical post-InitChain AppHash to the no-app_state baseline, writing ZERO
// agent records. This proves existing chains (which carry no app_state) replay /
// state-sync byte-identically under the new InitChain.
func TestIssue52_NoSeed_ByteIdenticalToBaseline(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	baseline, _ := i52InitHash(t, vals, nil)

	variants := [][]byte{
		nil, []byte(""), []byte(" "), []byte("\n\t"), []byte("null"),
		[]byte("{}"), []byte(`{"sage":{}}`),
		[]byte(`{"sage":{"initial_admin":""}}`),
		[]byte(`{"sage":{"initial_admin":"   "}}`),
		[]byte(`{not valid json`),
		[]byte(`123`), []byte(`"a string"`), []byte(`[1,2,3]`),
		[]byte(`{"sage":{"initial_admin":"nothexZZ"}}`),                         // non-hex
		[]byte(`{"sage":{"initial_admin":"` + strings.Repeat("ab", 16) + `"}}`), // 16 bytes — too short
		[]byte(`{"sage":{"initial_admin":"` + strings.Repeat("ab", 64) + `"}}`), // 64 bytes — too long (the 128-hex mistake)
		[]byte(`{"sage":{"initial_admin":"aa"},"sage":{"initial_admin":"bb"}}`), // duplicate key
	}
	for _, as := range variants {
		h, app := i52InitHash(t, vals, as)
		require.Equalf(t, baseline, h, "app_state %q must not change the AppHash", string(as))
		agents, err := app.badgerStore.ListRegisteredAgents()
		require.NoError(t, err)
		require.Emptyf(t, agents, "app_state %q must write zero agent records", string(as))
	}
}

// Test 2: a valid seed is deterministic across validators, registers the admin role,
// and changes the AppHash vs no-seed.
func TestIssue52_ValidSeed_DeterministicAndRegistersAdmin(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	adminID := i52AdminID(t)
	as := []byte(`{"sage":{"initial_admin":"` + adminID + `"}}`)

	h1, app1 := i52InitHash(t, vals, as)
	h2, _ := i52InitHash(t, vals, as)
	require.Equal(t, h1, h2, "same genesis app_state -> identical height-1 AppHash on every validator")

	require.True(t, app1.badgerStore.IsAgentRegistered(adminID))
	ag, err := app1.badgerStore.GetRegisteredAgent(adminID)
	require.NoError(t, err)
	require.Equal(t, "admin", ag.Role)
	require.EqualValues(t, 1, ag.RegisteredAt)

	baseline, _ := i52InitHash(t, vals, nil)
	require.NotEqual(t, baseline, h1, "a valid seed must change the AppHash vs no-seed")
}

// Test 3 (FORK-NEGATIVE): a multi-validator (quorum) genesis must NOT seed — a
// per-node initial_admin would diverge the height-1 AppHash and halt the chain.
func TestIssue52_Quorum_NoSeed(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t), i52Validator(t)}
	adminID := i52AdminID(t)
	as := []byte(`{"sage":{"initial_admin":"` + adminID + `"}}`)

	baseline, _ := i52InitHash(t, vals, nil)
	seeded, app := i52InitHash(t, vals, as)
	require.Equal(t, baseline, seeded, "2-validator chain must not seed (would fork height-1 AppHash)")
	require.False(t, app.badgerStore.IsAgentRegistered(adminID))
}

// Test 5: an uppercase initial_admin registers under the canonical LOWERCASE id
// (the signer derives lowercase; the propose gate compares verbatim).
func TestIssue52_UppercaseCanonicalizesToLowercase(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	lower := i52AdminID(t)
	upper := strings.ToUpper(lower)
	as := []byte(`{"sage":{"initial_admin":"` + upper + `"}}`)

	_, app := i52InitHash(t, vals, as)
	require.True(t, app.badgerStore.IsAgentRegistered(lower), "must register under canonical lowercase id")
	require.False(t, app.badgerStore.IsAgentRegistered(upper), "must not register under the uppercase string")
	ag, err := app.badgerStore.GetRegisteredAgent(lower)
	require.NoError(t, err)
	require.Equal(t, "admin", ag.Role)
}

// Test 6: re-running InitChain (reset / state-sync) is idempotent — no AppHash
// change, no re-stamp of the admin record.
func TestIssue52_IdempotentReInitChain(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	adminID := i52AdminID(t)
	as := []byte(`{"sage":{"initial_admin":"` + adminID + `"}}`)

	app := setupTestApp(t)
	ctx := context.Background()
	req := &abcitypes.RequestInitChain{Validators: vals, AppStateBytes: as}

	_, err := app.InitChain(ctx, req)
	require.NoError(t, err)
	h1, err := app.badgerStore.ComputeAppHash()
	require.NoError(t, err)
	ag1, err := app.badgerStore.GetRegisteredAgent(adminID)
	require.NoError(t, err)

	_, err = app.InitChain(ctx, req) // reset / state-sync re-InitChain
	require.NoError(t, err)
	h2, err := app.badgerStore.ComputeAppHash()
	require.NoError(t, err)
	ag2, err := app.badgerStore.GetRegisteredAgent(adminID)
	require.NoError(t, err)

	require.Equal(t, h1, h2, "re-InitChain must not change the AppHash")
	require.Equal(t, ag1.RegisteredAt, ag2.RegisteredAt, "must not re-stamp the admin record")
	require.Equal(t, "admin", ag2.Role)
}

// Test 6b (M7, NON-TAUTOLOGICAL): the height-1 RegisteredAt assertion in Test 6 is
// load-bearing only via the AppHash check, because seedGenesisAdmin always stamps the
// literal height 1 (so a re-stamp would re-write 1==1 and slip past a RegisteredAt
// compare). This test proves the IsAgentRegistered short-circuit ACTUALLY fires: it
// mutates the seeded admin's mutable display Name on-chain, re-runs InitChain, and
// asserts the mutation SURVIVES — which can only happen if the second InitChain wrote
// nothing. If seedGenesisAdmin re-ran RegisterAgent it would clobber Name back to the
// seed default.
func TestIssue52_ReInitChainDoesNotClobberExistingAdmin(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	adminID := i52AdminID(t)
	as := []byte(`{"sage":{"initial_admin":"` + adminID + `"}}`)

	app := setupTestApp(t)
	ctx := context.Background()
	req := &abcitypes.RequestInitChain{Validators: vals, AppStateBytes: as}

	_, err := app.InitChain(ctx, req)
	require.NoError(t, err)

	// Mutate the seeded admin's display name (RegisteredName stays immutable).
	require.NoError(t, app.badgerStore.UpdateAgentMeta(adminID, "operator-renamed", ""))
	before, err := app.badgerStore.GetRegisteredAgent(adminID)
	require.NoError(t, err)
	require.Equal(t, "operator-renamed", before.Name)
	hBefore, err := app.badgerStore.ComputeAppHash()
	require.NoError(t, err)

	_, err = app.InitChain(ctx, req) // reset / state-sync re-InitChain
	require.NoError(t, err)

	after, err := app.badgerStore.GetRegisteredAgent(adminID)
	require.NoError(t, err)
	require.Equal(t, "operator-renamed", after.Name,
		"re-InitChain must NOT re-seed/clobber an existing admin record (IsAgentRegistered guard)")
	require.Equal(t, "admin", after.Role, "role must be preserved")
	hAfter, err := app.badgerStore.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, hBefore, hAfter, "re-InitChain must not change the AppHash")
}

// Test 7 (THE BEHAVIORAL CONTRACT — the whole point of #52): a genesis-seeded
// operator key PASSES the post-app-v8 propose admin-gate, and an otherwise
// identical chain WITHOUT the seed has its proposer REJECTED. This is the actual
// deadlock: once a personal chain crosses app-v9 the propose gate (postAppV8Rules)
// only lets an *admin* open the single governance slot, and the genesis seed is the
// only thing that makes the operator's auto-advance key an admin. The gate keys the
// proposer off the TX-SIGNING identity (parsedTx.PublicKey), so the test outer-signs
// with the operator key — exactly what the auto-advance watchdog does on a live node.
func TestIssue52_SeededAdminPassesProposeGate(t *testing.T) {
	vals := []abcitypes.ValidatorUpdate{i52Validator(t)}
	operator := newAgentKey(t) // the operator's ~/.sage/agent.key identity

	// proposeOnce drives a fresh chain (seeded or not) post-app-v8 and submits one
	// canonical app-v9 propose outer-signed by the operator key, returning the result.
	proposeOnce := func(appState []byte) *abcitypes.ExecTxResult {
		app := setupTestApp(t)
		_, err := app.InitChain(context.Background(), &abcitypes.RequestInitChain{
			Validators: vals, AppStateBytes: appState,
		})
		require.NoError(t, err)
		// Move the chain past the app-v8 fork so the admin gate is live (mirrors a
		// personal chain that has auto-advanced up the fork ladder past app-v9).
		app.appV8AppliedHeight = 5
		require.True(t, app.postAppV8Rules(10), "gate must be active at height 10")

		prop := makeUpgradeProposeTx(t, operator, tx.CanonicalUpgradeName(9), 9, "", 200)
		require.NoError(t, tx.SignTx(prop, operator.priv)) // outer-sign = tx-signing identity the gate keys on
		return app.processUpgradePropose(prop, 10, time.Unix(prop.AgentTimestamp, 0))
	}

	// POSITIVE: seeded chain -> operator IS admin -> propose ACCEPTED (chain advances).
	seed := []byte(`{"sage":{"initial_admin":"` + operator.id + `"}}`)
	res := proposeOnce(seed)
	require.Equalf(t, uint32(0), res.Code,
		"a genesis-seeded operator must pass the post-app-v8 propose gate, got: %s", res.Log)

	// NEGATIVE: same operator key, NO genesis seed -> NOT registered -> REJECTED.
	// This is the pre-#52 permanent deadlock a personal chain falls into post-app-v9.
	bare := proposeOnce(nil)
	require.Equal(t, uint32(47), bare.Code,
		"an un-seeded chain's operator must be rejected at the propose gate (the #52 deadlock)")
	require.Contains(t, bare.Log, "proposer not registered",
		"rejection must be the admin-gate, not some unrelated failure")
}
