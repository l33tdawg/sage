package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// v7.5 upgrade-machinery handler tests
//
// These exercise the ABCI handler stubs added in v7.5 task #0. The stubs do
// NOT mutate state — they only round-trip identity verification, payload
// validation, and structured logging. Tests assert:
//   - happy path → code 0
//   - missing payload → handler-specific error code (47 / 48 / 49)
//   - bad signature → same error code (matches the existing handler pattern
//     where missing-payload and identity-verify-fail share a code)
//   - state is NOT mutated (no pendingWrites enqueued)
// ---------------------------------------------------------------------------

// makeUpgradeProposeTx builds a signed ParsedTx for TxTypeUpgradePropose.
func makeUpgradeProposeTx(t *testing.T, ak agentKey, name string, targetVersion uint64, sha string, delay int64) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradePropose,
		UpgradePropose: &tx.UpgradePropose{
			Name:               name,
			TargetAppVersion:   targetVersion,
			BinarySHA256:       sha,
			ProposerID:         ak.id,
			UpgradeDelayBlocks: delay,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func makeUpgradeCancelTx(t *testing.T, ak agentKey, name, reason string) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradeCancel,
		UpgradeCancel: &tx.UpgradeCancel{
			Name:        name,
			CancellerID: ak.id,
			Reason:      reason,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func makeUpgradeRevertTx(t *testing.T, ak agentKey, name string, targetVersion uint64, fromHeight int64) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradeRevert,
		UpgradeRevert: &tx.UpgradeRevert{
			Name:                name,
			TargetAppVersion:    targetVersion,
			RevertingFromHeight: fromHeight,
			ProposerID:          ak.id,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// ---------------------------------------------------------------------------
// UpgradePropose
// ---------------------------------------------------------------------------

func TestProcessUpgradePropose_HappyPath(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "deadbeefcafe", 200)
	pendingBefore := len(app.pendingWrites)

	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(0), result.Code, "happy path should return code 0, got log: %s", result.Log)
	assert.Contains(t, result.Log, "pre-fork stub", "stub should advertise itself in the log")
	// STUB constraint: handler must not mutate state.
	assert.Equal(t, pendingBefore, len(app.pendingWrites), "stub must NOT enqueue any pending writes")
}

func TestProcessUpgradePropose_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	// Build a tx with no UpgradePropose payload.
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradePropose}

	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code, "missing payload must return code 47")
	assert.Contains(t, result.Log, "missing upgrade propose payload")
}

func TestProcessUpgradePropose_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)
	// Tamper with the signature so verifyAgentIdentity rejects it.
	require.NotEmpty(t, ptx.AgentSig)
	ptx.AgentSig = make([]byte, len(ptx.AgentSig)) // all-zero sig

	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code, "bad signature must return code 47 (same as missing payload)")
	assert.Contains(t, result.Log, "agent identity verification failed")
}

func TestProcessUpgradePropose_MissingName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeProposeTx(t, ak, "", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "name is required")
}

func TestProcessUpgradePropose_ZeroTargetVersion(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 0, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "target_app_version must be > 0")
}

// ---------------------------------------------------------------------------
// UpgradeCancel
// ---------------------------------------------------------------------------

func TestProcessUpgradeCancel_HappyPath(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeCancelTx(t, ak, "v7.5.0", "binary digest mismatch")
	pendingBefore := len(app.pendingWrites)

	result := app.processUpgradeCancel(ptx, 100, time.Now())

	assert.Equal(t, uint32(0), result.Code, "happy path should return code 0, got log: %s", result.Log)
	assert.Contains(t, result.Log, "pre-fork stub")
	assert.Equal(t, pendingBefore, len(app.pendingWrites), "stub must NOT enqueue any pending writes")
}

func TestProcessUpgradeCancel_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradeCancel}

	result := app.processUpgradeCancel(ptx, 100, time.Now())

	assert.Equal(t, uint32(48), result.Code, "missing payload must return code 48")
	assert.Contains(t, result.Log, "missing upgrade cancel payload")
}

func TestProcessUpgradeCancel_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeCancelTx(t, ak, "v7.5.0", "test")
	ptx.AgentSig = make([]byte, len(ptx.AgentSig))

	result := app.processUpgradeCancel(ptx, 100, time.Now())

	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "agent identity verification failed")
}

func TestProcessUpgradeCancel_MissingName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeCancelTx(t, ak, "", "no name")
	result := app.processUpgradeCancel(ptx, 100, time.Now())

	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "name is required")
}

// ---------------------------------------------------------------------------
// UpgradeRevert
// ---------------------------------------------------------------------------

func TestProcessUpgradeRevert_HappyPath(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeRevertTx(t, ak, "v7.4.0-recovery", 6, 12345)
	pendingBefore := len(app.pendingWrites)

	result := app.processUpgradeRevert(ptx, 100, time.Now())

	assert.Equal(t, uint32(0), result.Code, "happy path should return code 0, got log: %s", result.Log)
	assert.Contains(t, result.Log, "pre-fork stub")
	assert.Equal(t, pendingBefore, len(app.pendingWrites), "stub must NOT enqueue any pending writes")
}

func TestProcessUpgradeRevert_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradeRevert}

	result := app.processUpgradeRevert(ptx, 100, time.Now())

	assert.Equal(t, uint32(49), result.Code, "missing payload must return code 49")
	assert.Contains(t, result.Log, "missing upgrade revert payload")
}

func TestProcessUpgradeRevert_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeRevertTx(t, ak, "v7.4.0", 6, 12345)
	ptx.AgentSig = make([]byte, len(ptx.AgentSig))

	result := app.processUpgradeRevert(ptx, 100, time.Now())

	assert.Equal(t, uint32(49), result.Code)
	assert.Contains(t, result.Log, "agent identity verification failed")
}

func TestProcessUpgradeRevert_MissingName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeRevertTx(t, ak, "", 6, 12345)
	result := app.processUpgradeRevert(ptx, 100, time.Now())

	assert.Equal(t, uint32(49), result.Code)
	assert.Contains(t, result.Log, "name is required")
}

// ---------------------------------------------------------------------------
// Dispatch: ensure processTx routes the new tx types to the new handlers.
// This guards against a future refactor accidentally dropping a case.
// ---------------------------------------------------------------------------

func TestProcessTx_RoutesUpgradeTypes(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	cases := []struct {
		name string
		ptx  *tx.ParsedTx
	}{
		{"propose", makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)},
		{"cancel", makeUpgradeCancelTx(t, ak, "v7.5.0", "")},
		{"revert", makeUpgradeRevertTx(t, ak, "v7.4.0", 6, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := app.processTx(tc.ptx, 100, time.Now())
			assert.Equal(t, uint32(0), result.Code, "dispatch should reach the stub and return code 0, got log: %s", result.Log)
			assert.Contains(t, result.Log, "pre-fork stub")
		})
	}
}
