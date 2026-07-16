package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func canonicalAppV20BootstrapForRecognitionTest(t *testing.T, signer agentKey, blockTime time.Time) *tx.ParsedTx {
	t.Helper()
	proposal := makeUpgradeProposeTx(t, signer, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	proposal.Nonce = 1
	proposal.Timestamp = blockTime
	require.NoError(t, tx.SignTx(proposal, signer.priv))
	return proposal
}

// The pre-activation selector is a censorship boundary: only an executable,
// authenticated app-v20 ceremony may be isolated from legacy mempool traffic.
// Every negative below must remain historical pass-through for proposers and
// ACCEPT for peers, and the read-only recognition path must not certify the
// legacy audit or consume the apparent signer's nonce.
func TestAppV20BootstrapRecognitionRejectsCensorshipSensitiveNearMisses(t *testing.T) {
	blockTime := time.Unix(21_302, 0).UTC()
	type arrangedBootstrap struct {
		raw      []byte
		signerID string
	}
	tests := []struct {
		name    string
		arrange func(*testing.T, *SageApp, agentKey) arrangedBootstrap
	}{
		{
			name: "bad outer signature",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				proposal.Signature[0] ^= 0xff
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "zero nonce",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				proposal.Nonce = 0
				require.NoError(t, tx.SignTx(proposal, admin.priv))
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "stale nonce",
			arrange: func(t *testing.T, app *SageApp, admin agentKey) arrangedBootstrap {
				require.NoError(t, app.badgerStore.SetNonce(admin.id, 1))
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "missing delegated request proof",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				delegated := newAgentKey(t)
				proposal.AgentPubKey, proposal.AgentSig, proposal.AgentBodyHash, proposal.AgentTimestamp =
					signAgentProof(t, delegated, []byte(appV20UpgradeName))
				proposal.AgentRequest = nil
				require.NoError(t, tx.SignTx(proposal, admin.priv))
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "corrupt delegated request proof",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				delegated := newAgentKey(t)
				proposal.AgentPubKey, proposal.AgentSig, proposal.AgentBodyHash, proposal.AgentTimestamp =
					signAgentProof(t, delegated, []byte(appV20UpgradeName))
				proposal.AgentRequest = []byte("POST /v1/upgrade/propose\n{}")
				require.NoError(t, tx.SignTx(proposal, admin.priv))
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "registered non-admin",
			arrange: func(t *testing.T, app *SageApp, _ agentKey) arrangedBootstrap {
				member := newAgentKey(t)
				registerAgent(t, app, member, "bootstrap-member", "member")
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, member, blockTime)
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: member.id}
			},
		},
		{
			name: "noncanonical name",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				proposal.UpgradePropose.Name = "app-v020"
				require.NoError(t, tx.SignTx(proposal, admin.priv))
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "noncanonical domain",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				proposal.UpgradePropose.GovernanceDomain = strings.ToUpper(governanceReplayTestDomain)
				require.NoError(t, tx.SignTx(proposal, admin.priv))
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "noncanonical wire encoding",
			arrange: func(t *testing.T, _ *SageApp, admin agentKey) arrangedBootstrap {
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				raw := append(encodeGovernanceReplayTx(t, proposal), 0xff)
				_, err := tx.DecodeTx(raw)
				require.NoError(t, err, "fixture must be decode-tolerant but fail canonical re-encoding")
				return arrangedBootstrap{raw: raw, signerID: admin.id}
			},
		},
		{
			name: "active governance slot",
			arrange: func(t *testing.T, app *SageApp, admin agentKey) arrangedBootstrap {
				require.NoError(t, app.badgerStore.SetState("gov:active", []byte("occupied-proposal")))
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "pending upgrade plan",
			arrange: func(t *testing.T, app *SageApp, admin agentKey) arrangedBootstrap {
				require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
					Name:             "app-v21",
					TargetAppVersion: 21,
					ActivationHeight: 100,
					ProposedAt:       1,
					ProposerID:       admin.id,
				}))
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
		{
			name: "legacy readiness audit failure",
			arrange: func(t *testing.T, app *SageApp, admin agentKey) arrangedBootstrap {
				require.NoError(t, app.badgerStore.SetMemoryHash(
					"oversized-legacy-memory",
					make([]byte, maxAppV20IdentifierBytes+1),
					"proposed",
				))
				proposal := canonicalAppV20BootstrapForRecognitionTest(t, admin, blockTime)
				return arrangedBootstrap{raw: encodeGovernanceReplayTx(t, proposal), signerID: admin.id}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app, admin := setupGovTestApp(t)
			app.appV19AppliedHeight = 1
			arranged := test.arrange(t, app, admin)

			nonceBefore, err := app.badgerStore.GetNonce(arranged.signerID)
			require.NoError(t, err)
			markerBefore, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
			require.NoError(t, err)

			assert.False(t, app.isAuthenticatedAppV20BootstrapProposal(arranged.raw, 2, blockTime))

			ordinaryKey := newAgentKey(t)
			ordinary := appV20ValidRegister(t, ordinaryKey, 1, "legacy-follower")
			mixed := [][]byte{arranged.raw, ordinary}
			prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
				Height: 2, Time: blockTime, MaxTxBytes: 2 << 20, Txs: mixed,
			})
			require.NoError(t, err)
			assert.Equal(t, mixed, prepared.Txs, "near-miss must not isolate or censor legacy traffic")

			processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
				Height: 2, Time: blockTime, Txs: mixed,
			})
			require.NoError(t, err)
			assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

			nonceAfter, err := app.badgerStore.GetNonce(arranged.signerID)
			require.NoError(t, err)
			assert.Equal(t, nonceBefore, nonceAfter, "recognition must not consume a nonce")
			markerAfter, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
			require.NoError(t, err)
			assert.Equal(t, markerBefore, markerAfter, "recognition must not certify the legacy audit")
		})
	}
}
