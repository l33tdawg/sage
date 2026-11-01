package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func freshGenesisFixture(t *testing.T, version uint64, count int, companion bool) (FreshGenesisManifest, agentKey, *agentKey) {
	t.Helper()
	root := newAgentKey(t)
	manifest := FreshGenesisManifest{Version: FreshGenesisManifestVersion, AppVersion: version, RootID: root.id}
	for range count {
		key := newAgentKey(t)
		manifest.Validators = append(manifest.Validators, FreshGenesisValidator{ValidatorID: key.id, Power: 10})
	}
	sort.Slice(manifest.Validators, func(i, j int) bool { return manifest.Validators[i].ValidatorID < manifest.Validators[j].ValidatorID })
	var agent *agentKey
	if companion {
		key := newAgentKey(t)
		agent = &key
		manifest.Agent = &FreshGenesisAgent{AgentID: key.id, Profile: store.AppV23ProfileCompanion, Clearance: 1, Capabilities: 15, HomeDomain: "fresh-home"}
	}
	return manifest, root, agent
}

func freshGenesisRequest(t *testing.T, chain string, manifest FreshGenesisManifest, root agentKey, agent *agentKey) *abcitypes.RequestInitChain {
	t.Helper()
	message := FreshGenesisManifestSignBytes(chain, manifest)
	manifest.RootSignature = hex.EncodeToString(ed25519.Sign(root.priv, message))
	if manifest.Agent != nil && agent != nil {
		copied := *manifest.Agent
		manifest.Agent = &copied
		manifest.Agent.Signature = hex.EncodeToString(ed25519.Sign(agent.priv, message))
	}
	data, err := json.Marshal(map[string]any{"sage": map[string]any{"initial_admin": manifest.RootID, "fresh_bootstrap": manifest}})
	require.NoError(t, err)
	req := &abcitypes.RequestInitChain{ChainId: chain, AppStateBytes: data, InitialHeight: 1, ConsensusParams: &cmtproto.ConsensusParams{Version: &cmtproto.VersionParams{App: manifest.AppVersion}}}
	for _, v := range manifest.Validators {
		key, err := hex.DecodeString(v.ValidatorID)
		require.NoError(t, err)
		req.Validators = append(req.Validators, abcitypes.ValidatorUpdate{Power: v.Power, PubKey: cryptoproto.PublicKey{Sum: &cryptoproto.PublicKey_Ed25519{Ed25519: key}}})
	}
	return req
}

func TestFreshGenesisRulesHashAndRecovery(t *testing.T) {
	for _, version := range []uint64{24, 25, 26, 27, 28, 29, 30} {
		for _, profile := range []struct {
			count     int
			companion bool
		}{{1, false}, {4, false}, {4, true}} {
			t.Run(fmt.Sprintf("v%d/validators%d/companion%t", version, profile.count, profile.companion), func(t *testing.T) {
				app := setupTestApp(t)
				manifest, root, agent := freshGenesisFixture(t, version, profile.count, profile.companion)
				req := freshGenesisRequest(t, "fresh-rules", manifest, root, agent)
				initialized, err := app.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, version, initialized.ConsensusParams.Version.App)
				require.Len(t, initialized.AppHash, 32)
				require.Equal(t, profile.count, app.validators.Size())
				require.Equal(t, version, app.currentAppVersion())
				require.True(t, app.postAppV20Fork(1))
				require.True(t, app.IsAppV24ActiveForNextTx())
				require.Equal(t, version >= 28, app.postAppV28Fork(1))
				require.Equal(t, version >= 29, app.postAppV29Fork(1))
				require.Equal(t, version >= 30, app.IsAppV30ActiveForNextTx())
				for v := uint64(2); v <= version; v++ {
					record, err := app.badgerStore.GetAppliedUpgrade(fmt.Sprintf("app-v%d", v))
					require.NoError(t, err)
					require.Nil(t, record)
				}
				legacy, err := app.badgerStore.GetAppV23GenesisActivation()
				require.NoError(t, err)
				require.Nil(t, legacy)
				replayed, err := app.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, initialized.AppHash, replayed.AppHash)
				// Comet validator updates may be reordered; the signed manifest stays canonical.
				sort.Slice(req.Validators, func(i, j int) bool {
					return string(req.Validators[i].PubKey.GetEd25519()) > string(req.Validators[j].PubKey.GetEd25519())
				})
				other := setupTestApp(t)
				same, err := other.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, initialized.AppHash, same.AppHash)
				if version >= 28 {
					public, err := app.badgerStore.ComputePublicMemoryAppHash()
					require.NoError(t, err)
					require.Equal(t, initialized.AppHash, public)
				}
				// Reopening at H0 must retain the exact target and pristine retry contract.
				reopened, err := NewSageAppWithStores(app.badgerStore, app.offchainStore, zerolog.Nop())
				require.NoError(t, err)
				require.Equal(t, version, reopened.currentAppVersion())
				retried, err := reopened.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, initialized.AppHash, retried.AppHash)
				for height := int64(1); height <= 2; height++ {
					finalized, err := reopened.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: height, Time: time.Unix(height, 0).UTC()})
					require.NoError(t, err)
					require.Len(t, finalized.AppHash, 32)
					_, err = reopened.Commit(context.Background(), &abcitypes.RequestCommit{})
					require.NoError(t, err)
					require.Equal(t, version, reopened.currentAppVersion())
				}
				height, hash, err := inspectAppV20StateSyncStore(context.Background(), app.badgerStore, "fresh test")
				require.NoError(t, err)
				require.Equal(t, uint64(2), height)
				require.Equal(t, reopened.state.AppHash, hash)
				recovered, err := NewSageAppWithStores(app.badgerStore, app.offchainStore, zerolog.Nop())
				require.NoError(t, err)
				require.Equal(t, version, recovered.currentAppVersion())
				require.Equal(t, version >= 30, recovered.IsAppV30ActiveForNextTx())
				_, err = recovered.InitChain(context.Background(), req)
				require.Error(t, err, "committed chain cannot be initialized again")
			})
		}
	}
}

func TestFreshGenesisRejectedRequestsLeaveNoEffects(t *testing.T) {
	manifest, root, agent := freshGenesisFixture(t, 30, 4, true)
	tests := map[string]func(*abcitypes.RequestInitChain){
		"chain":               func(r *abcitypes.RequestInitChain) { r.ChainId = "another-chain" },
		"version":             func(r *abcitypes.RequestInitChain) { r.ConsensusParams.Version.App = 29 },
		"power":               func(r *abcitypes.RequestInitChain) { r.Validators[0].Power++ },
		"missing validator":   func(r *abcitypes.RequestInitChain) { r.Validators = r.Validators[:3] },
		"duplicate validator": func(r *abcitypes.RequestInitChain) { r.Validators[1] = r.Validators[0] },
		"height":              func(r *abcitypes.RequestInitChain) { r.InitialHeight = 2 },
		"unknown manifest field": func(r *abcitypes.RequestInitChain) {
			var data map[string]map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(r.AppStateBytes, &data))
			var m map[string]any
			require.NoError(t, json.Unmarshal(data["sage"]["fresh_bootstrap"], &m))
			m["unsigned"] = true
			encoded, err := json.Marshal(m)
			require.NoError(t, err)
			data["sage"]["fresh_bootstrap"] = encoded
			r.AppStateBytes, err = json.Marshal(data)
			require.NoError(t, err)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			app := setupTestApp(t)
			before, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			req := freshGenesisRequest(t, "fresh-rejections", manifest, root, agent)
			mutate(req)
			_, err = app.InitChain(context.Background(), req)
			require.Error(t, err)
			after, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Empty(t, app.validators.GetAll())
			require.Zero(t, app.freshGenesisAppVersion)
		})
	}
	for _, profile := range []struct {
		name  string
		cap   uint32
		home  string
		valid bool
	}{{store.AppV23ProfileCompanion, 15, "fresh-home", true}, {store.AppV23ProfileStandard, 0, "fresh-home", true}, {store.AppV23ProfileReadOnly, uint32(store.AgentCapabilityReadAllDomains), "", true}, {store.AppV23ProfileReadOnly, uint32(store.AgentCapabilityReadAllDomains), "fresh-home", false}, {store.AppV23ProfileStandard, 15, "fresh-home", false}, {store.AppV23ProfileRoot, 0, "fresh-home", false}, {store.AppV23ProfileLegacyRestricted, 0, "fresh-home", false}} {
		t.Run(fmt.Sprintf("policy-%s-%d-%s", profile.name, profile.cap, profile.home), func(t *testing.T) {
			app := setupTestApp(t)
			m := manifest
			a := *m.Agent
			m.Agent = &a
			m.Agent.Profile = profile.name
			m.Agent.Capabilities = profile.cap
			m.Agent.HomeDomain = profile.home
			_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-policy", m, root, agent))
			if profile.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				origin, err := app.badgerStore.GetFreshGenesisActivation()
				require.NoError(t, err)
				require.Nil(t, origin)
				require.Empty(t, app.validators.GetAll())
			}
		})
	}
}

func TestFreshGenesisDirtyStoreAndChangedReplayFailClosed(t *testing.T) {
	manifest, root, agent := freshGenesisFixture(t, 29, 4, true)
	for _, dirty := range []bool{false, true} {
		t.Run(fmt.Sprintf("dirty%t", dirty), func(t *testing.T) {
			app := setupTestApp(t)
			req := freshGenesisRequest(t, "fresh-replay", manifest, root, agent)
			if dirty {
				require.NoError(t, app.badgerStore.SetState("unrelated", []byte("present")))
			} else {
				_, err := app.InitChain(context.Background(), req)
				require.NoError(t, err)
			}
			before, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			ids := app.ValidatorIDs()
			version := app.freshGenesisAppVersion
			for _, change := range []string{"target", "roster"} {
				changed := manifest
				changed.Validators = append([]FreshGenesisValidator(nil), manifest.Validators...)
				if change == "target" {
					changed.AppVersion = 30
				} else {
					changed.Validators[0].Power++
				}
				_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-replay", changed, root, agent))
				require.Error(t, err)
				after, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
				require.NoError(t, err)
				require.Equal(t, before, after)
				require.Equal(t, ids, app.ValidatorIDs())
				require.Equal(t, version, app.freshGenesisAppVersion)
			}
			if !dirty {
				require.NoError(t, app.badgerStore.SetState("extra", []byte("valid-but-extraneous")))
				_, err := app.InitChain(context.Background(), req)
				require.ErrorContains(t, err, "pristine")
				require.Equal(t, ids, app.ValidatorIDs())
				require.Equal(t, version, app.freshGenesisAppVersion)
			}
		})
	}
	unsorted := manifest
	unsorted.Validators = append([]FreshGenesisValidator(nil), manifest.Validators...)
	unsorted.Validators[0], unsorted.Validators[1] = unsorted.Validators[1], unsorted.Validators[0]
	app := setupTestApp(t)
	_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-canonical", unsorted, root, agent))
	require.ErrorContains(t, err, "sorted")
}

func TestFreshGenesisOriginIsCommittedAndRejectsSyntheticHistory(t *testing.T) {
	app := setupTestApp(t)
	manifest, root, agent := freshGenesisFixture(t, 30, 1, false)
	_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-origin", manifest, root, agent))
	require.NoError(t, err)
	before, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	origin, err := app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	origin.AppVersion = 29
	encoded, err := json.Marshal(origin)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.DB().Update(func(txn *badger.Txn) error { return txn.Set([]byte(consensuskeys.FreshGenesisActivation), encoded) }))
	after, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

// These tests use real signed tx admission for the optional agent at height1.
func TestFreshGenesisCompanionWritesAtFirstBlock(t *testing.T) {
	for _, version := range []uint64{29, 30} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			app := setupTestApp(t)
			manifest, root, agent := freshGenesisFixture(t, version, 1, true)
			_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-companion", manifest, root, agent))
			require.NoError(t, err)
			submit := makeMemorySubmitTx(t, *agent, "fresh-home", "fresh chain first memory")
			raw := encodeDirectH0Tx(t, submit, *agent, 1)
			finalized, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 1, Time: time.Now().UTC(), Txs: [][]byte{raw}})
			require.NoError(t, err)
			require.Len(t, finalized.TxResults, 1)
			require.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
			_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
			require.NoError(t, err)
		})
	}
}

func TestFreshGenesisImmediateSuccessorRetainsGovernedBoundary(t *testing.T) {
	app := setupTestApp(t)
	manifest, root, agent := freshGenesisFixture(t, 29, 4, false)
	_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-successor", manifest, root, agent))
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{Name: appV30UpgradeName, TargetAppVersion: 30, ActivationHeight: 1, ProposedAt: 0}))
	finalized, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 1, Time: time.Unix(1, 0).UTC()})
	require.NoError(t, err)
	require.Equal(t, uint64(30), finalized.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.True(t, app.IsAppV30ActiveForNextTx())
	require.False(t, app.postAppV30Fork(1))
	require.True(t, app.postAppV30Fork(2))
	origin, err := app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	require.Equal(t, uint64(29), origin.AppVersion)
	predecessor, err := app.badgerStore.GetAppliedUpgrade(appV29UpgradeName)
	require.NoError(t, err)
	require.Nil(t, predecessor)
	applied, err := app.badgerStore.GetAppliedUpgrade(appV30UpgradeName)
	require.NoError(t, err)
	require.Equal(t, int64(1), applied.AppliedHeight)
	_, err = app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 2, Time: time.Unix(2, 0).UTC()})
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	recovered, err := NewSageAppWithStores(app.badgerStore, app.offchainStore, zerolog.Nop())
	require.NoError(t, err)
	require.Equal(t, uint64(30), recovered.currentAppVersion())
	require.True(t, recovered.IsAppV30ActiveForNextTx())
	_, _, err = inspectAppV20StateSyncStore(context.Background(), app.badgerStore, "successor")
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV29UpgradeName, 29, 1))
	require.Error(t, app.badgerStore.ValidateFreshGenesisLineage())
	_, _, err = inspectAppV20StateSyncStore(context.Background(), app.badgerStore, "synthetic predecessor")
	require.Error(t, err)
}

func TestFreshGenesisMemoryTaintApplyAndClearRequireExplicitVotes(t *testing.T) {
	for _, version := range []uint64{29, 30} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			app := setupTestApp(t)
			manifest, root, agent := freshGenesisFixture(t, version, 3, false)
			_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-taint", manifest, root, agent))
			require.NoError(t, err)
			target := tx.ParentRef{SourceChainID: "fresh-taint", MemoryID: "fresh-taint-target", ContentHash: sha256.Sum256([]byte("content"))}
			require.NoError(t, app.badgerStore.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
			require.NoError(t, app.badgerStore.SetMemoryDomain(target.MemoryID, "general"))
			require.NoError(t, app.badgerStore.SetMemoryAuthor(target.MemoryID, root.id))
			require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(target.MemoryID, root.id))
			require.NoError(t, app.badgerStore.SetMemoryClassification(target.MemoryID, 1))
			require.NoError(t, app.badgerStore.SetMemorySubmissionHeight(target.MemoryID, 1))
			applyPayload, err := tx.EncodeTaintApplyPayload(appV30ApplyPayload(target))
			require.NoError(t, err)
			decisionID, err := tx.TaintApplyDecisionID(applyPayload)
			require.NoError(t, err)
			decision, err := hex.DecodeString(decisionID)
			require.NoError(t, err)
			evidence := sha256.Sum256([]byte("clear"))
			clearPayload, err := tx.EncodeTaintClearPayload(tx.TaintClearPayload{Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear, DecisionID: decision, ReasonCategory: 2, EvidenceDigest: evidence[:]})
			require.NoError(t, err)
			clearTarget, err := tx.TaintClearTargetID(clearPayload)
			require.NoError(t, err)
			for i, action := range []struct {
				target  string
				payload []byte
			}{{decisionID, applyPayload}, {clearTarget, clearPayload}} {
				height := int64(1 + i*100)
				pub, sig, bodyHash, ts := signAgentProof(t, root, []byte("fresh taint governance"))
				parsed := &tx.ParsedTx{Type: tx.TxTypeGovPropose, Nonce: uint64(i + 1), GovPropose: &tx.GovPropose{Operation: tx.GovOpMemoryTaint, TargetID: action.target, Payload: action.payload, Reason: "fresh taint governance"}, AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts}
				require.NoError(t, tx.SignTx(parsed, root.priv))
				result := app.processGovPropose(parsed, height, time.Now().UTC())
				if version == 29 {
					require.NotZero(t, result.Code)
					return
				}
				require.Zero(t, result.Code, result.Log)
				proposal, err := app.govEngine.GetActiveProposal()
				require.NoError(t, err)
				require.NotNil(t, proposal)
				require.Equal(t, height, proposal.CreatedHeight)
				votes, err := app.govEngine.GetProposalVotes(proposal.ProposalID)
				require.NoError(t, err)
				require.Empty(t, votes)
				for _, v := range manifest.Validators[:2] {
					require.NoError(t, app.govEngine.Vote(proposal.ProposalID, v.ValidatorID, "accept", height))
				}
				executed, err := app.govEngine.ProcessBlock(height + governance.MinVotingBlocks)
				require.NoError(t, err)
				require.Nil(t, executed, "two-thirds never executes fresh-v30 apply or clear")
				require.NoError(t, app.govEngine.Vote(proposal.ProposalID, manifest.Validators[2].ValidatorID, "accept", height+governance.MinVotingBlocks))
				executed, err = app.govEngine.ProcessBlock(height + governance.MinVotingBlocks + 1)
				require.NoError(t, err)
				require.NotNil(t, executed)
				_, err = app.applyGovernanceProposal(executed, height+governance.MinVotingBlocks+1)
				require.NoError(t, err)
				effective, err := app.badgerStore.GetEffectiveTaintDecisions(target.MemoryID)
				require.NoError(t, err)
				if i == 0 {
					require.Equal(t, []string{decisionID}, effective)
				} else {
					require.Empty(t, effective)
				}
			}
		})
	}
}

func TestFreshGenesisSignaturesBindAllInitialAuthority(t *testing.T) {
	manifest, root, agent := freshGenesisFixture(t, 30, 4, true)
	req := freshGenesisRequest(t, "fresh-signature", manifest, root, agent)
	signed, err := prevalidateFreshGenesisRequest(req)
	require.NoError(t, err)
	changes := map[string]func(*FreshGenesisManifest){
		"target":    func(m *FreshGenesisManifest) { m.AppVersion = 29 },
		"root":      func(m *FreshGenesisManifest) { m.RootID = newAgentKey(t).id },
		"agent":     func(m *FreshGenesisManifest) { m.Agent.AgentID = newAgentKey(t).id },
		"home":      func(m *FreshGenesisManifest) { m.Agent.HomeDomain = "another-home" },
		"clearance": func(m *FreshGenesisManifest) { m.Agent.Clearance++ },
		"power":     func(m *FreshGenesisManifest) { m.Validators[0].Power++ },
		"root signature": func(m *FreshGenesisManifest) {
			m.RootSignature = hex.EncodeToString(make([]byte, ed25519.SignatureSize))
		},
		"agent signature": func(m *FreshGenesisManifest) {
			m.Agent.Signature = hex.EncodeToString(make([]byte, ed25519.SignatureSize))
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			m := *signed
			copyAgent := *m.Agent
			m.Agent = &copyAgent
			m.Validators = append([]FreshGenesisValidator(nil), m.Validators...)
			change(&m)
			_, err := VerifyFreshGenesisManifest("fresh-signature", m)
			require.Error(t, err)
		})
	}
	for _, version := range []uint64{0, 23, 31} {
		m := manifest
		m.AppVersion = version
		app := setupTestApp(t)
		_, err := app.InitChain(context.Background(), freshGenesisRequest(t, "fresh-version", m, root, agent))
		require.Error(t, err)
		require.Empty(t, app.ValidatorIDs())
	}
	require.Equal(t, uint64(29), MaxSupportedAppVersion(), "new genesis support does not authorize app-v30 auto-vote activation")
}
