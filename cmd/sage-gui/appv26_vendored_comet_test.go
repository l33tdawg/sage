package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	rpccore "github.com/cometbft/cometbft/rpc/core"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// activateVendoredUpgradeAtNextHeight bypasses the governance proposal, quorum,
// and production delay used to create the plan. The supplied plan is still
// consumed by a real CometBFT block through the application's normal
// FinalizeBlock/Commit path. This test therefore proves activation, migration,
// version publication, and restart semantics; the governance ceremony remains
// covered by the ABCI upgrade tests.
func activateVendoredUpgradeAtNextHeight(
	t *testing.T,
	app *sageabci.SageApp,
	badgerStore *store.BadgerStore,
	rpcEnvironment *rpccore.Environment,
	rootKey ed25519.PrivateKey,
	target uint64,
) int64 {
	t.Helper()
	status, err := rpcEnvironment.Status(nil)
	require.NoError(t, err)
	activationHeight := status.SyncInfo.LatestBlockHeight + 1
	require.NoError(t, badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name:             tx.CanonicalUpgradeName(target),
		TargetAppVersion: target,
		ActivationHeight: activationHeight,
		ProposedAt:       activationHeight - 1,
	}))

	heartbeatRaw, err := buildOperatorRegisterTx(upgradeWatchdogConfig{
		ResolveSigningKey: func() (ed25519.PrivateKey, error) {
			return rootKey, nil
		},
	})
	require.NoError(t, err)
	activation, err := rpcEnvironment.BroadcastTxCommit(
		&rpctypes.Context{},
		cmttypes.Tx(heartbeatRaw),
	)
	require.NoError(t, err)
	require.Zero(t, activation.CheckTx.Code, activation.CheckTx.Log)
	// Root is intentionally not an ordinary Agent, so its heartbeat advances
	// the block while execution is denied. Upgrade activation is block-scoped
	// and must not depend on the payload succeeding.
	require.Equal(t, uint32(110), activation.TxResult.Code)
	require.Equal(t, "access denied", activation.TxResult.Log)
	require.Equal(t, activationHeight, activation.Height)

	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, target, info.AppVersion)
	applied, err := badgerStore.GetAppliedUpgrade(tx.CanonicalUpgradeName(target))
	require.NoError(t, err)
	require.Equal(t, &store.AppliedUpgradeRecord{
		Name:             tx.CanonicalUpgradeName(target),
		TargetAppVersion: target,
		AppliedHeight:    activationHeight,
	}, applied)
	_, err = badgerStore.GetUpgradePlan()
	require.ErrorIs(t, err, store.ErrNoUpgradePlan)
	return activationHeight
}

func vendoredAccessGroupRaw(
	t *testing.T,
	rootKey ed25519.PrivateKey,
	groupID string,
	members []string,
	memberAuthority string,
	expectedRevision uint64,
) []byte {
	t.Helper()
	mutation := &tx.AccessGroupMutate{
		GroupID:          groupID,
		Name:             "Real Comet Team",
		Members:          append([]string(nil), members...),
		MemberAuthority:  memberAuthority,
		ExpectedRevision: expectedRevision,
	}
	bodyHash := sha256.Sum256([]byte("vendored real-Comet Access Group mutation"))
	proofTime := time.Now().Unix()
	var proofTimeBytes [8]byte
	binary.BigEndian.PutUint64(proofTimeBytes[:], uint64(proofTime))
	proofMessage := append(append([]byte(nil), bodyHash[:]...), proofTimeBytes[:]...)
	parsed := &tx.ParsedTx{
		Type:              tx.TxTypeAccessGroupMutate,
		AccessGroupMutate: mutation,
		AgentPubKey:       rootKey.Public().(ed25519.PublicKey),
		AgentSig:          ed25519.Sign(rootKey, proofMessage),
		AgentBodyHash:     bodyHash[:],
		AgentTimestamp:    proofTime,
		Nonce:             tx.MonotonicNonce(rootKey),
		Timestamp:         time.Unix(proofTime, 0).UTC(),
	}
	require.NoError(t, tx.SignTx(parsed, rootKey))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func broadcastVendoredAccessGroup(
	t *testing.T,
	rpcEnvironment *rpccore.Environment,
	raw []byte,
) {
	t.Helper()
	broadcast, err := rpcEnvironment.BroadcastTxCommit(
		&rpctypes.Context{}, cmttypes.Tx(raw),
	)
	require.NoError(t, err)
	require.Zero(t, broadcast.CheckTx.Code, broadcast.CheckTx.Log)
	require.Zero(t, broadcast.TxResult.Code, broadcast.TxResult.Log)
}

// TestVendoredAgentRealCometAppV25ToV26Restart proves the release-critical
// boundary against real CometBFT rather than the direct ABCI fixture: an
// existing, restartable app-v25 chain activates app-v26, commits its version
// update, and cleanly handshakes at app-v26 after another process restart.
func TestVendoredAgentRealCometAppV25ToV26Restart(t *testing.T) {
	cometHome := t.TempDir()
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	rootKeyPath := filepath.Join(sageHome, "agent.key")
	bootstrap := &VendoredAgentBootstrapConfig{
		AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
		HomeDomain:   "voice-interface",
		Clearance:    1,
	}
	require.NoError(t, initCometBFTConfigWithBootstrap(
		cometHome,
		rootKeyPath,
		bootstrap,
	))
	genesis, err := cmttypes.GenesisDocFromFile(
		filepath.Join(cometHome, "config", "genesis.json"),
	)
	require.NoError(t, err)
	rootKey, ok := parseKeyFile(rootKeyPath)
	require.True(t, ok)

	badgerPath := filepath.Join(t.TempDir(), "badger")
	projectionPath := filepath.Join(t.TempDir(), "projection.db")
	app, badgerStore, projection := openVendoredTestApp(t, badgerPath, projectionPath)
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))
	firstRecorder := &recordingGenesisApplication{Application: app}
	firstController := startVendoredCometTestNode(
		t,
		cometHome,
		filepath.Dir(badgerPath),
		firstRecorder,
	)
	t.Cleanup(func() { _ = firstController.StopChain() })
	firstRPC, err := firstController.GetCometNode().ConfigureRPC()
	require.NoError(t, err)

	activation24 := activateVendoredUpgradeAtNextHeight(
		t, app, badgerStore, firstRPC, rootKey, 24,
	)
	activation25 := activateVendoredUpgradeAtNextHeight(
		t, app, badgerStore, firstRPC, rootKey, 25,
	)
	require.Equal(t, activation24+1, activation25)
	require.NoError(t, firstController.StopChain())
	require.NoError(t, projection.Close())
	require.NoError(t, badgerStore.CloseBadger())

	// Prove app-v25 is a complete, governed restart point before attempting
	// app-v26. Comet must perform Info at version 25 and must not replay genesis.
	app, badgerStore, projection = openVendoredTestApp(t, badgerPath, projectionPath)
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))
	secondRecorder := &recordingGenesisApplication{Application: app}
	secondController := startVendoredCometTestNode(
		t,
		cometHome,
		filepath.Dir(badgerPath),
		secondRecorder,
	)
	t.Cleanup(func() { _ = secondController.StopChain() })
	secondInfo, secondInit := secondRecorder.snapshot()
	require.NotEmpty(t, secondInfo)
	require.Equal(t, uint64(25), secondInfo[0])
	require.Empty(t, secondInit)
	secondRPC, err := secondController.GetCometNode().ConfigureRPC()
	require.NoError(t, err)
	secondStatus, err := secondRPC.Status(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(25), secondStatus.NodeInfo.ProtocolVersion.App)
	require.Equal(t, activation25, secondStatus.SyncInfo.LatestBlockHeight)

	// Create the historical wire form through real Comet while app-v25 is the
	// committed protocol. Its empty authority is the exact state app-v26 must
	// migrate deterministically at activation H.
	companionKey, ok := parseKeyFile(bootstrap.AgentKeyFile)
	require.True(t, ok)
	companionID := appV23AgentIDForKey(companionKey)
	broadcastVendoredAccessGroup(t, secondRPC, vendoredAccessGroupRaw(
		t, rootKey, "real-comet-team", []string{companionID}, "", 0,
	))
	legacyGroup, err := badgerStore.GetAppV23AccessGroup("real-comet-team")
	require.NoError(t, err)
	require.Empty(t, legacyGroup.MemberAuthority)
	require.Equal(t, uint64(1), legacyGroup.Revision)

	activation26 := activateVendoredUpgradeAtNextHeight(
		t, app, badgerStore, secondRPC, rootKey, 26,
	)
	require.Equal(t, activation25+2, activation26)
	require.True(t, app.IsAppV26ActiveForNextTx(),
		"app-v26 rules must be active for the first post-activation transaction")
	migratedGroup, err := badgerStore.GetAppV23AccessGroup("real-comet-team")
	require.NoError(t, err)
	require.Equal(t, store.AppV26GroupAuthorityRead, migratedGroup.MemberAuthority)
	require.Equal(t, legacyGroup.Revision, migratedGroup.Revision,
		"fork migration is not an operator-authored revision")

	// The first H+1 transaction uses the extended wire field through both
	// CheckTx and FinalizeBlock, proving this is not merely an Info-version test.
	broadcastVendoredAccessGroup(t, secondRPC, vendoredAccessGroupRaw(
		t, rootKey, "real-comet-team", []string{companionID},
		store.AppV26GroupAuthorityReadWrite, migratedGroup.Revision,
	))
	updatedGroup, err := badgerStore.GetAppV23AccessGroup("real-comet-team")
	require.NoError(t, err)
	require.Equal(t, store.AppV26GroupAuthorityReadWrite, updatedGroup.MemberAuthority)
	require.Equal(t, uint64(2), updatedGroup.Revision)
	require.NoError(t, secondController.StopChain())
	require.NoError(t, projection.Close())
	require.NoError(t, badgerStore.CloseBadger())

	// A third process proves both sides of the handshake persisted app-v26. A
	// stale Info implementation or missing applied-height reload would halt here.
	app, badgerStore, projection = openVendoredTestApp(t, badgerPath, projectionPath)
	t.Cleanup(func() {
		_ = projection.Close()
		_ = badgerStore.CloseBadger()
	})
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))
	thirdRecorder := &recordingGenesisApplication{Application: app}
	thirdController := startVendoredCometTestNode(
		t,
		cometHome,
		filepath.Dir(badgerPath),
		thirdRecorder,
	)
	t.Cleanup(func() { _ = thirdController.StopChain() })
	thirdInfo, thirdInit := thirdRecorder.snapshot()
	require.NotEmpty(t, thirdInfo)
	require.Equal(t, uint64(26), thirdInfo[0])
	require.Empty(t, thirdInit)
	thirdRPC, err := thirdController.GetCometNode().ConfigureRPC()
	require.NoError(t, err)
	thirdStatus, err := thirdRPC.Status(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(26), thirdStatus.NodeInfo.ProtocolVersion.App)
	require.Equal(t, activation26+1, thirdStatus.SyncInfo.LatestBlockHeight)
	require.True(t, app.IsAppV26ActiveForNextTx())
	restartedGroup, err := badgerStore.GetAppV23AccessGroup("real-comet-team")
	require.NoError(t, err)
	require.Equal(t, store.AppV26GroupAuthorityReadWrite, restartedGroup.MemberAuthority)
	require.Equal(t, uint64(2), restartedGroup.Revision)
}
