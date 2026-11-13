package orgpolicy

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
)

func testJoinOverride(t *testing.T, rootKey ed25519.PrivateKey, peer string, revision uint64) JoinOverride {
	t.Helper()
	override := JoinOverride{
		OrgID: "org-acme", PeerAgentPub: peer, PeerChainID: "peer-chain",
		Reason: "owner-approved lab pairing", IssuedAt: testNow - 60,
		ExpiresAt: testNow + 3600, Revision: revision,
	}
	if err := override.Sign(rootKey); err != nil {
		t.Fatalf("sign override: %v", err)
	}
	return override
}

func TestJoinGateLeavesNonFleetNodesUnchanged(t *testing.T) {
	gate := JoinGate{Store: NewStore(newFakeKV()), Now: func() int64 { return testNow }}
	decision, err := gate.Evaluate(JoinGateCheck{
		PeerAgentPub: agentHex(2), PeerChainID: "peer-chain",
		SessionID: "session", Role: "host", Phase: "request",
	})
	if err != nil || decision.Managed || !decision.Allowed {
		t.Fatalf("non-fleet decision=%+v err=%v", decision, err)
	}
}

func TestJoinGateUsesCurrentPolicyAndDurableOverrideUsage(t *testing.T) {
	kv := newFakeKV()
	seedManagedCardNode(t, kv, agentHex(1))
	rootKey := fixedKey(t, 70)
	peer := agentHex(2)
	root := CurrentRoot{CredentialID: agentHex(70), PublicKey: pub(rootKey)}
	override := testJoinOverride(t, rootKey, peer, 1)
	if _, decision, err := NewStore(kv).ApplyJoinOverride(override, root.PublicKey); err != nil || decision != DecisionApplied {
		t.Fatalf("seed override: decision=%q err=%v", decision, err)
	}
	gate := JoinGate{
		Store:       NewStore(kv),
		CurrentRoot: func() (CurrentRoot, error) { return root, nil },
		Now:         func() int64 { return testNow },
	}
	check := JoinGateCheck{
		PeerAgentPub: peer, PeerChainID: "peer-chain",
		SessionID: "session-1", Role: "host", Phase: "request",
	}
	decision, err := gate.Evaluate(check)
	if err != nil || !decision.Managed || !decision.Allowed || decision.OverrideRevision != 1 {
		t.Fatalf("override request decision=%+v err=%v", decision, err)
	}
	if _, err := gate.Evaluate(check); err != nil {
		t.Fatalf("idempotent request use: %v", err)
	}
	check.Phase = "commit"
	if decision, err = gate.Evaluate(check); err != nil || !decision.Allowed {
		t.Fatalf("override commit decision=%+v err=%v", decision, err)
	}
	usage, err := NewStore(kv).LoadJoinOverrideUsage("org-acme", peer, "peer-chain")
	if err != nil || len(usage) != 2 {
		t.Fatalf("usage=%+v err=%v, want request+commit", usage, err)
	}
	if usage[0].Role != "host" || usage[1].Phase != "commit" || usage[0].RootCredentialID != root.CredentialID {
		t.Fatalf("usage audit lost attribution: %+v", usage)
	}
}

func TestJoinGateRefusesExpiredOrRetiredRootOverride(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gateNow    int64
		currentKey int
		want       string
	}{
		{name: "expired", gateNow: testNow + 7200, currentKey: 70, want: "expired"},
		{name: "Root handover", gateNow: testNow, currentKey: 71, want: "current Root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kv := newFakeKV()
			seedManagedCardNode(t, kv, agentHex(1))
			oldRoot := fixedKey(t, 70)
			currentRoot := fixedKey(t, byte(tc.currentKey))
			peer := agentHex(2)
			override := testJoinOverride(t, oldRoot, peer, 1)
			if _, decision, err := NewStore(kv).ApplyJoinOverride(override, pub(oldRoot)); err != nil || decision != DecisionApplied {
				t.Fatalf("seed override: decision=%q err=%v", decision, err)
			}
			gate := JoinGate{
				Store: NewStore(kv),
				CurrentRoot: func() (CurrentRoot, error) {
					return CurrentRoot{CredentialID: agentHex(byte(tc.currentKey)), PublicKey: pub(currentRoot)}, nil
				},
				Now: func() int64 { return tc.gateNow },
			}
			decision, err := gate.Evaluate(JoinGateCheck{
				PeerAgentPub: peer, PeerChainID: "peer-chain",
				SessionID: "session", Role: "host", Phase: "request",
			})
			if err != nil || decision.Allowed || !decision.Managed {
				t.Fatalf("decision=%+v err=%v", decision, err)
			}
			if tc.want != "" && !strings.Contains(decision.Reason, tc.want) {
				t.Fatalf("reason=%q, want %q", decision.Reason, tc.want)
			}
			usage, err := NewStore(kv).LoadJoinOverrideUsage("org-acme", peer, "peer-chain")
			if err != nil || len(usage) != 0 {
				t.Fatalf("denied override recorded usage: %+v err=%v", usage, err)
			}
		})
	}
}

func TestJoinOverrideRejectsNonRootAndRevisionRollback(t *testing.T) {
	kv := newFakeKV()
	rootKey := fixedKey(t, 70)
	nonRoot := fixedKey(t, 71)
	peer := agentHex(2)
	override := testJoinOverride(t, rootKey, peer, 2)
	if _, _, err := NewStore(kv).ApplyJoinOverride(override, pub(nonRoot)); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("non-Root override error=%v", err)
	}
	if _, decision, err := NewStore(kv).ApplyJoinOverride(override, pub(rootKey)); err != nil || decision != DecisionApplied {
		t.Fatalf("seed override: decision=%q err=%v", decision, err)
	}
	stale := testJoinOverride(t, rootKey, peer, 1)
	got, decision, err := NewStore(kv).ApplyJoinOverride(stale, pub(rootKey))
	if err != nil || decision != DecisionRejectedRollback || got.Revision != 2 {
		t.Fatalf("stale override: got=%+v decision=%q err=%v", got, decision, err)
	}
	equivocation := override
	equivocation.Reason = "different reason at the current revision"
	if err := equivocation.Sign(rootKey); err != nil {
		t.Fatal(err)
	}
	got, decision, err = NewStore(kv).ApplyJoinOverride(equivocation, pub(rootKey))
	if err != nil || decision != DecisionRejectedEquivocation || got.Digest() != override.Digest() {
		t.Fatalf("override equivocation: got=%+v decision=%q err=%v", got, decision, err)
	}
}
