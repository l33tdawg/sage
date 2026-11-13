package orgpolicy

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func seedManagedCardNode(t *testing.T, kv *fakeKV, enrollee string) AcceptedState {
	t.Helper()
	orgKey := fixedKey(t, 50)
	policy := signed(t, orgKey, func(p *Policy) {
		p.Schema = SchemaIDV2
		p.Payload.PeerAllowList = []string{enrollee}
	})
	outcome := sampleOutcome(t, policy, enrollee)
	if err := NewStore(kv).CommitFirstEnrollment(outcome); err != nil {
		t.Fatalf("seed enrollment: %v", err)
	}
	return outcome.Accepted
}

func TestApplyLocalCardFloorNoOpAndRestart(t *testing.T) {
	kv := newFakeKV()
	accepted := seedManagedCardNode(t, kv, agentHex(1))
	st := NewStore(kv)

	card1 := sampleCard(1, 1, nil)
	got, decision, notice, err := st.ApplyLocalCard(card1, card1.AgentPub, accepted)
	if err != nil || decision != DecisionApplied || notice != nil || got.Digest != card1.Digest() {
		t.Fatalf("first card: card=%+v decision=%q notice=%v err=%v", got, decision, notice, err)
	}
	got, decision, _, err = NewStore(kv).ApplyLocalCard(card1, card1.AgentPub, accepted)
	if err != nil || decision != DecisionNoOpSameDigest || got.CardRev != 1 {
		t.Fatalf("idempotent card: card=%+v decision=%q err=%v", got, decision, err)
	}

	card2 := sampleCard(1, 2, func(c *LinkCard) { c.Endpoint = "https://10.0.0.2:8443" })
	if _, decision, _, err = NewStore(kv).ApplyLocalCard(card2, card2.AgentPub, accepted); err != nil || decision != DecisionApplied {
		t.Fatalf("higher card: decision=%q err=%v", decision, err)
	}
	stored, ok, err := NewStore(kv).LoadCard(card1.OrgID, card1.AgentPub)
	if err != nil || !ok || stored.CardRev != 2 || stored.Digest != card2.Digest() {
		t.Fatalf("restart card floor: card=%+v ok=%v err=%v", stored, ok, err)
	}
	if _, _, _, err := NewStore(kv).ApplyLocalCard(card1, card1.AgentPub, accepted); !errors.Is(err, ErrCardRegression) {
		t.Fatalf("lower revision error = %v, want ErrCardRegression", err)
	}
}

func TestApplyLocalCardEquivocationIsDurable(t *testing.T) {
	kv := newFakeKV()
	accepted := seedManagedCardNode(t, kv, agentHex(1))
	st := NewStore(kv)
	first := sampleCard(1, 3, nil)
	if _, decision, _, err := st.ApplyLocalCard(first, first.AgentPub, accepted); err != nil || decision != DecisionApplied {
		t.Fatalf("seed card: decision=%q err=%v", decision, err)
	}
	conflict := sampleCard(1, 3, func(c *LinkCard) { c.Endpoint = "https://10.0.0.9:8443" })
	card, decision, notice, err := NewStore(kv).ApplyLocalCard(conflict, conflict.AgentPub, accepted)
	if err != nil || decision != DecisionRejectedEquivocation || notice == nil ||
		notice.Kind != "card" || notice.AcceptedDigest != first.Digest() || notice.ConflictDigest != conflict.Digest() {
		t.Fatalf("equivocation: card=%+v decision=%q notice=%+v err=%v", card, decision, notice, err)
	}
	persisted, ok, err := NewStore(kv).LoadCardNotice(first.OrgID)
	if err != nil || !ok || persisted.Kind != notice.Kind ||
		persisted.CardRev != notice.CardRev ||
		persisted.AcceptedDigest != notice.AcceptedDigest ||
		persisted.ConflictDigest != notice.ConflictDigest {
		t.Fatalf("persisted notice: notice=%+v ok=%v err=%v", persisted, ok, err)
	}
	current, ok, err := NewStore(kv).LoadCard(first.OrgID, first.AgentPub)
	if err != nil || !ok || current.Digest != first.Digest() {
		t.Fatalf("equivocation changed floor: card=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestApplyLocalCardBindsAcceptedPolicyGeneration(t *testing.T) {
	kv := newFakeKV()
	accepted := seedManagedCardNode(t, kv, agentHex(1))
	card := sampleCard(1, 1, nil)
	other := accepted
	other.Revision++
	if _, _, _, err := NewStore(kv).ApplyLocalCard(card, card.AgentPub, other); !errors.Is(err, ErrCardPolicyChanged) {
		t.Fatalf("stale policy generation error = %v, want ErrCardPolicyChanged", err)
	}
	if _, ok, err := NewStore(kv).LoadCard(card.OrgID, card.AgentPub); err != nil || ok {
		t.Fatalf("stale generation wrote a card: ok=%v err=%v", ok, err)
	}
}

func TestApplyLocalCardConcurrentPolicySwapFailsCAS(t *testing.T) {
	kv := newFakeKV()
	accepted := seedManagedCardNode(t, kv, agentHex(1))
	first := sampleCard(1, 1, nil)
	if _, decision, _, err := NewStore(kv).ApplyLocalCard(first, first.AgentPub, accepted); err != nil || decision != DecisionApplied {
		t.Fatalf("seed card: decision=%q err=%v", decision, err)
	}
	card := sampleCard(1, 2, func(c *LinkCard) { c.Endpoint = "https://10.0.0.8:8443" })
	rotated := signed(t, fixedKey(t, 60), func(p *Policy) {
		p.Revision = accepted.Revision + 1
		p.Scope = accepted.Scope
		p.KeyEpoch = "epoch-2"
	})
	rotatedRaw, err := json.Marshal(stateFrom(rotated))
	if err != nil {
		t.Fatal(err)
	}
	kv.afterGet = func(key string) {
		if key != cardKey(card.OrgID, card.AgentPub) {
			return
		}
		kv.afterGet = nil
		if err := kv.SetStatesAtomic([]KVWrite{{
			Key: acceptedKey(card.OrgID), Value: rotatedRaw,
		}}); err != nil {
			t.Errorf("rotate policy during card admission: %v", err)
		}
	}
	if _, _, _, err := NewStore(kv).ApplyLocalCard(card, card.AgentPub, accepted); !errors.Is(err, ErrCardPolicyChanged) {
		t.Fatalf("concurrent policy swap error=%v, want ErrCardPolicyChanged", err)
	}
	current, ok, err := NewStore(kv).LoadCard(card.OrgID, card.AgentPub)
	if err != nil || !ok || current.CardRev != 1 {
		t.Fatalf("policy-race changed floor: card=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestApplyLocalCardConcurrentSameRevisionConvergesOnOneFloor(t *testing.T) {
	kv := newFakeKV()
	accepted := seedManagedCardNode(t, kv, agentHex(1))
	left := sampleCard(1, 4, func(c *LinkCard) { c.Endpoint = "https://10.0.0.4:8443" })
	right := sampleCard(1, 4, func(c *LinkCard) { c.Endpoint = "https://10.0.0.5:8443" })

	type result struct {
		decision Decision
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, card := range []LinkCard{left, right} {
		card := card
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, decision, _, err := NewStore(kv).ApplyLocalCard(card, card.AgentPub, accepted)
			results <- result{decision: decision, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	applied, equivocated := 0, 0
	for result := range results {
		switch {
		case result.err == nil && result.decision == DecisionApplied:
			applied++
		case result.err == nil && result.decision == DecisionRejectedEquivocation:
			equivocated++
		case errors.Is(result.err, ErrCardStateConflict):
			// A compare-and-set loser may retry and observe the equivocation;
			// either way it must not create a second accepted floor.
		default:
			t.Fatalf("unexpected concurrent result: decision=%q err=%v", result.decision, result.err)
		}
	}
	if applied != 1 {
		t.Fatalf("applied=%d, want exactly one winning card", applied)
	}
	current, ok, err := NewStore(kv).LoadCard(left.OrgID, left.AgentPub)
	if err != nil || !ok || (current.Digest != left.Digest() && current.Digest != right.Digest()) {
		t.Fatalf("concurrent floor is torn: card=%+v ok=%v err=%v", current, ok, err)
	}
	if equivocated > 1 {
		t.Fatalf("equivocation notices=%d, want at most one", equivocated)
	}
}
