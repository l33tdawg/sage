package orgpolicy

import (
	"encoding/hex"
	"testing"
)

// fakeKV is an in-memory KV that records every atomic batch, so a test can
// assert the "single durable write" property directly (how many transactions,
// which keys) without badger. It mirrors the store contract: absent key → nil.
type fakeKV struct {
	m      map[string][]byte
	writes [][]KVWrite // one entry per SetStatesAtomic call
}

func newFakeKV() *fakeKV { return &fakeKV{m: map[string][]byte{}} }

func (f *fakeKV) GetState(key string) ([]byte, error) {
	v, ok := f.m[key]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (f *fakeKV) SetStatesAtomic(w []KVWrite) error {
	batch := make([]KVWrite, len(w))
	copy(batch, w)
	f.writes = append(f.writes, batch)
	for _, e := range w {
		cp := make([]byte, len(e.Value))
		copy(cp, e.Value)
		f.m[e.Key] = cp
	}
	return nil
}

func (f *fakeKV) batchKeys(i int) map[string]bool {
	out := map[string]bool{}
	for _, e := range f.writes[i] {
		out[e.Key] = true
	}
	return out
}

func sampleOutcome(t *testing.T, p *Policy, enrollee string) EnrollmentOutcome {
	t.Helper()
	return EnrollmentOutcome{
		TokenID:       "tok-123",
		EnrolleePub:   enrollee,
		OrgID:         p.OrgID,
		BoundIdentity: "owner-1/node-west-7/role-member",
		FirstPolicy:   p,
		Accepted:      stateFrom(p),
		ConsumedAt:    1700,
	}
}

func TestCommitFirstEnrollmentIsOneAtomicWrite(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	key := fixedKey(t, 1)
	p := signed(t, key, nil)
	enrollee := hex.EncodeToString(pub(fixedKey(t, 9)))
	out := sampleOutcome(t, p, enrollee)

	if err := st.CommitFirstEnrollment(out); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// RFC #443 point 2: consume + accepted in ONE transaction, no two-phase window.
	if len(kv.writes) != 1 {
		t.Fatalf("expected exactly 1 atomic write, got %d", len(kv.writes))
	}
	keys := kv.batchKeys(0)
	if !keys[enrollKey(out.TokenID)] || !keys[acceptedKey(out.OrgID)] {
		t.Fatalf("single batch missing enroll+accepted keys: %v", keys)
	}
	if len(keys) != 2 {
		t.Fatalf("batch wrote unexpected extra keys: %v", keys)
	}
}

func TestEnrollmentReplayIsIdempotent(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	p := signed(t, fixedKey(t, 1), nil)
	enrollee := hex.EncodeToString(pub(fixedKey(t, 9)))
	out := sampleOutcome(t, p, enrollee)
	if err := st.CommitFirstEnrollment(out); err != nil {
		t.Fatal(err)
	}

	got, ok, err := st.LoadEnrollment(out.TokenID)
	if err != nil || !ok {
		t.Fatalf("load enrollment: ok=%v err=%v", ok, err)
	}
	// The replayable result is recoverable: same enrollee, same first policy digest,
	// same bound identity — a lost response never strands the node.
	if got.EnrolleePub != enrollee {
		t.Fatalf("enrollee mismatch: %q != %q", got.EnrolleePub, enrollee)
	}
	if got.FirstPolicy == nil || got.FirstPolicy.Digest() != p.Digest() {
		t.Fatal("stored first policy is not the delivered policy")
	}
	if got.BoundIdentity != out.BoundIdentity {
		t.Fatalf("bound identity mismatch: %q", got.BoundIdentity)
	}
	// The accepted high-water mark was set by the same write.
	acc, err := st.LoadAccepted(p.OrgID)
	if err != nil || acc.Revision != p.Revision || acc.Digest != p.Digest() {
		t.Fatalf("accepted not established by enrollment: %+v err=%v", acc, err)
	}
}

func TestCommitFirstEnrollmentRejectsInconsistent(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	p := signed(t, fixedKey(t, 1), nil)
	enrollee := hex.EncodeToString(pub(fixedKey(t, 9)))

	// nil first policy
	bad := sampleOutcome(t, p, enrollee)
	bad.FirstPolicy = nil
	if err := st.CommitFirstEnrollment(bad); err == nil {
		t.Fatal("accepted outcome with no first policy")
	}
	// accepted state disagrees with first policy
	bad2 := sampleOutcome(t, p, enrollee)
	bad2.Accepted.Revision = p.Revision + 1
	if err := st.CommitFirstEnrollment(bad2); err == nil {
		t.Fatal("accepted mismatched high-water mark")
	}
	// org mismatch
	bad3 := sampleOutcome(t, p, enrollee)
	bad3.OrgID = "org-other"
	if err := st.CommitFirstEnrollment(bad3); err == nil {
		t.Fatal("accepted cross-org outcome")
	}
	if len(kv.writes) != 0 {
		t.Fatalf("a rejected commit still wrote state: %d batches", len(kv.writes))
	}
}

func TestApplyPolicyAppliedPersists(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	key := fixedKey(t, 1)
	p := signed(t, key, nil) // rev 3
	next, d, notice, err := st.ApplyPolicy(p, pub(key))
	if err != nil || d != DecisionApplied || notice != nil {
		t.Fatalf("first apply: d=%q notice=%v err=%v", d, notice, err)
	}
	if next.Revision != p.Revision {
		t.Fatalf("applied state wrong: %+v", next)
	}
	if len(kv.writes) != 1 {
		t.Fatalf("applied should persist exactly one write, got %d", len(kv.writes))
	}
	// A strictly newer revision is applied and persisted.
	p4 := signed(t, key, func(p *Policy) { p.Revision = 4 })
	if _, d, _, err := st.ApplyPolicy(p4, pub(key)); err != nil || d != DecisionApplied {
		t.Fatalf("newer apply: d=%q err=%v", d, err)
	}
	acc, _ := st.LoadAccepted(p.OrgID)
	if acc.Revision != 4 {
		t.Fatalf("persisted revision not advanced: %d", acc.Revision)
	}
}

func TestApplyPolicyNoOpAndRollbackDoNotWrite(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	key := fixedKey(t, 1)
	p := signed(t, key, func(p *Policy) { p.Revision = 5 })
	if _, d, _, err := st.ApplyPolicy(p, pub(key)); err != nil || d != DecisionApplied {
		t.Fatalf("seed apply: d=%q err=%v", d, err)
	}
	writesAfterSeed := len(kv.writes)

	// Same digest re-fetch: no-op, no write (must not extend authority).
	if _, d, _, err := st.ApplyPolicy(p, pub(key)); err != nil || d != DecisionNoOpSameDigest {
		t.Fatalf("noop: d=%q err=%v", d, err)
	}
	// Stale revision: rollback, no write.
	stale := signed(t, key, func(p *Policy) { p.Revision = 4 })
	if _, d, _, err := st.ApplyPolicy(stale, pub(key)); err != nil || d != DecisionRejectedRollback {
		t.Fatalf("rollback: d=%q err=%v", d, err)
	}
	if len(kv.writes) != writesAfterSeed {
		t.Fatalf("no-op/rollback wrote state: %d extra batches", len(kv.writes)-writesAfterSeed)
	}
}

func TestApplyPolicyEquivocationPersistsNoticeNotAccepted(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	key := fixedKey(t, 1)
	p0 := signed(t, key, nil) // rev 3, content A
	if _, d, _, err := st.ApplyPolicy(p0, pub(key)); err != nil || d != DecisionApplied {
		t.Fatalf("seed: d=%q err=%v", d, err)
	}
	accBefore, _ := st.LoadAccepted(p0.OrgID)

	// Same revision, different content → equivocation.
	p1 := signed(t, key, func(p *Policy) { p.Payload.ChecksVersion = "sage-lasting/2+supported/1" })
	next, d, notice, err := st.ApplyPolicy(p1, pub(key))
	if err != nil || d != DecisionRejectedEquivocation {
		t.Fatalf("equivocation: d=%q err=%v", d, err)
	}
	if notice == nil || notice.AcceptedDigest != accBefore.Digest || notice.ConflictDigest != p1.Digest() {
		t.Fatalf("notice wrong: %+v", notice)
	}
	// Accepted state is UNCHANGED by equivocation.
	if next != accBefore {
		t.Fatal("equivocation changed the accepted state")
	}
	accAfter, _ := st.LoadAccepted(p0.OrgID)
	if accAfter != accBefore {
		t.Fatal("equivocation overwrote persisted accepted state")
	}
	// The notice is DURABLE for the operator surface.
	stored, ok, err := st.LoadEquivocation(p0.OrgID)
	if err != nil || !ok || stored.ConflictDigest != p1.Digest() {
		t.Fatalf("equivocation notice not persisted: ok=%v stored=%+v err=%v", ok, stored, err)
	}
}

func TestAcceptedChainSurvivesRestart(t *testing.T) {
	kv := newFakeKV()
	key := fixedKey(t, 1)
	p := signed(t, key, func(p *Policy) { p.Revision = 7 })
	if _, d, _, err := NewStore(kv).ApplyPolicy(p, pub(key)); err != nil || d != DecisionApplied {
		t.Fatalf("seed: d=%q err=%v", d, err)
	}
	// A fresh Store over the same durable KV = a node restart. Rollback must
	// still be rejected because the accepted high-water mark was persisted.
	restarted := NewStore(kv)
	stale := signed(t, key, func(p *Policy) { p.Revision = 6 })
	if _, d, _, err := restarted.ApplyPolicy(stale, pub(key)); err != nil || d != DecisionRejectedRollback {
		t.Fatalf("after restart rollback not rejected: d=%q err=%v", d, err)
	}
}
