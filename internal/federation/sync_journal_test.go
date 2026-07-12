package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

func mustEntry(t *testing.T, groupID, subchain string, seq int64, prev, etype, authorChain string, pub ed25519.PublicKey, key ed25519.PrivateKey, payload map[string]string) store.SyncGroupLogEntry {
	t.Helper()
	e, err := buildJournalEntry(groupID, subchain, seq, prev, etype, authorChain, pub, key, payload)
	if err != nil {
		t.Fatalf("buildJournalEntry: %v", err)
	}
	return e
}

func TestJournalEntryBuildVerify(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", pub, key,
		map[string]string{"controller": "chain-ctl", "epoch": "e1"})

	if err := verifyJournalEntry(e, pub); err != nil {
		t.Fatalf("valid entry did not verify: %v", err)
	}
	// A valid signature from the WRONG author is caught.
	other, _, _ := ed25519.GenerateKey(nil)
	if err := verifyJournalEntry(e, other); err == nil {
		t.Fatalf("wrong expected author must be rejected")
	}

	// Tamper each stored field -> verify fails.
	mut := func(name string, f func(*store.SyncGroupLogEntry)) {
		c := e
		f(&c)
		if err := verifyJournalEntry(c, pub); err == nil {
			t.Fatalf("tampered %s verified but must not", name)
		}
	}
	mut("payload", func(c *store.SyncGroupLogEntry) { c.PayloadJSON = `{"controller":"chain-evil","epoch":"e1"}` })
	mut("prev_hash", func(c *store.SyncGroupLogEntry) { c.PrevHash = "deadbeef" })
	mut("entry_type", func(c *store.SyncGroupLogEntry) { c.EntryType = "member_remove" })
	mut("author_chain", func(c *store.SyncGroupLogEntry) { c.AuthorChainID = "chain-evil" })
	mut("seq", func(c *store.SyncGroupLogEntry) { c.Seq = 5 })
	mut("entry_hash", func(c *store.SyncGroupLogEntry) { c.EntryHash = "00" })
	mut("sig", func(c *store.SyncGroupLogEntry) { c.AuthorSig = "00" })
}

func TestFoldSubchain(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	resolveCtl := func(store.SyncGroupLogEntry) ed25519.PublicKey { return pub }

	// A valid 3-entry chain.
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "c", pub, key, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "c", pub, key, map[string]string{"member": "chain-a"})
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_activate", "c", pub, key, map[string]string{"member": "chain-a"})
	chain := []store.SyncGroupLogEntry{e0, e1, e2}

	res, err := foldSubchain(chain, resolveCtl)
	if err != nil {
		t.Fatalf("valid chain fold: %v", err)
	}
	if res.HeadSeq != 2 || res.HeadHash != e2.EntryHash || res.Count != 3 {
		t.Fatalf("fold head wrong: %+v", res)
	}

	// Empty chain is valid.
	if r, err := foldSubchain(nil, resolveCtl); err != nil || r.HeadSeq != -1 {
		t.Fatalf("empty fold: %+v %v", r, err)
	}

	// A broken prev_hash link fails at that seq.
	broken := []store.SyncGroupLogEntry{e0, e1, e2}
	broken[2].PrevHash = e0.EntryHash // skips e1
	if _, err := foldSubchain(broken, resolveCtl); err == nil {
		t.Fatalf("chain break must fail")
	}

	// A seq gap fails.
	gap := []store.SyncGroupLogEntry{e0, e2}
	if _, err := foldSubchain(gap, resolveCtl); err == nil {
		t.Fatalf("seq gap must fail")
	}

	// A resolver that refuses an entry (nil key) rejects the chain.
	refuse := func(e store.SyncGroupLogEntry) ed25519.PublicKey {
		if e.Seq == 1 {
			return nil
		}
		return pub
	}
	if _, err := foldSubchain(chain, refuse); err == nil {
		t.Fatalf("unauthorized author must fail")
	}

	// Truncation to a valid prefix folds OK on its own — anti-rollback (rejecting
	// a shorter head than the stored floor) is the caller's job, verified here to
	// exist as a distinct concern (docs §5.5).
	if r, err := foldSubchain(chain[:2], resolveCtl); err != nil || r.HeadSeq != 1 {
		t.Fatalf("valid prefix should fold: %+v %v", r, err)
	}
}

func TestAppendGroupJournalEntry(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	pub, key, _ := ed25519.GenerateKey(nil)
	// The controller key must match what we author with (AppendGroupJournalEntry
	// now self-checks the entry against the group's own author resolver).
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "c", ControllerAgentPubkey: hex.EncodeToString(pub)}); err != nil {
		t.Fatalf("UpsertSyncGroup: %v", err)
	}

	e0, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "group_create", "c", pub, key, nil)
	if err != nil {
		t.Fatalf("append e0: %v", err)
	}
	e1, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "member_invite", "c", pub, key, map[string]string{"member": "chain-a"})
	if err != nil {
		t.Fatalf("append e1: %v", err)
	}
	if e0.Seq != 0 || e1.Seq != 1 || e1.PrevHash != e0.EntryHash {
		t.Fatalf("append seq/prev wrong: e0=%+v e1=%+v", e0, e1)
	}

	// The roster head cache advanced to the latest entry.
	g, _ := ms.GetSyncGroup(ctx, "g1")
	if g.RosterJournalHead != e1.EntryHash {
		t.Fatalf("head cache = %q, want %q", g.RosterJournalHead, e1.EntryHash)
	}

	// LoadAndFoldSubchain re-derives the authoritative head from the log.
	res, err := m.LoadAndFoldSubchain(ctx, "g1", RosterSubchain, func(store.SyncGroupLogEntry) ed25519.PublicKey { return pub })
	if err != nil {
		t.Fatalf("load+fold: %v", err)
	}
	if res.HeadHash != e1.EntryHash || res.HeadSeq != 1 {
		t.Fatalf("folded head wrong: %+v", res)
	}
}

func TestCanonicalPayloadOrderIndependent(t *testing.T) {
	a := canonicalPayloadBytes(map[string]string{"b": "2", "a": "1"})
	b := canonicalPayloadBytes(map[string]string{"a": "1", "b": "2"})
	if !bytes.Equal(a, b) {
		t.Fatalf("payload encoding must be order-independent")
	}
	// Boundary injectivity: {"a":"bc"} != {"ab":"c"} (length-prefixed).
	if bytes.Equal(canonicalPayloadBytes(map[string]string{"a": "bc"}), canonicalPayloadBytes(map[string]string{"ab": "c"})) {
		t.Fatalf("length-prefix boundary collision")
	}
}

// TestFoldRejectsForgedAuthor locks the AuthorKeyResolver security contract: the
// fold rejects an entry not signed by the resolver's AUTHORITATIVE key, and a
// naive self-trusting resolver would accept a forgery (documenting the footgun).
func TestFoldRejectsForgedAuthor(t *testing.T) {
	ctlPub, _, _ := ed25519.GenerateKey(nil)
	forgerPub, forgerKey, _ := ed25519.GenerateKey(nil)

	// A forger self-signs a member_remove — internally consistent under its OWN key.
	forged := mustEntry(t, "g1", RosterSubchain, 0, "", "member_remove", "chain-forger",
		forgerPub, forgerKey, map[string]string{"member": "chain-victim"})
	if err := verifyJournalEntry(forged, forgerPub); err != nil {
		t.Fatalf("self-consistent entry should verify against its own key: %v", err)
	}

	// The fold with a resolver returning the AUTHORITATIVE controller key rejects
	// it (author-key mismatch) — the contract that keeps forgeries out.
	rejectByController := func(store.SyncGroupLogEntry) ed25519.PublicKey { return ctlPub }
	if _, err := foldSubchain([]store.SyncGroupLogEntry{forged}, rejectByController); err == nil {
		t.Fatalf("fold must reject an entry not signed by the resolver's authoritative key")
	}

	// A NAIVE self-trusting resolver (return decodeHex(e.AuthorAgentPubkey)) WOULD
	// accept the forgery — the exact contract violation AuthorKeyResolver forbids.
	// Asserting it accepts here documents WHY a resolver must never trust the entry.
	naiveSelfTrust := func(e store.SyncGroupLogEntry) ed25519.PublicKey {
		b, _ := hex.DecodeString(e.AuthorAgentPubkey)
		return ed25519.PublicKey(b)
	}
	if _, err := foldSubchain([]store.SyncGroupLogEntry{forged}, naiveSelfTrust); err != nil {
		t.Fatalf("self-trusting resolver would accept the forgery (footgun demo): %v", err)
	}
}

// TestJournalPayloadCodecRobust verifies the signature binds the parsed MAP
// (§5.3), independent of JSON escaping: a non-canonical spelling that parses to
// the SAME map still verifies (so a conformant non-Go peer isn't false-rejected),
// while a spelling that CHANGES the map fails the hash. canonicalPayloadJSON
// normalizes any accepted spelling back to the canonical stored form.
func TestJournalPayloadCodecRobust(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "member_invite", "c", pub, key,
		map[string]string{"a": "1", "b": "2"})
	if err := verifyJournalEntry(e, pub); err != nil {
		t.Fatalf("canonical entry must verify: %v", err)
	}
	// Same map, different (RFC-equal) spelling -> ACCEPTED, and normalizes back.
	for _, spelling := range []string{`{"b":"2","a":"1"}`, `{"a":"1", "b":"2"}`, ` {"a":"1","b":"2"}`} {
		c := e
		c.PayloadJSON = spelling
		if err := verifyJournalEntry(c, pub); err != nil {
			t.Fatalf("same-map spelling %q must verify (sig binds the map): %v", spelling, err)
		}
		if got := canonicalPayloadJSON(spelling); got != e.PayloadJSON {
			t.Fatalf("canonicalPayloadJSON(%q)=%q, want %q", spelling, got, e.PayloadJSON)
		}
	}
	// A spelling that CHANGES the map (empty vs non-empty, dropped key) -> REJECTED.
	for _, spelling := range []string{"{}", "", `{"a":"1"}`} {
		c := e
		c.PayloadJSON = spelling
		if err := verifyJournalEntry(c, pub); err == nil {
			t.Fatalf("map-changing payload %q must be rejected", spelling)
		}
	}
}
