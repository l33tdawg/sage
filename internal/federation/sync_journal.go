package federation

// v11.8 group journal engine (build step 3, docs/v11.8-PLAN.md §5).
//
// A synchronization group is coordinated OFF-CONSENSUS by a partitioned,
// hash-chained, ed25519-signed audit JOURNAL (sync_group_log). This file is the
// journal PRIMITIVE: a deterministic canonical encoding, per-entry hash-chaining
// + signing, whole-sub-chain fold/verification with a per-entry author-key
// resolver, and a serialized append. It deliberately does NOT interpret entry
// semantics (apply a member_invite to the roster, compute the manifest over
// reconstructed state, exchange sub-chains with peers) — that is steps 4-5.
//
// PARTITIONING (docs §5.2, the metadata-isolation fix): the journal is split into
// a 'roster' sub-chain (replicated to all members) and independent per-domain
// sub-chains 'domain:<tag>' (replicated only to members sharing that domain), so
// a member never learns of a domain it does not share. Each sub-chain is an
// independent hash chain keyed (group_id, subchain, seq).
//
// SOURCE OF TRUTH: the sync_group_log rows. sync_group.roster_journal_head /
// roster_revision are a best-effort CACHE re-derivable by folding the log, so a
// crash between an append and the cache advance is harmless (no TOCTOU window
// that can corrupt state — the (group_id,subchain,seq) PK is the backstop and the
// fold is authoritative).

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	// journalDomainSep domain-separates journal signatures from every other
	// ed25519 signature in the system (federation requests, receipts, origin_sig).
	journalDomainSep = "sage-sync-journal-v1\x00"
	// RosterSubchain names the membership sub-chain (all members converge on it).
	RosterSubchain = "roster"
)

// DomainSubchain names the per-domain audit sub-chain for a shared domain. Only
// members whose group consent includes the domain ever receive it (docs §5.2).
func DomainSubchain(domainTag string) string { return "domain:" + domainTag }

// lpAppend appends a 4-byte big-endian length prefix followed by s — the
// injective framing shared by every canonical encoder here.
func lpAppend(b []byte, s string) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s))) // #nosec G115 -- bounded inputs
	b = append(b, n[:]...)
	return append(b, []byte(s)...)
}

// canonicalPayloadBytes deterministically encodes a journal entry's payload: a
// 4-byte key count, then each (key, value) pair in ascending key order, both
// length-prefixed. Order-independent and injective, so two implementations
// derive identical bytes for identical semantic content (docs §5.3).
func canonicalPayloadBytes(payload map[string]string) []byte {
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(len(keys))) // #nosec G115 -- bounded
	for _, k := range keys {
		b = lpAppend(b, k)
		b = lpAppend(b, payload[k])
	}
	return b
}

// entryCanonicalBytes builds the exact bytes hashed AND signed for one entry.
// prev_hash is included, so entry_hash chains to its predecessor; every field an
// author attests (group, subchain, seq, type, author identity, payload) is bound.
func entryCanonicalBytes(groupID, subchain string, seq int64, prevHash, entryType, authorChainID, authorPubHex string, payload map[string]string) []byte {
	b := make([]byte, 0, 128)
	b = append(b, []byte(journalDomainSep)...)
	b = lpAppend(b, groupID)
	b = lpAppend(b, subchain)
	var s8 [8]byte
	binary.BigEndian.PutUint64(s8[:], uint64(seq)) // #nosec G115 -- seq non-negative
	b = append(b, s8[:]...)
	b = lpAppend(b, prevHash)
	b = lpAppend(b, entryType)
	b = lpAppend(b, authorChainID)
	b = lpAppend(b, authorPubHex)
	return append(b, canonicalPayloadBytes(payload)...)
}

// buildJournalEntry constructs a hash-chained, signed entry. prevHash is the
// current sub-chain head ("" at genesis) and seq is head+1 (0 at genesis). The
// payload map is stored as canonical JSON (readable for audit/dashboard) but the
// hash+signature are over the length-prefixed form, so the digest is codec-robust
// and independent of JSON escaping.
func buildJournalEntry(groupID, subchain string, seq int64, prevHash, entryType, authorChainID string, authorPub ed25519.PublicKey, authorKey ed25519.PrivateKey, payload map[string]string) (store.SyncGroupLogEntry, error) {
	if len(authorPub) != ed25519.PublicKeySize {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invalid author pubkey")
	}
	if payload == nil {
		payload = map[string]string{}
	}
	pj, err := json.Marshal(payload)
	if err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("marshal payload: %w", err)
	}
	authorPubHex := hex.EncodeToString(authorPub)
	cb := entryCanonicalBytes(groupID, subchain, seq, prevHash, entryType, authorChainID, authorPubHex, payload)
	sum := sha256.Sum256(cb)
	return store.SyncGroupLogEntry{
		GroupID:           groupID,
		Subchain:          subchain,
		Seq:               seq,
		PrevHash:          prevHash,
		EntryHash:         hex.EncodeToString(sum[:]),
		EntryType:         entryType,
		PayloadJSON:       string(pj),
		AuthorChainID:     authorChainID,
		AuthorAgentPubkey: authorPubHex,
		AuthorSig:         hex.EncodeToString(ed25519.Sign(authorKey, cb)),
	}, nil
}

// verifyJournalEntry recomputes an entry's hash from its stored fields and
// verifies its signature against expectedAuthorPub. It rejects (fail-closed): an
// entry whose stored author key != expectedAuthorPub (a valid signature from the
// WRONG author is caught), an undecodable payload, a recomputed hash != stored
// EntryHash, or an invalid signature.
func verifyJournalEntry(e store.SyncGroupLogEntry, expectedAuthorPub ed25519.PublicKey) error {
	if len(expectedAuthorPub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid expected author pubkey")
	}
	if e.AuthorAgentPubkey != hex.EncodeToString(expectedAuthorPub) {
		return fmt.Errorf("entry %s/%d: author key does not match expected author", e.Subchain, e.Seq)
	}
	var payload map[string]string
	if e.PayloadJSON != "" {
		if err := json.Unmarshal([]byte(e.PayloadJSON), &payload); err != nil {
			return fmt.Errorf("entry %s/%d: undecodable payload: %w", e.Subchain, e.Seq, err)
		}
	}
	cb := entryCanonicalBytes(e.GroupID, e.Subchain, e.Seq, e.PrevHash, e.EntryType, e.AuthorChainID, e.AuthorAgentPubkey, payload)
	sum := sha256.Sum256(cb)
	if hex.EncodeToString(sum[:]) != e.EntryHash {
		return fmt.Errorf("entry %s/%d: hash mismatch", e.Subchain, e.Seq)
	}
	sig, err := hex.DecodeString(e.AuthorSig)
	if err != nil || !ed25519.Verify(expectedAuthorPub, cb, sig) {
		return fmt.Errorf("entry %s/%d: signature invalid", e.Subchain, e.Seq)
	}
	return nil
}

// AuthorKeyResolver returns the ed25519 public key an entry MUST be signed by —
// the controller for most roster entries, the leaving member for a self-signed
// member_leave, the domain owner for a domain sub-chain entry — or nil to reject
// the entry as having no legitimate author. The caller supplies the policy; the
// fold enforces it.
type AuthorKeyResolver func(e store.SyncGroupLogEntry) ed25519.PublicKey

// FoldResult is the verified tip of a sub-chain.
type FoldResult struct {
	HeadHash string
	HeadSeq  int64 // -1 for an empty sub-chain
	Count    int
}

// foldSubchain verifies an ordered sub-chain end to end: seq continuity from 0,
// prev_hash linkage (genesis prev_hash == ""), and each entry's signature against
// the key the resolver returns. It returns the verified head or an error at the
// first break. An empty chain is valid (genesis-empty). Anti-rollback (rejecting
// a head whose derived roster_revision < the stored floor) is applied by the
// caller at the exchange/apply layer, not here — this proves INTEGRITY.
func foldSubchain(entries []store.SyncGroupLogEntry, resolve AuthorKeyResolver) (FoldResult, error) {
	prev := ""
	for i, e := range entries {
		if e.Seq != int64(i) {
			return FoldResult{}, fmt.Errorf("seq gap at index %d: got seq %d", i, e.Seq)
		}
		if e.PrevHash != prev {
			return FoldResult{}, fmt.Errorf("chain break at seq %d: prev_hash does not link to predecessor", e.Seq)
		}
		key := resolve(e)
		if key == nil {
			return FoldResult{}, fmt.Errorf("entry %s/%d: no authorized author", e.Subchain, e.Seq)
		}
		if err := verifyJournalEntry(e, key); err != nil {
			return FoldResult{}, err
		}
		prev = e.EntryHash
	}
	if len(entries) == 0 {
		return FoldResult{HeadSeq: -1}, nil
	}
	return FoldResult{HeadHash: prev, HeadSeq: int64(len(entries) - 1), Count: len(entries)}, nil
}

// AppendGroupJournalEntry builds, signs, and appends the next entry to a group
// sub-chain under journalMu (so read-head -> build -> append is atomic against
// concurrent appenders), then best-effort advances the roster head cache. The
// (group_id,subchain,seq) PK is the correctness backstop; journalMu only avoids
// the retryable-loser churn of a naive check-then-act. Returns the appended entry.
func (m *Manager) AppendGroupJournalEntry(ctx context.Context, groupID, subchain, entryType, authorChainID string, authorPub ed25519.PublicKey, authorKey ed25519.PrivateKey, payload map[string]string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	m.journalMu.Lock()
	defer m.journalMu.Unlock()

	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
	if err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("read sub-chain head: %w", err)
	}
	seq := int64(0)
	prev := ""
	if head != nil {
		seq = head.Seq + 1
		prev = head.EntryHash
	}
	entry, err := buildJournalEntry(groupID, subchain, seq, prev, entryType, authorChainID, authorPub, authorKey, payload)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if err := ss.AppendSyncGroupLog(ctx, entry); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	// Best-effort head-cache advance for the roster sub-chain (a projection; the
	// log is authoritative, so a failure here is non-fatal — the fold re-derives).
	if subchain == RosterSubchain {
		if g, gErr := ss.GetSyncGroup(ctx, groupID); gErr == nil && g != nil {
			g.RosterJournalHead = entry.EntryHash
			_ = ss.UpsertSyncGroup(ctx, *g)
		}
	}
	return entry, nil
}

// LoadAndFoldSubchain reads a whole sub-chain from the store (paging) and folds
// it, returning the verified head. Used on rebuild/rejoin to re-establish the
// authoritative head from the log before any cache is trusted.
func (m *Manager) LoadAndFoldSubchain(ctx context.Context, groupID, subchain string, resolve AuthorKeyResolver) (FoldResult, error) {
	ss := m.syncStore()
	if ss == nil {
		return FoldResult{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	var all []store.SyncGroupLogEntry
	after := int64(-1)
	for {
		page, err := ss.ListSyncGroupLog(ctx, groupID, subchain, after, 2000)
		if err != nil {
			return FoldResult{}, err
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		after = page[len(page)-1].Seq
		if len(page) < 2000 {
			break
		}
	}
	return foldSubchain(all, resolve)
}
