package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// This file covers web/'s two obligations to the signer fence: every producer
// that signs with a shared key must go THROUGH the lease, and a fenced request
// must be reported as "not sent yet" rather than as a consensus rejection.

// fenceResolvingRPC wraps a CometBFT stub whose answers are deliberately never
// definitive, and guarantees any fence such a test raises is RESOLVED — with
// proof, never deleted — before the test returns.
//
// WHY EVERY FENCE-RAISING TEST MUST GO THROUGH THIS. Fences live in a
// process-global map inside internal/tx, and the restart veto
// (handleRestart -> tx.RestartVetoReason) reads ALL of it. A test that leaves
// its fence behind therefore 503s every later restart test in the package
// ("SAGE is not safe to restart yet") — which is exactly how a full ./web run
// failed while each test passed in isolation — and it leaks a reconciliation
// goroutine re-broadcasting to a closed, OS-recyclable port for the rest of the
// run. The fence's own invariant is respected here: nothing reaches into the
// map. At teardown the stub stops stonewalling and starts answering the /tx
// probe with a committed result for the exact hash it is asked about, so
// reconciliation lifts the fence the same way it would on a live node whose
// transaction finally landed.
func fenceResolvingRPC(t *testing.T, garbled http.HandlerFunc) *httptest.Server {
	t.Helper()
	var resolveNow atomic.Bool
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if resolveNow.Load() {
			answerCommitted(w, r)
			return
		}
		garbled(w, r)
	}))
	// LIFO: the drain (registered second) runs before Close (registered first),
	// so the server is still answering while reconciliation collects its proof.
	t.Cleanup(rpc.Close)
	t.Cleanup(func() {
		resolveNow.Store(true)
		// Global emptiness, not just this test's key: every fence-raising test
		// drains through this helper, so anything left in the map at teardown is
		// a leak that will fail the restart tests far from its cause. The wait
		// rides the fence's own retry schedule (2s doubling to 60s), hence the
		// generous ceiling; the normal case is one or two retry intervals.
		assert.Eventuallyf(t, func() bool { return len(tx.FencedSigners()) == 0 },
			90*time.Second, 25*time.Millisecond,
			"the fence this test raised was never resolved; left behind it would 503 every later "+
				"restart test and leak a reconciliation goroutine: %+v", tx.FencedSigners())
	})
	return rpc
}

// answerCommitted answers a reconciliation probe with proof that the exact
// transaction it asks about landed: the /tx lookup echoes the requested hash as
// committed. Reconciliation checks /tx before re-submitting, so this is the
// first thing it sees after the flip.
func answerCommitted(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/tx" {
		hash := strings.TrimPrefix(r.URL.Query().Get("hash"), "0x")
		_, _ = fmt.Fprintf(w, `{"result":{"hash":%q,"height":"1","tx_result":{"code":0,"log":""}}}`, hash)
		return
	}
	// The re-submit leg, reached only if a probe raced the flip: report the
	// commit a real node would — with the REAL hash of the submitted bytes.
	// The resolver now refuses any verdict whose hash is not bound to the
	// exact bytes it re-submitted, so a placeholder here would stall the drain
	// for an extra retry interval instead of proving anything.
	txHex := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
	hash := ""
	if raw, err := hex.DecodeString(txHex); err == nil {
		sum := tx.CometTxHash(raw)
		hash = strings.ToUpper(hex.EncodeToString(sum[:]))
	}
	_, _ = fmt.Fprintf(w, `{"result":{"check_tx":{"code":0,"log":""},"tx_result":{"code":0,"log":""},"hash":%q,"height":"1"}}`, hash)
}

// fenceOneSigningKey drives a real indeterminate broadcast so the given key ends
// up genuinely fenced, and returns the RPC stub that keeps it that way for the
// duration of the test (teardown resolves it — see fenceResolvingRPC).
//
// The stub answers every path with a truncated body, so nothing it says is ever
// definitive: the handler's broadcast is indeterminate, and reconciliation's
// re-submissions stay unresolved for the duration of the test. That is the state
// an operator is actually in when a fence matters.
func fenceOneSigningKey(t *testing.T, key ed25519.PrivateKey) *httptest.Server {
	t.Helper()
	rpc := fenceResolvingRPC(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{`)
	})

	h := &DashboardHandler{CometBFTRPC: rpc.URL}
	_, _, _, err := h.signAndBroadcastCommitContext(
		context.Background(), accessGrantTx(agentIDForKey(key), "agent-fenced"), key)
	require.Error(t, err, "the truncated commit response must be reported as a failure")

	require.Eventually(t, func() bool {
		return fencedSignerPresent(key)
	}, 5*time.Second, 10*time.Millisecond, "the indeterminate broadcast did not fence its signing key")
	return rpc
}

func fencedSignerPresent(key ed25519.PrivateKey) bool {
	want := agentIDForKey(key)
	for _, held := range tx.FencedSigners() {
		if held.SignerPubKeyHex == want {
			return true
		}
	}
	return false
}

func TestBroadcastTxSyncUnprovenResponsesFenceExactSigner(t *testing.T) {
	for _, tc := range []struct {
		name string
		body func(*http.Request) string
	}{
		{"malformed JSON", func(*http.Request) string { return `{"result":` }},
		{"missing result", func(*http.Request) string { return `{}` }},
		{"null result", func(*http.Request) string { return `{"result":null}` }},
		{"missing code", func(r *http.Request) string {
			return `{"result":{"hash":"` + requestBoundHash(r) + `"}}`
		}},
		{"null code", func(r *http.Request) string {
			return `{"result":{"code":null,"hash":"` + requestBoundHash(r) + `"}}`
		}},
		{"missing hash", func(*http.Request) string {
			return `{"result":{"code":0}}`
		}},
		{"null hash", func(*http.Request) string {
			return `{"result":{"code":0,"hash":null}}`
		}},
		{"wrong hash", func(*http.Request) string {
			return `{"result":{"code":0,"hash":"AB12"}}`
		}},
		{"trailing JSON document", func(r *http.Request) string {
			valid := `{"result":{"code":0,"hash":"` + requestBoundHash(r) + `"}}`
			return valid + ` {}`
		}},
		{"invalid trailing data", func(r *http.Request) string {
			valid := `{"result":{"code":0,"hash":"` + requestBoundHash(r) + `"}}`
			return valid + ` garbage`
		}},
		{"oversized body", func(r *http.Request) string {
			valid := `{"result":{"code":0,"hash":"` + requestBoundHash(r) + `"}}`
			return valid + strings.Repeat(" ", int(tx.CometRPCMaxResponseBytes)+1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			rpc := fenceResolvingRPC(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, tc.body(r))
			})
			h := &DashboardHandler{CometBFTRPC: rpc.URL}
			err = h.signAndBroadcastSyncContext(
				context.Background(), accessGrantTx(agentIDForKey(key), "sync-unproven"), key)
			require.Error(t, err)
			require.Eventually(t, func() bool { return fencedSignerPresent(key) },
				5*time.Second, 10*time.Millisecond,
				"unproven sync response released the exact signer: %v", err)
		})
	}
}

func TestBroadcastTxSyncBoundCheckTxRefusalIsDefinitiveWithoutFence(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"result":{"code":4,"log":"nonce too low","hash":"`+requestBoundHash(r)+`"}}`)
	}))
	t.Cleanup(rpc.Close)

	h := &DashboardHandler{CometBFTRPC: rpc.URL}
	err = h.signAndBroadcastSyncContext(
		context.Background(), accessGrantTx(agentIDForKey(key), "sync-refused"), key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CheckTx (code 4)")
	assert.False(t, fencedSignerPresent(key), "hash-bound CheckTx refusal must retire registration")
}

func requestBoundHash(r *http.Request) string {
	raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
	if err != nil {
		return ""
	}
	sum := tx.CometTxHash(raw)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

// TestFencedRequestIsReportedAsNotSentNotAsARejection pins the HTTP contract.
//
// tx.ErrSignerFenced means NOTHING WAS SIGNED OR SENT. Reporting it as a
// rejection would tell an operator their change was refused when it was merely
// deferred — so they would go looking for a change to undo that does not exist,
// or redo the work by hand. 503 + Retry-After is the honest shape: ask again.
//
// The body must also not advise a restart. Restarting discards the fence without
// resolving anything, which is exactly how the transaction gets lost.
func TestFencedRequestIsReportedAsNotSentNotAsARejection(t *testing.T) {
	rec := httptest.NewRecorder()
	fenced := fmt.Errorf("nothing was signed or sent for this request: %w", tx.ErrSignerFenced)
	writeGovernanceBroadcastFailure(rec, "governance vote rejected: ", fenced)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"a fenced request must be 503 'not sent yet', never a rejection status")
	retryAfter, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	require.NoError(t, err, "a 503 that does not say when to retry is a dead end")
	require.Positive(t, retryAfter)

	body := strings.ToLower(rec.Body.String())
	assert.Contains(t, body, "not sent")
	assert.NotContains(t, body, "rejected",
		"a fenced request has no consensus verdict, so the word must not appear")
	assertNeverAdvisesARestart(t, body)
}

// assertNeverAdvisesARestart is the guard against the specific false claim this
// whole workstream exists because of.
//
// It matches ADVICE, not the word: telling an operator "SAGE is restarting" is a
// statement of fact and is fine, while telling them to restart is telling them to
// take the exact action that discards the fence and loses the transaction. The
// distinction is the point — a blanket ban on the word would just push the bad
// advice into a synonym.
func assertNeverAdvisesARestart(t *testing.T, lowered string) {
	t.Helper()
	for _, forbidden := range []string{
		"restart to", "restart sage to", "restart the node", "restarting will clear",
		"restart anyway", "try restarting", "quit and open", "quit sage",
	} {
		assert.NotContainsf(t, lowered, forbidden,
			"operator text advises %q; restarting discards the fence without resolving anything, "+
				"after which the abandoned transaction is lost as an untraceable Code 4", forbidden)
	}
}

// TestFencedRequestNeverFallsThroughToConsensusRejected is the regression for a
// gap that a handler-by-handler reading is guaranteed to miss.
//
// tx.ErrSignerFenced is deliberately NOT an "indeterminate commit": nothing is in
// flight, so there is nothing for a handler to reconcile against. That means it
// falls through every `if isIndeterminateCommitError(err) { … } else { … }` to
// the else branch — which in web/ is where handlers report
// `consensus_rejected`. So the exact error whose whole purpose is to be
// distinguishable from a rejection would be reported AS a rejection unless every
// such branch checks for it FIRST. writeSignerNotSentIfHeld is that check; this
// asserts it comes first.
func TestFencedRequestNeverFallsThroughToConsensusRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"fenced", fmt.Errorf("held pending confirmation: %w", tx.ErrSignerFenced)},
		{"quiesced", fmt.Errorf("await signing slot for this key: %w", tx.ErrSigningQuiesced)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, isIndeterminateCommitError(tc.err),
				"if this ever becomes indeterminate, handlers would go hunting for a change that was "+
					"never sent — the guard below is what makes that safe")

			rec := httptest.NewRecorder()
			require.True(t, writeSignerNotSentIfHeld(rec, tc.err),
				"the guard did not claim the error, so the handler would report a consensus rejection")
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.NotEmpty(t, rec.Header().Get("Retry-After"))
			body := strings.ToLower(rec.Body.String())
			assert.Contains(t, body, "not sent")
			assertNeverAdvisesARestart(t, body)
		})
	}

	// And it must NOT claim anything else, or a real rejection would be dressed
	// up as "try again later" and the operator would loop forever.
	rec := httptest.NewRecorder()
	assert.False(t, writeSignerNotSentIfHeld(rec, fmt.Errorf("tx rejected in CheckTx (code 4): nonce too low")))
	assert.Equal(t, http.StatusOK, rec.Code, "the guard wrote a response for an error it does not own")
}

// TestFencedAdminKeySurfacesPolicyEditAsNotSent drives one representative
// handler END TO END with a genuinely fenced signing key, because the
// unit-level guard tests above cannot catch the gap that actually shipped: the
// guard existing but a handler's error branch never calling it, so the fenced
// error fell through to that handler's "rejected" verdict. The agent policy
// edit (PATCH /network/agents/{id}) is the representative: its failure branch
// both rolls back the local projection and used to answer 502 "was rejected".
func TestFencedAdminKeySurfacesPolicyEditAsNotSent(t *testing.T) {
	// Shrink the commit budget so the fenced lease wait (2x this value) fails
	// fast instead of holding the test for the production 120s.
	t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", "50")

	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rpc := fenceOneSigningKey(t, adminKey)

	h, s := newTestHandler(t)
	h.CometBFTRPC = rpc.URL
	_, h.SigningKey, _ = ed25519.GenerateKey(nil)
	h.AdminSigningKey = adminKey
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-fenced-policy", Name: "Fenced", Role: "member",
		Clearance: 1, Status: "active", CreatedAt: time.Now().UTC(),
	}))

	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-fenced-policy",
		strings.NewReader(`{"clearance":3}`))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	rec := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.NotEmpty(t, rec.Header().Get("Retry-After"), "a 503 that does not say when to retry is a dead end")
	lowered := strings.ToLower(rec.Body.String())
	assert.Contains(t, lowered, "not sent")
	assert.NotContains(t, lowered, "rejected",
		"a fenced request has no consensus verdict, so the word must not appear")
	assertNeverAdvisesARestart(t, lowered)

	// The projection must mirror the chain, which never received the change.
	updated, err := s.GetAgent(context.Background(), "agent-fenced-policy")
	require.NoError(t, err)
	assert.Equal(t, 1, updated.Clearance, "a never-sent policy edit must not survive in the local projection")
}

// TestHandleRestartIsVetoedWhileASigningKeyIsFenced pins the manual-restart
// veto — handoff condition 2's guard at the dashboard surface.
//
// handleRestart's no-capability branch answers "fully quit SAGE and open it
// again": an instruction to do BY HAND the exact thing that discards the fence
// and loses the transaction. The fence check must therefore run before every
// other branch, and nothing downstream (drain preparation, the restart hooks)
// may be touched. Without this test, a refactor that reorders the early returns
// or consolidates the update-status guards would put the quit-and-reopen advice
// back on a fenced node with every other test staying green.
func TestHandleRestartIsVetoedWhileASigningKeyIsFenced(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fenceOneSigningKey(t, key)

	h, _ := newTestHandler(t)
	var restartMachineryTouched atomic.Bool
	h.RequestRestart = func() error { restartMachineryTouched.Store(true); return nil }
	h.RequestRestartPrepared = func(release, commit, abort func()) error {
		restartMachineryTouched.Store(true)
		return nil
	}
	h.PrepareRestartDrain = func(context.Context) (func(), func(), error) {
		restartMachineryTouched.Store(true)
		return func() {}, func() {}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/restart", nil)
	markLocalDashboardRequest(h, req)
	rec := httptest.NewRecorder()
	h.handleRestart(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.NotEmpty(t, rec.Header().Get("Retry-After"), "a 503 that does not say when to retry is a dead end")
	lowered := strings.ToLower(rec.Body.String())
	assert.Contains(t, lowered, "not safe to restart")
	assertNeverAdvisesARestart(t, lowered)
	assert.NotContains(t, lowered, "restart_required",
		"a fenced node must not answer with the manual quit-and-reopen instruction")
	assert.False(t, restartMachineryTouched.Load(),
		"a vetoed restart still reached the drain/restart hooks; the veto must run before any of them")
}

// TestLegacyImportLaneStopsTheBatchWhenTheSigningKeyIsHeld is the batch-break
// regression for the pre-app-v23 import lane.
//
// An earlier revision discarded signAndBroadcastSyncContext's error with `_ =`,
// so a fenced signing key made every record wait out the FULL background signing
// budget (120s in production) one after another — a 50-record import crawling
// for ~100 minutes — while each record was still counted "imported" locally
// with nothing on-chain and no error anywhere mentioning the fence. The batch
// must instead stop at the first held-key refusal, exactly like the app-v23
// lane directly above it, with an honest "not sent" that is not a rejection.
func TestLegacyImportLaneStopsTheBatchWhenTheSigningKeyIsHeld(t *testing.T) {
	// Shrink the commit budget so the fenced lease wait (2x this value) fails
	// fast instead of holding the test for the production 120s.
	t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", "50")

	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rpc := fenceOneSigningKey(t, key)

	h, s := newTestHandler(t)
	h.CometBFTRPC = rpc.URL
	h.SigningKey = key // legacy lane: appV23IsActive() is false on a bare test handler

	records := []*memory.MemoryRecord{
		{MemoryID: "legacy-import-1", Content: "first", DomainTag: "general", CreatedAt: time.Now().UTC()},
		{MemoryID: "legacy-import-2", Content: "second", DomainTag: "general", CreatedAt: time.Now().UTC()},
		{MemoryID: "legacy-import-3", Content: "third", DomainTag: "general", CreatedAt: time.Now().UTC()},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/import/upload", nil)
	rec := httptest.NewRecorder()
	h.processImportRecords(rec, req, records, "test", nil)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res importResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	assert.Zero(t, res.Imported,
		"records were counted imported while the signing key was held: nothing reached the chain")
	assert.Equal(t, len(records), res.Skipped, "the whole batch must stop at the first held-key refusal")
	require.NotEmpty(t, res.Errors, "a stopped import that reports no error is the silent loss this guard exists for")
	joined := strings.ToLower(strings.Join(res.Errors, " "))
	assert.Contains(t, joined, "not sent")
	assert.Contains(t, joined, "were not attempted")
	assert.NotContains(t, joined, "rejected",
		"a held key has no consensus verdict, so the word must not appear")
	assertNeverAdvisesARestart(t, joined)

	// The local projection must mirror the chain, which received nothing.
	//
	// Absence is asserted against the store's REAL contract: GetMemory reports a
	// missing row as an ERROR ("memory not found: %s", internal/store/sqlite.go),
	// not as (nil, nil). Requiring NoError here would demand the store behave in a
	// way it never has, so the test could only pass if the record HAD been
	// written — i.e. it would fail precisely when the guard under test works.
	stored, getErr := s.GetMemory(context.Background(), "legacy-import-1")
	require.Error(t, getErr, "a never-sent record must be absent, and absence is an error from this store")
	assert.Contains(t, getErr.Error(), "memory not found",
		"absence must be the reason; any other error means the projection failed for an unrelated cause")
	assert.Nil(t, stored, "a never-sent legacy import record must not survive in the local projection")
}

// TestDefinitiveRejectionStaysABadGateway is the other side of the same coin. A
// real consensus rejection must NOT be dressed up as "try again later": the
// change was refused, and telling the operator to retry it unchanged would just
// loop.
func TestDefinitiveRejectionStaysABadGateway(t *testing.T) {
	rec := httptest.NewRecorder()
	writeGovernanceBroadcastFailure(rec, "governance vote rejected: ",
		fmt.Errorf("tx rejected in CheckTx (code 19): revision conflict"))

	require.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "revision conflict")
}

// TestQuiescedRequestIsAlsoReportedAsNotSent covers the restart lane. Signing is
// stopped once a coordinated restart is committed, and that refusal carries the
// same meaning as a fence: nothing happened, ask again.
func TestQuiescedRequestIsAlsoReportedAsNotSent(t *testing.T) {
	rec := httptest.NewRecorder()
	writeGovernanceBroadcastFailure(rec, "governance proposal rejected: ",
		fmt.Errorf("await signing slot for this key: %w", tx.ErrSigningQuiesced))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotEmpty(t, rec.Header().Get("Retry-After"))
	assert.Contains(t, strings.ToLower(rec.Body.String()), "not sent")
}

// TestSignerFenceHealthHidesIdentifiersFromUnauthenticatedCallers pins the two
// tiers of the status surface.
//
// /v1/dashboard/health is PUBLIC — the CLI status command reads it without
// credentials. A held fence must still be visible there, because an invisible
// hold is a mystery outage and the whole case for holding rather than conceding
// rests on the failure being loud. But the per-agent detail (which signer, which
// transaction, which nonce) belongs behind the operator gate with the rest of the
// operator view.
func TestSignerFenceHealthHidesIdentifiersFromUnauthenticatedCallers(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fenceOneSigningKey(t, key)

	public := signerFenceHealth(false)
	require.GreaterOrEqual(t, public["active"].(int), 1, "a held fence is invisible on the public status surface")
	assert.NotContains(t, public, "signers", "the public surface must not enumerate fenced signing keys")

	encoded, err := json.Marshal(public)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), agentIDForKey(key),
		"the public surface leaked a signer identity")

	operator := signerFenceHealth(true)
	signers, ok := operator["signers"].([]map[string]any)
	require.True(t, ok, "the operator surface must enumerate fenced signing keys")
	require.NotEmpty(t, signers)

	var row map[string]any
	for _, candidate := range signers {
		if candidate["signer_agent_id"] == agentIDForKey(key) {
			row = candidate
			break
		}
	}
	require.NotNil(t, row, "the fenced key is missing from the operator surface")
	for _, field := range []string{"signer", "tx_hash", "held_seconds", "attempts", "cause"} {
		assert.Contains(t, row, field, "the operator surface omits %q, which triage needs", field)
	}
	// The nonce crosses the wire as a decimal STRING. It is a nanosecond
	// allocation, so it exceeds JavaScript's safe integer range and a JSON
	// number would reach the dashboard silently rounded — and the dashboard is
	// where it gets compared against the chain's committed nonce.
	nonce, hasNonce := row["nonce"]
	require.True(t, hasNonce, "a fence raised by a signed broadcast must report its nonce")
	nonceText, isString := nonce.(string)
	require.True(t, isString, "the nonce must be a string so the dashboard cannot round it, got %T", nonce)
	_, parseErr := strconv.ParseUint(nonceText, 10, 64)
	require.NoError(t, parseErr, "the nonce must be decimal: %q", nonceText)
	// The resolution class is what turns "a key is held" into an actionable
	// answer: this fence was raised by a live indeterminate broadcast, so the
	// node holds its bytes and reconciliation will keep re-submitting them. A
	// caller that cannot tell this from a restored fence ends up waiting for a
	// self-heal that was never going to come — the reported failure this field
	// exists to end.
	assert.Equal(t, "reconciling", row["resolution"],
		"a live fence must report that reconciliation can still settle it")
	assert.Contains(t, public["explanation"], "re-submitting",
		"the public explanation must name the route out of a live fence")

	// Everything on this surface has already been sanitized by internal/tx. The
	// assertion that matters is the one that would catch a regression there
	// leaking through: no encoded transaction, in any field.
	rendered, err := json.Marshal(operator)
	require.NoError(t, err)
	assert.NotContains(t, string(rendered), "broadcast_tx_commit?tx=0x",
		"the status surface carries a broadcast URL, which contains the signed transaction")

	// A fence that has ENDED stays visible to the operator. The held-fence block
	// is empty by then, so the last resolution is the only trace the dashboard
	// has — and lifting it must not hand the public tier the signer identity
	// through the new field.
	var fenced tx.FencedSigner
	for _, candidate := range tx.FencedSigners() {
		if candidate.SignerPubKeyHex == agentIDForKey(key) {
			fenced = candidate
			break
		}
	}
	require.NotEmpty(t, fenced.SignerPubKeyHex, "the fixture fence disappeared before it could be lifted")
	require.True(t, fenced.HasNonce, "a fence raised by a signed broadcast must carry a nonce")
	require.NoError(t, tx.LiftFenceWithProof(context.Background(), agentIDForKey(key), tx.FenceLiftProof{
		Kind:            "superseded",
		SignerPubKeyHex: agentIDForKey(key),
		Nonce:           fenced.Nonce,
		HasNonce:        true,
		CommittedNonce:  fenced.Nonce + 1,
		Detail:          "test proof",
	}))

	resolved := signerFenceHealth(true)
	assert.Equal(t, 0, resolved["active"], "the lift must clear the held-fence count")
	last, ok := resolved["last_resolution"].(map[string]any)
	require.True(t, ok, "a resolved fence must stay visible on the operator surface")
	assert.Equal(t, "superseded", last["mode"])
	assert.Equal(t, fenced.SignerPubKeyPrefix, last["signer"])
	assert.NotContains(t, signerFenceHealth(false), "last_resolution",
		"the public status surface must not learn which signer resolved")
}

// TestSignerFenceRoutesNamesTheRouteOutOfEachClass pins the sentence the status
// surface shows for a RESTORED fence — the one whose signed bytes did not
// survive the process. Reporting "reconciliation is re-submitting the identical
// bytes" there is a false promise, and it is precisely what agents reported
// back as a self-heal that never fires: the node was reading the chain for a
// proof, not re-submitting anything.
func TestSignerFenceRoutesNamesTheRouteOutOfEachClass(t *testing.T) {
	restored := signerFenceRoutes([]tx.FencedSigner{
		{Resolution: "proof_or_operator"},
	})
	assert.Contains(t, restored, "did not survive",
		"a restored fence must say its bytes are gone")
	assert.Contains(t, restored, "operator abandon",
		"a restored fence must name the operator route")
	assert.NotContains(t, restored, "re-submitting",
		"a restored fence must never be described as re-submitting anything")

	live := signerFenceRoutes([]tx.FencedSigner{{Resolution: "reconciling"}})
	assert.Contains(t, live, "re-submitting",
		"a live fence must say reconciliation is still pushing its bytes")

	mixed := signerFenceRoutes([]tx.FencedSigner{
		{Resolution: "reconciling"},
		{Resolution: "proof_or_operator"},
	})
	assert.Contains(t, mixed, "re-submitting")
	assert.Contains(t, mixed, "did not survive")

	// An unclassified cause must not be advertised as self-healing: the safe
	// reading of "we cannot say how this ends" is that it may need an operator.
	unknown := signerFenceRoutes([]tx.FencedSigner{{Resolution: "unknown"}})
	assert.Contains(t, unknown, "did not survive")
	assert.NotContains(t, unknown, "re-submitting")
}

// TestManualRestartAdviceRefusesToAdviseARestartWhileFenced pins the central
// helper every "fully quit SAGE and open it again" surface now routes through.
// The manual quit-and-reopen lane has no enforcement hook — the process cannot
// intercept Cmd-Q — so while a signing key is fenced, the ADVICE is the only
// control that lane has, and it must become the hold notice instead of the
// instruction.
func TestManualRestartAdviceRefusesToAdviseARestartWhileFenced(t *testing.T) {
	const instruction = "Fully quit SAGE and open it again to finish."

	// Nothing held: the instruction passes through verbatim, or ordinary
	// setting changes would nag operators about fences that do not exist.
	require.Empty(t, tx.FencedSigners(), "an earlier test leaked a fence into this one")
	assert.Equal(t, instruction, manualRestartAdvice(instruction))

	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fenceOneSigningKey(t, key)

	advice := manualRestartAdvice(instruction)
	assert.NotContains(t, advice, instruction,
		"a fenced signer's advice still tells the operator to quit and reopen — the manual bypass of the restart veto")
	lowered := strings.ToLower(advice)
	assert.Contains(t, lowered, "do not quit or restart",
		"the hold notice must say plainly that restarting is the wrong move")
	assert.Contains(t, lowered, "may still be in flight",
		"the hold notice must say WHY: the fence is protecting a transaction whose fate is unproven")
}

// TestEmbeddingsProviderSwitchDoesNotSwallowTheRestartVeto is the regression
// for the worst of the manual-restart advice sites: persistEmbeddingProvider
// called RequestRestart, the fence veto FIRED, and the handler swallowed the
// error and answered 200 telling the operator to quit and relaunch by hand —
// recommending the exact action the veto had just refused, at the exact moment
// it was known to be unsafe.
func TestEmbeddingsProviderSwitchDoesNotSwallowTheRestartVeto(t *testing.T) {
	if !restartInProcessSupported() {
		t.Skip("the RequestRestart error branch is unreachable where in-process restart is unsupported")
	}
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fenceOneSigningKey(t, key)

	vetoed := false
	h := &DashboardHandler{
		SetEmbeddingProvider: func(string) error { return nil },
		RequestRestart: func() error {
			// What cmd/sage-gui's wiring does with a fence held: refuse.
			vetoed = true
			return fmt.Errorf("restart refused: %s", tx.RestartVetoReason())
		},
	}
	rec := httptest.NewRecorder()
	h.persistEmbeddingProvider(rec, httptest.NewRequest(http.MethodPost, "/v1/dashboard/embeddings/provider", nil), "hash")

	require.True(t, vetoed, "test wiring: the restart request never ran")
	require.Equal(t, http.StatusOK, rec.Code,
		"the frontend discards non-200 guidance here; the honesty lives in the message")
	var body struct {
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	lowered := strings.ToLower(body.Message)
	assert.Contains(t, lowered, "do not quit or restart",
		"the veto fired and the operator must be told to hold, not to relaunch by hand")
	for _, forbidden := range []string{"fully quit", "relaunch", "reopen"} {
		assert.NotContains(t, lowered, forbidden,
			"the response advises the manual restart the veto just refused")
	}
}

// TestBroadcastRequestBuildFailureNeverQuotesTheSignedTransaction pins the last
// origin of the P0 signed-byte leak. http.NewRequestWithContext fails with a
// *url.Error whose message is `parse "<the whole URL>": …`, and a broadcast URL
// carries the entire signed transaction as its tx= parameter. A misconfigured
// CometBFTRPC (here: a control character) would put those bytes into every 502
// body and log line until the config was fixed — and because this failure is
// pre-send it is correctly NOT fenced, so the fence's scrubber never sees it.
// The error must therefore be built from the typed transport cause, never by
// wrapping the parse error.
func TestBroadcastRequestBuildFailureNeverQuotesTheSignedTransaction(t *testing.T) {
	const badRPC = "http://127.0.0.1:1\x7f" // control character: NewRequestWithContext refuses to parse it
	signed := []byte("signed-transaction-bytes")
	signedHex := hex.EncodeToString(signed)

	_, buildKey, keyErr := ed25519.GenerateKey(nil)
	require.NoError(t, keyErr)
	hash, height, txLog, err := broadcastTxCommitWebContext(context.Background(), badRPC, buildKey, signed)
	require.Error(t, err)
	assert.Empty(t, hash)
	assert.Zero(t, height)
	assert.Empty(t, txLog)
	assert.False(t, isIndeterminateCommitError(err),
		"the URL was never dialed, so nothing is in flight and this must not fence")
	assert.NotContains(t, err.Error(), signedHex,
		"the request-build error quotes the broadcast URL, which contains the whole signed transaction")
	assert.NotContains(t, err.Error(), "tx=0x", "the request-build error quotes the broadcast URL")

	syncErr := broadcastTxSyncContext(context.Background(), badRPC, buildKey, signed)
	require.Error(t, syncErr)
	assert.False(t, isIndeterminateCommitError(syncErr))
	assert.NotContains(t, syncErr.Error(), signedHex)
	assert.NotContains(t, syncErr.Error(), "tx=0x")
}

// TestBroadcastCommitOversizedBodyIsIndeterminate is the web half of the
// oversized-body bound. The commit path's decoder feeds the SAME retry engine
// as internal/tx's reconciliation (an indeterminate result fences, and the
// fence re-reads forever), so it takes the same cap — and the same rule: a
// body past the cap proves nothing, however perfect the envelope inside it,
// and must be indeterminate rather than a success or a definitive rejection.
func TestBroadcastCommitOversizedBodyIsIndeterminate(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signed := []byte("oversized-commit-answer")
	sum := tx.CometTxHash(signed)
	boundHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	pad := strings.Repeat("a", int(tx.CometRPCMaxResponseBytes)+(1<<20))

	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"pad":"`+pad+`","result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"`+
			boundHash+`","height":"7"}}`)
	}))
	t.Cleanup(rpc.Close)

	hash, height, txLog, err := broadcastTxCommitWebContext(context.Background(), rpc.URL, key, signed)
	require.Error(t, err)
	assert.Empty(t, hash)
	assert.Zero(t, height)
	assert.Empty(t, txLog)
	assert.True(t, isIndeterminateCommitError(err),
		"an oversized body is not proof of anything: it must fence, not fail definitively")
}

func TestBroadcastCommitTrailingDataIsIndeterminate(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signed := []byte("trailing-commit-answer")
	sum := tx.CometTxHash(signed)
	boundHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	proof := `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"` +
		boundHash + `","height":"7"}}`

	for _, tc := range []struct {
		name   string
		suffix string
	}{
		{name: "second JSON document", suffix: `{}`},
		{name: "oversized whitespace suffix", suffix: strings.Repeat(" ", int(tx.CometRPCMaxResponseBytes)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, proof+tc.suffix)
			}))
			t.Cleanup(rpc.Close)

			hash, height, txLog, err := broadcastTxCommitWebContext(context.Background(), rpc.URL, key, signed)
			require.Error(t, err)
			assert.Empty(t, hash)
			assert.Zero(t, height)
			assert.Empty(t, txLog)
			assert.True(t, isIndeterminateCommitError(err),
				"a non-single-document response is not proof and must hold the fence")
		})
	}
}

// TestBackgroundProducersAllocateNoncesUnderTheLease is condition 7 at the web
// layer: EVERY producer sharing a signing key must go through the lease. One
// that does not reopens the descending-arrival race for that key no matter how
// careful the others are — and the fire-and-forget producers are the worst place
// to leave it open, because nobody is waiting for the answer, so the loss is
// silent (an agent that simply never appears on-chain).
//
// broadcastAgentUpdate is the representative: it is the last web/ producer that
// used to call tx.MonotonicNonce directly and broadcast without serialization.
func TestBackgroundProducersAllocateNoncesUnderTheLease(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		nonces  []uint64
		signers = map[string]struct{}{}
	)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nonce, signer := nonceFromBroadcast(t, r)
		mu.Lock()
		nonces = append(nonces, nonce)
		signers[signer] = struct{}{}
		mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"result":{"code":0,"hash":"%s"}}`, requestBoundHash(r))
	}))
	t.Cleanup(rpc.Close)

	h := &DashboardHandler{CometBFTRPC: rpc.URL, SigningKey: key}

	const fanOut = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < fanOut; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			assert.NoError(t, h.broadcastAgentUpdate(fmt.Sprintf("agent-%d", i), "name", "bio"))
		}(i)
	}
	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, nonces, fanOut)
	require.Len(t, signers, 1, "the fan-out did not all sign with the same key, so it proves nothing")
	// ARRIVAL order, not allocation order: that is the property app-v9's
	// strictly-">" replay gate actually checks.
	for i := 1; i < len(nonces); i++ {
		require.Greaterf(t, nonces[i], nonces[i-1],
			"transaction %d arrived with nonce %d after %d — a descending arrival is rejected Code 4 "+
				"'nonce too low' and silently dropped on a fire-and-forget path",
			i, nonces[i], nonces[i-1])
	}
}
