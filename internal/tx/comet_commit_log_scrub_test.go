package tx

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// The CheckTx and TxResult logs that BroadcastCometCommit and BroadcastCometSync
// hand back are remote-derived text: a compromised CometBFT endpoint, or a
// reverse proxy in front of an honest one, chooses every byte of them. They are
// scrubbed at the call site so a hostile node cannot echo the signed transaction
// it was just handed back into operator-visible output.
//
// Until these tests existed the scrub on those three fields was pinned only from
// the web package. Deleting all three calls from comet_commit.go left the whole
// of ./internal/tx green, so the package that owns the scrub could not detect
// losing it. Everything below drives the real broadcasters against an httptest
// CometBFT and asserts on what the caller actually receives.

// submittedTxHexFromRequest recovers the hex of the transaction THIS request
// actually carried, read back off the wire in whichever shape
// cometBroadcastRequest chose for it.
//
// The fixtures embed that recovered value rather than a hand-written constant.
// A log quoting some other string is not scrubbable by the exact-match pass at
// all, so asserting it came back clean would prove nothing -- which is the exact
// mistake that has been made on this code before.
func submittedTxHexFromRequest(t *testing.T, r *http.Request) string {
	t.Helper()
	if r.Method == http.MethodGet {
		raw := r.URL.Query().Get("tx")
		if !strings.HasPrefix(raw, "0x") {
			t.Errorf("GET broadcast carried tx=%q, want an 0x-prefixed hex transaction", raw)
			return ""
		}
		return raw[len("0x"):]
	}
	var envelope struct {
		Params struct {
			Tx string `json:"tx"`
		} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		t.Errorf("decode JSON-RPC broadcast body: %v", err)
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(envelope.Params.Tx)
	if err != nil {
		t.Errorf("decode base64 transaction from JSON-RPC body: %v", err)
		return ""
	}
	return hex.EncodeToString(raw)
}

// leakedLogFor wraps the submitted transaction hex in prose, BARE: no tx=0x
// parameter around it, and short enough to stay under the long-run and
// chunked-hex budgets.
//
// That shape is deliberate. It is invisible to every scrub pass except the
// exact match, and the exact match is the only one that needs the encoded bytes
// handed to it. So a fixture in this shape fails not just when the scrub call is
// deleted, but also when it survives as a call that passes nil where the encoded
// transaction belongs. requireExactMatchOnlyWitness re-proves that isolation at
// runtime instead of trusting the arithmetic in this comment.
func leakedLogFor(txHex string) string {
	return "mempool rejected submission " + txHex + " upstream"
}

// scrubbedLogFor is leakedLogFor with the payload replaced, i.e. exactly what a
// correct call site must return. Asserting equality against this rather than
// merely searching for the marker is what stops a wrong-but-similar value --
// a truncated hex tail, or the marker appended beside a surviving payload --
// from passing.
func scrubbedLogFor() string {
	return "mempool rejected submission " + redactedTxMarker + " upstream"
}

// requireExactMatchOnlyWitness proves the fixture is a real witness before
// anything is asserted about the scrubbed output.
//
// Two ways these tests could go quietly vacuous, both closed here: a fixture
// that never carried the submitted transaction, and a fixture whose shape one of
// the heuristic passes redacts on its own -- the latter would keep the
// assertions passing even with the encoded bytes unbound at the call site.
func requireExactMatchOnlyWitness(t *testing.T, rawLog, txHex string) {
	t.Helper()
	if txHex == "" {
		t.Fatal("no transaction hex was recovered from the wire; the fixture cannot witness a leak")
	}
	if !strings.Contains(rawLog, txHex) {
		t.Fatalf("fixture log %q omits the submitted transaction hex %q, so it has nothing to scrub",
			rawLog, txHex)
	}
	if unbound := ScrubBroadcastText(rawLog, nil); !strings.Contains(unbound, txHex) {
		t.Fatalf("fixture log is redacted even with no encoded transaction bound (%q); it no longer "+
			"isolates the exact-match pass and would pass against a call site that scrubs with nil",
			unbound)
	}
}

// assertLogScrubbed states the contract on one surfaced log field.
func assertLogScrubbed(t *testing.T, where, got, txHex string) {
	t.Helper()
	if strings.Contains(got, txHex) {
		t.Fatalf("%s handed back the signed transaction verbatim: %q", where, got)
	}
	if strings.Contains(strings.ToUpper(got), strings.ToUpper(txHex)) {
		t.Fatalf("%s leaked the signed transaction in a different hex case: %q", where, got)
	}
	if !strings.Contains(got, redactedTxMarker) {
		t.Fatalf("%s dropped the redaction marker %q, so the sentence was lost rather than redacted: %q",
			where, redactedTxMarker, got)
	}
	if want := scrubbedLogFor(); got != want {
		t.Fatalf("%s = %q, want exactly %q", where, got, want)
	}
}

// TestBroadcastCometCommitScrubsRemoteSuppliedLogs pins the ScrubBroadcastText
// calls that build CometCommitResult.CheckTxLog and CometCommitResult.TxResultLog.
//
// All three verdict shapes are covered because all three return the result to the
// caller: a CheckTx refusal and a FinalizeBlock refusal both hand back a
// populated result with a nil error, and a committed transaction's log is
// exactly as remote-controlled as a failed one's.
func TestBroadcastCometCommitScrubsRemoteSuppliedLogs(t *testing.T) {
	encoded := []byte("internal-tx-commit-log-scrub-witness")
	txHex := hex.EncodeToString(encoded)
	bound := cometHashHexForTest(encoded)

	for _, tc := range []struct {
		name      string
		checkCode uint32
		txCode    uint32
		height    int
		// leaked names the field whose log carries the transaction hex.
		leakCheckTx  bool
		leakTxResult bool
	}{
		{name: "CheckTx refusal log", checkCode: 2, height: 0, leakCheckTx: true},
		{name: "FinalizeBlock refusal log", txCode: 5, height: 12, leakTxResult: true},
		{name: "committed success logs", height: 12, leakCheckTx: true, leakTxResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed atomic.Value
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wireHex := submittedTxHexFromRequest(t, r)
				observed.Store(wireHex)
				leak := leakedLogFor(wireHex)
				checkLog, txLog := "admitted", "applied"
				if tc.leakCheckTx {
					checkLog = leak
				}
				if tc.leakTxResult {
					txLog = leak
				}
				_, _ = fmt.Fprintf(w,
					`{"result":{"check_tx":{"code":%d,"log":%q},"tx_result":{"code":%d,"log":%q},"hash":%q,"height":"%d"}}`,
					tc.checkCode, checkLog, tc.txCode, txLog, bound, tc.height)
			}))
			defer rpc.Close()

			got, err := BroadcastCometCommit(context.Background(), rpc.URL, nil, encoded)
			if err != nil {
				t.Fatalf("BroadcastCometCommit: %v", err)
			}
			if got == nil {
				t.Fatal("BroadcastCometCommit returned no result")
			}

			wireHex, _ := observed.Load().(string)
			if wireHex != txHex {
				t.Fatalf("node observed transaction hex %q, want %q; the fixture did not carry the "+
					"bytes this submission actually sent", wireHex, txHex)
			}
			requireExactMatchOnlyWitness(t, leakedLogFor(wireHex), txHex)

			if tc.leakCheckTx {
				assertLogScrubbed(t, "CometCommitResult.CheckTxLog", got.CheckTxLog, txHex)
			}
			if tc.leakTxResult {
				assertLogScrubbed(t, "CometCommitResult.TxResultLog", got.TxResultLog, txHex)
			}
		})
	}
}

// TestBroadcastCometSyncScrubsRemoteSuppliedLog pins the ScrubBroadcastText call
// that builds CometSyncResult.CheckTxLog. Sync surfaces its log on both verdicts
// -- a refusal clears the fence and returns, an admission returns -- so both are
// covered.
func TestBroadcastCometSyncScrubsRemoteSuppliedLog(t *testing.T) {
	encoded := []byte("internal-tx-sync-log-scrub-witness")
	txHex := hex.EncodeToString(encoded)
	bound := cometHashHexForTest(encoded)

	for _, tc := range []struct {
		name string
		code uint32
	}{
		{name: "CheckTx refusal log", code: 2},
		{name: "admitted log", code: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed atomic.Value
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wireHex := submittedTxHexFromRequest(t, r)
				observed.Store(wireHex)
				_, _ = fmt.Fprintf(w, `{"result":{"code":%d,"hash":%q,"log":%q}}`,
					tc.code, bound, leakedLogFor(wireHex))
			}))
			defer rpc.Close()

			got, err := BroadcastCometSync(context.Background(), rpc.URL, nil, encoded)
			if err != nil {
				t.Fatalf("BroadcastCometSync: %v", err)
			}
			if got == nil {
				t.Fatal("BroadcastCometSync returned no result")
			}

			wireHex, _ := observed.Load().(string)
			if wireHex != txHex {
				t.Fatalf("node observed transaction hex %q, want %q; the fixture did not carry the "+
					"bytes this submission actually sent", wireHex, txHex)
			}
			requireExactMatchOnlyWitness(t, leakedLogFor(wireHex), txHex)

			assertLogScrubbed(t, "CometSyncResult.CheckTxLog", got.CheckTxLog, txHex)
		})
	}
}
