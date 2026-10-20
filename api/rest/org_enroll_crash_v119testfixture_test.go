//go:build v119testfixture

package rest

// Crash-durability matrix for org fleet enrollment (RFC #443 point 2).
//
// A worker subprocess redeems an enrollment token through the real handler and
// is SIGKILLed while blocked at each SetStatesAtomic boundary, so no Go defer or
// Badger Close can mask a torn write. The parent then reopens the store and
// proves the enrollment is either entirely absent (crash before commit) or
// entirely present and consistent (crash after commit) — never a consume marker
// without the accepted state or vice versa — and that a retry of the same
// request converges: a fresh commit in the first case, a verbatim replay in the
// second, and a refusal for any other identity in both.
//
// The state lives in the node's SQLite store (WAL, synchronous=FULL), whose
// SetOrgPolicyStatesAtomic fires the same four hook stages; with
// synchronous=FULL the commit is fsynced, so after_atomic_commit and after_sync
// coincide. Scope: this proves process-crash atomicity at every write boundary;
// power-loss durability of a committed write rests on SQLite's FULL sync.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	enrollCrashWorkerEnv = "SAGE_ORG_ENROLL_CRASH_WORKER"
	enrollCrashStoreEnv  = "SAGE_ORG_ENROLL_CRASH_STORE"
	enrollCrashMarkerEnv = "SAGE_ORG_ENROLL_CRASH_MARKER"
	enrollCrashStageEnv  = "SAGE_ORG_ENROLL_CRASH_STAGE"

	enrollCrashOrgID   = "org-crash"
	enrollCrashTokenID = "tok-crash"
	enrollCrashToken   = "crash-bearer-token"
)

// enrollCrashFixture derives the same keys, token and signed first policy in
// the worker and the parent (ed25519 signing is deterministic).
type enrollCrashFixture struct {
	nodeHex  string
	otherHex string
	tokens   *stubEnrollTokenStore
	body     []byte
}

func newEnrollCrashFixture(t *testing.T) enrollCrashFixture {
	t.Helper()
	key := func(b byte) ed25519.PrivateKey {
		return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{b}, ed25519.SeedSize))
	}
	hexOf := func(k ed25519.PrivateKey) string { return auth.PublicKeyToAgentID(k.Public().(ed25519.PublicKey)) }
	orgKey, node, other := key(0x01), key(0x02), key(0x03)

	tokens := newStubEnrollTokenStore()
	tokens.seed(enrollCrashToken, &store.OrgEnrollmentToken{
		ID: enrollCrashTokenID, OrgID: enrollCrashOrgID, OwnerID: "owner", NodeLabel: "edge-1",
		Role: "member", OrgSignerPub: hexOf(orgKey), KeyEpoch: "e1",
	})
	policy := signedOrgPolicy(t, orgKey, enrollCrashOrgID, "e1", 1)
	return enrollCrashFixture{
		nodeHex:  hexOf(node),
		otherHex: hexOf(other),
		tokens:   tokens,
		body:     enrollBody(t, enrollCrashToken, policy, nil),
	}
}

func (f enrollCrashFixture) enroll(sq *store.SQLiteStore, callerHex string) *httptest.ResponseRecorder {
	s := &Server{orgPolicyState: sq, orgEnrollTokens: f.tokens, logger: zerolog.Nop()}
	rr := httptest.NewRecorder()
	enrollRouterAs(s, callerHex).ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/v1/org/"+enrollCrashOrgID+"/enroll", bytes.NewReader(f.body)))
	return rr
}

// TestOrgEnrollCrashWorker is launched by the matrix below. Its hook fsyncs a
// readiness marker at the requested stage and then blocks until SIGKILL.
func TestOrgEnrollCrashWorker(t *testing.T) {
	if os.Getenv(enrollCrashWorkerEnv) != "1" {
		t.Skip("subprocess helper")
	}
	path := os.Getenv(enrollCrashStoreEnv)
	marker := os.Getenv(enrollCrashMarkerEnv)
	target := store.AtomicStateWriteStage(os.Getenv(enrollCrashStageEnv))
	if path == "" || marker == "" || target == "" {
		t.Fatal("enroll crash worker environment is incomplete")
	}

	sq, err := store.NewSQLiteStore(context.Background(), path)
	require.NoError(t, err)
	restore := store.SetAtomicStateWriteFaultHookForTest(func(stage store.AtomicStateWriteStage) error {
		if stage != target {
			return nil
		}
		file, openErr := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if openErr != nil {
			return fmt.Errorf("create crash marker: %w", openErr)
		}
		if _, writeErr := file.WriteString(string(stage)); writeErr != nil {
			_ = file.Close()
			return fmt.Errorf("write crash marker: %w", writeErr)
		}
		if syncErr := file.Sync(); syncErr != nil {
			_ = file.Close()
			return fmt.Errorf("sync crash marker: %w", syncErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("close crash marker: %w", closeErr)
		}
		select {}
	})
	defer restore()

	f := newEnrollCrashFixture(t)
	rr := f.enroll(sq, f.nodeHex)
	t.Fatalf("requested crash stage %q did not fire (HTTP %d: %s)", target, rr.Code, rr.Body.String())
}

func TestOrgEnrollCrashBoundariesReopenAllOrNothingAndRetryConverges(t *testing.T) {
	tests := []struct {
		stage     store.AtomicStateWriteStage
		committed bool
	}{
		{stage: store.AtomicStateWriteBeforeTransaction},
		{stage: store.AtomicStateWriteBeforeCommit},
		{stage: store.AtomicStateWriteAfterCommit, committed: true},
		{stage: store.AtomicStateWriteAfterSync, committed: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.stage), func(t *testing.T) {
			root := t.TempDir()
			dbPath := filepath.Join(root, "org-policy.db")
			marker := filepath.Join(root, "crash-ready")

			cmd := exec.Command(os.Args[0], "-test.run=^TestOrgEnrollCrashWorker$", "-test.v")
			cmd.Env = append(os.Environ(),
				enrollCrashWorkerEnv+"=1",
				enrollCrashStoreEnv+"="+dbPath,
				enrollCrashMarkerEnv+"="+marker,
				enrollCrashStageEnv+"="+string(tt.stage),
			)
			var childOutput bytes.Buffer
			cmd.Stdout = &childOutput
			cmd.Stderr = &childOutput
			require.NoError(t, cmd.Start())

			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, statErr := os.Stat(marker); statErr == nil {
					break
				} else if !os.IsNotExist(statErr) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("inspect crash marker: %v\n%s", statErr, childOutput.String())
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("worker did not reach %s\n%s", tt.stage, childOutput.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			require.NoError(t, cmd.Process.Kill())
			require.Error(t, cmd.Wait(), "SIGKILLed worker must not exit successfully")

			reopened, err := store.NewSQLiteStore(context.Background(), dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			ps := orgpolicy.NewStore(orgKV{ctx: context.Background(), st: reopened})
			fx := newEnrollCrashFixture(t)

			// All-or-nothing: the consume marker and the accepted high-water mark
			// are present together or absent together.
			outcome, consumed, err := ps.LoadEnrollment(enrollCrashTokenID)
			require.NoError(t, err)
			accepted, err := ps.LoadAccepted(enrollCrashOrgID)
			require.NoError(t, err)
			require.Equal(t, consumed, accepted.Revision != 0, "consume marker and accepted state must never be torn")
			require.Equal(t, tt.committed, consumed, "crash at %s", tt.stage)
			if consumed {
				assert.Equal(t, fx.nodeHex, outcome.EnrolleePub)
				assert.Equal(t, accepted, outcome.Accepted, "accepted state must match the stored outcome")
				assert.Equal(t, uint64(1), accepted.Revision)
			}

			// Retry by the same node converges: replay if the crash landed after
			// commit, otherwise a fresh single-write commit.
			retry := fx.enroll(reopened, fx.nodeHex)
			var resp OrgEnrollResponse
			require.NoError(t, json.Unmarshal(retry.Body.Bytes(), &resp), retry.Body.String())
			if tt.committed {
				require.Equal(t, http.StatusOK, retry.Code, retry.Body.String())
				assert.True(t, resp.Replayed)
				assert.Equal(t, outcome.BoundIdentity, resp.BoundIdentity)
				assert.Equal(t, outcome.Accepted, resp.Accepted)
			} else {
				require.Equal(t, http.StatusCreated, retry.Code, retry.Body.String())
				assert.False(t, resp.Replayed)
			}
			after, ok, err := ps.LoadEnrollment(enrollCrashTokenID)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, fx.nodeHex, after.EnrolleePub)

			// Either way the token is now spent: another identity is refused.
			other := fx.enroll(reopened, fx.otherHex)
			assert.Equal(t, http.StatusConflict, other.Code, other.Body.String())
		})
	}
}
