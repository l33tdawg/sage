package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// scriptedDedupStore answers the tombstone lookup with a fixed verdict and
// records the exact arguments the co-commit guard passed to it.
type scriptedDedupStore struct {
	*mockMemoryStore
	tombstoned bool
	err        error
	hash       string
	excludeID  string
	calls      int
}

func (s *scriptedDedupStore) FindByContentHash(_ context.Context, contentHash, excludeMemoryID string) (bool, error) {
	s.calls++
	s.hash = contentHash
	s.excludeID = excludeMemoryID
	return s.tombstoned, s.err
}

// coCommitBody builds a fully-signed two-coauthor request body. The envelope
// reproduces the handler's own construction exactly, because each coauthor
// signs the canonical core the handler will rebuild from the request.
func coCommitBody(t *testing.T, content string) (body []byte, contentHash, sharedID string) {
	t.Helper()
	const (
		domain     = "test.tombstone"
		createdAt  = int64(1_700_000_000)
		confidence = 0.9
	)
	nonce := []byte("nonce-tombstone-guard")
	sum := sha256.Sum256([]byte(content))

	env := &tx.CoCommitSubmit{
		SchemaVersion:   1,
		ContentHash:     sum[:],
		MemoryType:      tx.MemoryTypeFact,
		Domain:          domain,
		Classification:  tx.ClearanceInternal,
		ConfidenceScore: confidence,
		CreatedAtUnix:   createdAt,
		AgreementNonce:  nonce,
	}
	privs := make([]ed25519.PrivateKey, 0, 2)
	for _, chainID := range []string{"sage-a", "sage-b"} {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		env.Coauthors = append(env.Coauthors, tx.CoCommitCoauthor{PubKey: pub, ChainID: chainID})
		privs = append(privs, priv)
	}
	core := tx.CanonicalCoreBytes(env)
	for i := range env.Coauthors {
		env.Coauthors[i].Sig = ed25519.Sign(privs[i], core)
	}
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, nonce)

	coauthors := make([]CoCommitCoauthorJSON, 0, len(env.Coauthors))
	for _, c := range env.Coauthors {
		coauthors = append(coauthors, CoCommitCoauthorJSON{
			PubKey:  hex.EncodeToString(c.PubKey),
			ChainID: c.ChainID,
			Sig:     hex.EncodeToString(c.Sig),
		})
	}
	encoded, err := json.Marshal(CoCommitSubmitRequest{
		SchemaVersion:   1,
		Content:         content,
		MemoryType:      "fact",
		DomainTag:       domain,
		Classification:  int(tx.ClearanceInternal),
		ConfidenceScore: confidence,
		CreatedAtUnix:   createdAt,
		AgreementNonce:  hex.EncodeToString(nonce),
		Coauthors:       coauthors,
	})
	require.NoError(t, err)
	return encoded, hex.EncodeToString(sum[:]), env.SharedID
}

// TestCoCommitSubmitTombstoneGuard covers the one tombstone question on the
// co-commit path. Block inclusion is decisive there and the voter never runs, so
// without this guard a jointly-signed envelope could re-commit bytes the quorum
// already deprecated under a fresh memory id.
func TestCoCommitSubmitTombstoneGuard(t *testing.T) {
	var cometHits int32
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cometHits, 1)
		writeCometCommitFixture(t, w, r, 0, "", 0, "co-commit submitted", 1)
	}))
	defer cometMock.Close()

	for _, tc := range []struct {
		name       string
		tombstoned bool
		lookupErr  error
	}{
		{name: "tombstoned content is refused", tombstoned: true},
		{name: "clean content is submitted", tombstoned: false},
		{name: "lookup error fails open", tombstoned: false, lookupErr: errors.New("projection unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atomic.StoreInt32(&cometHits, 0)
			srv, _, _ := newTestServer(t, cometMock.URL)
			dedup := &scriptedDedupStore{
				mockMemoryStore: newMockMemoryStore(),
				tombstoned:      tc.tombstoned,
				err:             tc.lookupErr,
			}
			srv.store = dedup

			body, contentHash, sharedID := coCommitBody(t, "shared tombstone content")
			rec := httptest.NewRecorder()
			srv.handleCoCommitSubmit(rec, httptest.NewRequest(http.MethodPost, "/v1/cocommit/submit", bytes.NewReader(body)))

			require.Equal(t, 1, dedup.calls, "the co-commit path must ask the tombstone question exactly once")
			require.Equal(t, contentHash, dedup.hash, "the guard must look up the content hash the envelope carries")
			require.Equal(t, sharedID, dedup.excludeID,
				"the envelope's own SharedID must be excluded so an idempotent re-send is not refused by the row it wrote")

			if tc.tombstoned {
				require.Equal(t, http.StatusConflict, rec.Code)
				require.Contains(t, rec.Body.String(), "Tombstoned content")
				require.Equal(t, int32(0), atomic.LoadInt32(&cometHits),
					"a tombstoned co-commit must be refused before it is broadcast")
				return
			}
			require.NotEqual(t, http.StatusConflict, rec.Code,
				"a clean or unverifiable lookup must never be reported as a tombstone")
			require.Equal(t, int32(1), atomic.LoadInt32(&cometHits),
				"the submit must proceed to the chain")
		})
	}
}
