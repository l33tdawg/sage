package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

type crossSetBarrierFederation struct {
	*fakeFederation
	pin          []byte
	commitCA     func() error
	stageEntered chan struct{}
	stageOnce    sync.Once
	agreementMu  sync.Mutex
}

func (f *crossSetBarrierFederation) StageRemoteCA(string, []byte) ([]byte, func() error, func(), error) {
	if f.stageEntered != nil {
		f.stageOnce.Do(func() { close(f.stageEntered) })
	}
	return append([]byte(nil), f.pin...), f.commitCA, func() {}, nil
}

func (f *crossSetBarrierFederation) LockAgreementMutation() func() {
	f.agreementMu.Lock()
	return f.agreementMu.Unlock
}

func TestHandleCrossFedSetUsesPolicyWriteBarrierThroughCACommit(t *testing.T) {
	cometEntered := make(chan struct{})
	var cometOnce sync.Once
	comet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cometOnce.Do(func() { close(cometEntered) })
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"check_tx":  map[string]any{"code": 0, "log": ""},
				"tx_result": map[string]any{"code": 0, "log": "agreement updated"},
				"hash":      "TX33NARROW",
				"height":    "7",
			},
		})
	}))
	defer comet.Close()

	ss, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "federation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	commitEntered := make(chan struct{})
	commitRelease := make(chan struct{})
	stageEntered := make(chan struct{})
	var commitOnce sync.Once
	srv, _, _ := newTestServer(t, comet.URL)
	srv.store = ss
	srv.SetNodeOperatorID("node-operator")
	srv.SetFederation(&crossSetBarrierFederation{
		fakeFederation: &fakeFederation{},
		pin:            make([]byte, 32),
		stageEntered:   stageEntered,
		commitCA: func() error {
			commitOnce.Do(func() { close(commitEntered) })
			<-commitRelease
			return nil
		},
	})

	body, err := json.Marshal(CrossFedSetRequest{
		RemoteChainID: "chain-peer",
		Endpoint:      "https://peer.example:8444",
		RemoteCAPEM:   "staged by test federation",
		MaxClearance:  4,
		AllowedDomains: []string{
			"narrowed",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/federation/cross", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	// Model an old-generation peer response already holding the read lease.
	readUnlock := ss.LockSyncPolicyRead()
	handlerDone := make(chan struct{})
	go func() {
		srv.handleCrossFedSet(rr, req.WithContext(middleware.WithAgentID(req.Context(), "node-operator")))
		close(handlerDone)
	}()
	select {
	case <-stageEntered:
	case <-time.After(5 * time.Second):
		readUnlock()
		close(commitRelease)
		t.Fatal("cross-fed set did not reach its policy barrier")
	}
	earlyBroadcast := false
	select {
	case <-cometEntered:
		earlyBroadcast = true
	case <-time.After(100 * time.Millisecond):
	}
	readUnlock()

	select {
	case <-commitEntered:
	case <-time.After(5 * time.Second):
		close(commitRelease)
		<-handlerDone
		t.Fatalf("tx-33 did not reach CA commit after the old reader released; status=%d body=%s", rr.Code, rr.Body.String())
	}

	// Promotion of the CA is part of the same generation change: a new peer
	// reader must not enter between consensus commit and the matching CA commit.
	readerEntered := make(chan struct{})
	go func() {
		unlock := ss.LockSyncPolicyRead()
		close(readerEntered)
		unlock()
	}()
	earlyReader := false
	select {
	case <-readerEntered:
		earlyReader = true
	case <-time.After(100 * time.Millisecond):
	}
	earlyReturn := false
	select {
	case <-handlerDone:
		earlyReturn = true
	case <-time.After(100 * time.Millisecond):
	}
	close(commitRelease)
	if !earlyReturn {
		select {
		case <-handlerDone:
		case <-time.After(5 * time.Second):
			t.Fatal("cross-fed set handler did not finish after CA commit")
		}
	}
	select {
	case <-readerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("new peer reader did not enter after CA commit completed")
	}

	if earlyBroadcast || earlyReader || earlyReturn {
		t.Fatalf("REST tx-33 crossed its policy barrier: broadcast_before_old_response=%v reader_during_ca_commit=%v return_before_ca_commit=%v",
			earlyBroadcast, earlyReader, earlyReturn)
	}
	if rr.Code != http.StatusCreated {
		t.Fatalf("cross-fed set status=%d body=%s", rr.Code, rr.Body.String())
	}
}
