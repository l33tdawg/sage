package rest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/tx"
)

const agreementMutationRaceTimeout = 5 * time.Second

var _ federationAgreementMutationLeaser = (*federation.Manager)(nil)

// agreementMutationRaceFederation models the live node-local artifacts that
// belong to one agreement generation. Its agreement lease deliberately lives
// outside SQLite so these tests cannot accidentally pass because the sync
// policy write barrier serialized the two REST handlers instead.
type agreementMutationRaceFederation struct {
	*fakeFederation

	agreementMu  sync.Mutex
	lockAttempts chan struct{}

	stageEntered     chan struct{}
	stageOnce        sync.Once
	caCommitEntered  chan struct{}
	caCommitOnce     sync.Once
	caCommitRelease  chan struct{}
	caReleaseOnce    sync.Once
	purgeEntered     chan struct{}
	purgeOnce        sync.Once
	purgeRelease     chan struct{}
	purgeReleaseOnce sync.Once

	stateMu                sync.Mutex
	localGenerationPresent bool
	events                 []string
}

var _ federationAgreementMutationLeaser = (*agreementMutationRaceFederation)(nil)

func newAgreementMutationRaceFederation(localGenerationPresent bool) *agreementMutationRaceFederation {
	return &agreementMutationRaceFederation{
		fakeFederation:         &fakeFederation{},
		lockAttempts:           make(chan struct{}, 4),
		stageEntered:           make(chan struct{}),
		caCommitEntered:        make(chan struct{}),
		caCommitRelease:        make(chan struct{}),
		purgeEntered:           make(chan struct{}),
		purgeRelease:           make(chan struct{}),
		localGenerationPresent: localGenerationPresent,
	}
}

func (f *agreementMutationRaceFederation) LockAgreementMutation() func() {
	// Signal before blocking. A test that receives this signal knows the
	// contender reached the exact serialization point; no scheduling sleep is
	// needed to assert that it cannot cross the held lease.
	f.lockAttempts <- struct{}{}
	f.agreementMu.Lock()
	return f.agreementMu.Unlock
}

func (f *agreementMutationRaceFederation) StageRemoteCA(string, []byte) ([]byte, func() error, func(), error) {
	f.recordEvent("stage")
	f.stageOnce.Do(func() { close(f.stageEntered) })
	commit := func() error {
		f.recordEvent("ca-enter")
		f.caCommitOnce.Do(func() { close(f.caCommitEntered) })
		<-f.caCommitRelease
		f.setLocalGenerationPresent(true)
		f.recordEvent("ca-finish")
		return nil
	}
	return make([]byte, 32), commit, func() {}, nil
}

func (f *agreementMutationRaceFederation) PurgeLocalFederationState(string) {
	f.recordEvent("purge-enter")
	f.purgeOnce.Do(func() { close(f.purgeEntered) })
	<-f.purgeRelease
	f.setLocalGenerationPresent(false)
	f.recordEvent("purge-finish")
}

func (f *agreementMutationRaceFederation) releaseCACommit() {
	f.caReleaseOnce.Do(func() { close(f.caCommitRelease) })
}

func (f *agreementMutationRaceFederation) releasePurge() {
	f.purgeReleaseOnce.Do(func() { close(f.purgeRelease) })
}

func (f *agreementMutationRaceFederation) setLocalGenerationPresent(present bool) {
	f.stateMu.Lock()
	f.localGenerationPresent = present
	f.stateMu.Unlock()
}

func (f *agreementMutationRaceFederation) localGenerationIsPresent() bool {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	return f.localGenerationPresent
}

func (f *agreementMutationRaceFederation) recordEvent(event string) {
	f.stateMu.Lock()
	f.events = append(f.events, event)
	f.stateMu.Unlock()
}

func (f *agreementMutationRaceFederation) eventSnapshot() []string {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	return append([]string(nil), f.events...)
}

type agreementMutationRaceRPC struct {
	setCommitted    chan struct{}
	setOnce         sync.Once
	revokeCommitted chan struct{}
	revokeOnce      sync.Once

	errMu sync.Mutex
	err   error
}

func newAgreementMutationRaceRPC(f *agreementMutationRaceFederation) (*httptest.Server, *agreementMutationRaceRPC) {
	rpc := &agreementMutationRaceRPC{
		setCommitted:    make(chan struct{}),
		revokeCommitted: make(chan struct{}),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txHex := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		raw, err := hex.DecodeString(txHex)
		if err != nil {
			rpc.setError(fmt.Errorf("decode Comet tx hex: %w", err))
			http.Error(w, "bad tx", http.StatusBadRequest)
			return
		}
		parsed, err := tx.DecodeTx(raw)
		if err != nil {
			rpc.setError(fmt.Errorf("decode Comet tx: %w", err))
			http.Error(w, "bad tx", http.StatusBadRequest)
			return
		}

		hash := ""
		switch parsed.Type {
		case tx.TxTypeCrossFedSet:
			f.recordEvent("set-tx")
			rpc.setOnce.Do(func() { close(rpc.setCommitted) })
			hash = "TX33RACE"
		case tx.TxTypeCrossFedRevoke:
			f.recordEvent("revoke-tx")
			rpc.revokeOnce.Do(func() { close(rpc.revokeCommitted) })
			hash = "TX34RACE"
		default:
			rpc.setError(fmt.Errorf("unexpected transaction type %d", parsed.Type))
			http.Error(w, "unexpected tx", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"check_tx":  map[string]any{"code": 0, "log": ""},
				"tx_result": map[string]any{"code": 0, "log": "agreement updated"},
				"hash":      hash,
				"height":    "7",
			},
		})
	}))
	return server, rpc
}

func (r *agreementMutationRaceRPC) setError(err error) {
	r.errMu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.errMu.Unlock()
}

func (r *agreementMutationRaceRPC) error() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

type agreementMutationHTTPCall struct {
	response *httptest.ResponseRecorder
	done     chan struct{}
}

func startAgreementMutationHTTPCall(handler http.Handler, req *http.Request) *agreementMutationHTTPCall {
	call := &agreementMutationHTTPCall{
		response: httptest.NewRecorder(),
		done:     make(chan struct{}),
	}
	go func() {
		defer close(call.done)
		handler.ServeHTTP(call.response, req)
	}()
	return call
}

func crossFedSetRaceRequest(t *testing.T) *http.Request {
	t.Helper()
	body, err := json.Marshal(CrossFedSetRequest{
		RemoteChainID:  "chain-peer",
		Endpoint:       "https://peer.example:8444",
		RemoteCAPEM:    "staged by race fixture",
		MaxClearance:   4,
		AllowedDomains: []string{"shared"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequest(http.MethodPost, "/v1/federation/cross", bytes.NewReader(body))
}

func crossFedRevokeRaceRequest() *http.Request {
	return httptest.NewRequest(http.MethodPost, "/v1/federation/cross/chain-peer/revoke", nil)
}

func waitForAgreementRaceSignal(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(agreementMutationRaceTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func assertAgreementRaceSignalOpen(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signal:
		t.Fatalf("%s crossed the held agreement mutation lease", what)
	default:
	}
}

func waitForAgreementMutationCall(t *testing.T, call *agreementMutationHTTPCall, what string) {
	t.Helper()
	select {
	case <-call.done:
	case <-time.After(agreementMutationRaceTimeout):
		t.Fatalf("timed out waiting for %s handler", what)
	}
}

func cleanupAgreementMutationRace(t *testing.T, f *agreementMutationRaceFederation, calls *[]*agreementMutationHTTPCall) {
	t.Helper()
	f.releaseCACommit()
	f.releasePurge()
	for i, call := range *calls {
		if call == nil {
			continue
		}
		select {
		case <-call.done:
		case <-time.After(agreementMutationRaceTimeout):
			t.Errorf("agreement mutation handler %d did not exit during cleanup", i)
		}
	}
}

func TestCrossFedAgreementLeaseOrdersRevokePurgeBeforeNewSet(t *testing.T) {
	// The old generation starts with live local artifacts. Revoke commits and
	// pauses at purge entry. A concurrent set must not even stage its CA until
	// that purge finishes, or the old revoke can delete the new generation.
	fed := newAgreementMutationRaceFederation(true)
	fed.releaseCACommit() // set promotion is not the blocker in this interleaving
	calls := make([]*agreementMutationHTTPCall, 0, 2)

	comet, rpc := newAgreementMutationRaceRPC(fed)
	defer comet.Close()
	defer cleanupAgreementMutationRace(t, fed, &calls)
	srv, _, _ := newTestServer(t, comet.URL)
	srv.SetNodeOperatorID("node-operator")
	srv.SetFederation(fed)
	router := legacyFederationControlRouter(srv, "node-operator")

	revokeCall := startAgreementMutationHTTPCall(router, crossFedRevokeRaceRequest())
	calls = append(calls, revokeCall)
	waitForAgreementRaceSignal(t, fed.lockAttempts, "revoke lease acquisition")
	waitForAgreementRaceSignal(t, rpc.revokeCommitted, "tx-34 commit")
	waitForAgreementRaceSignal(t, fed.purgeEntered, "revoke purge entry")

	setCall := startAgreementMutationHTTPCall(router, crossFedSetRaceRequest(t))
	calls = append(calls, setCall)
	select {
	case <-fed.lockAttempts:
		// The set reached the lease and is now blocked behind revoke.
	case <-fed.stageEntered:
		t.Fatal("new set staged its CA while the earlier revoke was paused in purge")
	case <-time.After(agreementMutationRaceTimeout):
		t.Fatal("new set reached neither the agreement lease nor CA staging")
	}
	assertAgreementRaceSignalOpen(t, fed.stageEntered, "new set CA staging")
	assertAgreementRaceSignalOpen(t, rpc.setCommitted, "new set tx-33")
	assertAgreementRaceSignalOpen(t, setCall.done, "new set handler return")

	fed.releasePurge()
	waitForAgreementMutationCall(t, revokeCall, "revoke")
	waitForAgreementRaceSignal(t, fed.stageEntered, "new set CA staging after purge")
	waitForAgreementRaceSignal(t, rpc.setCommitted, "new set tx-33 commit after purge")
	waitForAgreementRaceSignal(t, fed.caCommitEntered, "new set CA promotion")
	waitForAgreementMutationCall(t, setCall, "set")

	if revokeCall.response.Code != http.StatusOK {
		t.Fatalf("revoke status=%d body=%s", revokeCall.response.Code, revokeCall.response.Body.String())
	}
	if setCall.response.Code != http.StatusCreated {
		t.Fatalf("set status=%d body=%s", setCall.response.Code, setCall.response.Body.String())
	}
	if err := rpc.error(); err != nil {
		t.Fatal(err)
	}
	if !fed.localGenerationIsPresent() {
		t.Fatal("completed old revoke purged the newer active agreement generation")
	}
	wantEvents := []string{"revoke-tx", "purge-enter", "purge-finish", "stage", "set-tx", "ca-enter", "ca-finish"}
	if got := fed.eventSnapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("agreement mutation order=%v want=%v", got, wantEvents)
	}
}

func TestCrossFedAgreementLeaseOrdersSetCAPromotionBeforeRevoke(t *testing.T) {
	// The set commits tx-33 and pauses before promoting its matching CA. Revoke
	// must not commit tx-34 or purge until promotion finishes, or the delayed CA
	// commit can resurrect local artifacts after the agreement is revoked.
	fed := newAgreementMutationRaceFederation(false)
	fed.releasePurge() // CA promotion is the blocker in this interleaving
	calls := make([]*agreementMutationHTTPCall, 0, 2)

	comet, rpc := newAgreementMutationRaceRPC(fed)
	defer comet.Close()
	defer cleanupAgreementMutationRace(t, fed, &calls)
	srv, _, _ := newTestServer(t, comet.URL)
	srv.SetNodeOperatorID("node-operator")
	srv.SetFederation(fed)
	router := legacyFederationControlRouter(srv, "node-operator")

	setCall := startAgreementMutationHTTPCall(router, crossFedSetRaceRequest(t))
	calls = append(calls, setCall)
	waitForAgreementRaceSignal(t, fed.lockAttempts, "set lease acquisition")
	waitForAgreementRaceSignal(t, rpc.setCommitted, "tx-33 commit")
	waitForAgreementRaceSignal(t, fed.caCommitEntered, "CA promotion entry")

	revokeCall := startAgreementMutationHTTPCall(router, crossFedRevokeRaceRequest())
	calls = append(calls, revokeCall)
	select {
	case <-fed.lockAttempts:
		// Revoke reached the lease and is now blocked behind CA promotion.
	case <-rpc.revokeCommitted:
		t.Fatal("revoke committed tx-34 while the earlier set was promoting its CA")
	case <-time.After(agreementMutationRaceTimeout):
		t.Fatal("revoke reached neither the agreement lease nor tx-34 commit")
	}
	assertAgreementRaceSignalOpen(t, rpc.revokeCommitted, "revoke tx-34")
	assertAgreementRaceSignalOpen(t, fed.purgeEntered, "revoke purge")
	assertAgreementRaceSignalOpen(t, revokeCall.done, "revoke handler return")

	fed.releaseCACommit()
	waitForAgreementMutationCall(t, setCall, "set")
	waitForAgreementRaceSignal(t, rpc.revokeCommitted, "tx-34 commit after CA promotion")
	waitForAgreementRaceSignal(t, fed.purgeEntered, "revoke purge after CA promotion")
	waitForAgreementMutationCall(t, revokeCall, "revoke")

	if setCall.response.Code != http.StatusCreated {
		t.Fatalf("set status=%d body=%s", setCall.response.Code, setCall.response.Body.String())
	}
	if revokeCall.response.Code != http.StatusOK {
		t.Fatalf("revoke status=%d body=%s", revokeCall.response.Code, revokeCall.response.Body.String())
	}
	if err := rpc.error(); err != nil {
		t.Fatal(err)
	}
	if fed.localGenerationIsPresent() {
		t.Fatal("delayed CA promotion resurrected local artifacts after revoke")
	}
	wantEvents := []string{"stage", "set-tx", "ca-enter", "ca-finish", "revoke-tx", "purge-enter", "purge-finish"}
	if got := fed.eventSnapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("agreement mutation order=%v want=%v", got, wantEvents)
	}
}
