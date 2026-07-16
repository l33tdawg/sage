package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestUpdateAgreementSharingReplacesSnapshotAndPreservesTreaty(t *testing.T) {
	node := newTestChain(t, "chain-local")
	pin := bytes.Repeat([]byte{0x42}, 32)
	wantDepts := []string{"engineering", "research"}
	if err := node.badger.SetCrossFed("chain-peer", "https://peer.example:8444", pin,
		3, 1_900_000_000, []string{"old", "removed"}, wantDepts, "active"); err != nil {
		t.Fatal(err)
	}

	var captured *tx.CrossFedTerms
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err != nil {
			return "", 0, err
		}
		captured = parsed.CrossFedTerms
		return "tx-sharing", 42, nil
	}

	input := []string{"zeta", "alpha"}
	result, err := node.mgr.UpdateAgreementSharing("chain-peer", input)
	if err != nil {
		t.Fatalf("UpdateAgreementSharing: %v", err)
	}
	if captured == nil {
		t.Fatal("no CrossFedTerms broadcast")
	}
	if got, want := strings.Join(captured.AllowedDomains, ","), "alpha,zeta"; got != want {
		t.Fatalf("replacement domains = %q, want %q", got, want)
	}
	if got := strings.Join(input, ","); got != "zeta,alpha" {
		t.Fatalf("caller input mutated: %q", got)
	}
	if captured.RemoteChainID != "chain-peer" || captured.Endpoint != "https://peer.example:8444" ||
		!bytes.Equal(captured.PeerPubKey, pin) || captured.MaxClearance != tx.ClearanceSecret ||
		captured.ExpiresAt != 1_900_000_000 || captured.Status != "active" ||
		strings.Join(captured.AllowedDepts, ",") != strings.Join(wantDepts, ",") {
		t.Fatalf("non-sharing treaty terms changed: %+v", captured)
	}
	if result.TxHash != "tx-sharing" || result.MaxClearance != 3 || strings.Join(result.Domains, ",") != "alpha,zeta" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestUpdateAgreementSharingAllowsEmptyFullSnapshot(t *testing.T) {
	node := newTestChain(t, "chain-local")
	if err := node.badger.SetCrossFed("chain-peer", "https://peer.example:8444", bytes.Repeat([]byte{7}, 32),
		2, 0, []string{"previous"}, []string{"*"}, "active"); err != nil {
		t.Fatal(err)
	}

	var captured *tx.CrossFedTerms
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err != nil {
			return "", 0, err
		}
		captured = parsed.CrossFedTerms
		return "tx-off", 1, nil
	}
	result, err := node.mgr.UpdateAgreementSharing("chain-peer", nil)
	if err != nil {
		t.Fatalf("disable sharing: %v", err)
	}
	if captured == nil || len(captured.AllowedDomains) != 0 {
		t.Fatalf("broadcast domains = %#v, want empty full snapshot", captured)
	}
	if result.Domains == nil || len(result.Domains) != 0 {
		t.Fatalf("result domains = %#v, want a present empty snapshot", result.Domains)
	}
}

func TestUpdateAgreementSharingValidatesCanonicalSnapshotBeforeBroadcast(t *testing.T) {
	node := newTestChain(t, "chain-local")
	if err := node.badger.SetCrossFed("chain-peer", "https://peer.example:8444", bytes.Repeat([]byte{7}, 32),
		2, 0, []string{"previous"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	broadcasts := 0
	node.mgr.broadcastFn = func(_ []byte) (string, int64, error) {
		broadcasts++
		return "", 0, errors.New("must not broadcast")
	}

	tooMany := make([]string, maxSharingDomains+1)
	for i := range tooMany {
		tooMany[i] = "domain-" + strings.Repeat("x", i+1)
	}
	tests := map[string][]string{
		"too many":       tooMany,
		"empty entry":    {""},
		"padded":         {" research"},
		"control":        {"research\nprivate"},
		"too long":       {strings.Repeat("x", maxSharingDomainLen+1)},
		"duplicate":      {"research", "research"},
		"wildcard mixed": {"*", "research"},
	}
	for name, domains := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := node.mgr.UpdateAgreementSharing("chain-peer", domains); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	if broadcasts != 0 {
		t.Fatalf("invalid snapshots caused %d broadcasts", broadcasts)
	}
}

func TestUpdateAgreementSharingSerializesCurrentTermsRead(t *testing.T) {
	node := newTestChain(t, "chain-local")
	pin := bytes.Repeat([]byte{7}, 32)
	if err := node.badger.SetCrossFed("chain-peer", "https://old.example:8444", pin,
		2, 0, []string{"old"}, nil, "active"); err != nil {
		t.Fatal(err)
	}

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondSeen := make(chan *tx.CrossFedTerms, 1)
	var once sync.Once
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err != nil {
			return "", 0, err
		}
		terms := parsed.CrossFedTerms
		if strings.Join(terms.AllowedDomains, ",") == "old" {
			once.Do(func() { close(firstEntered) })
			<-releaseFirst
			if err := node.badger.SetCrossFed(terms.RemoteChainID, terms.Endpoint, terms.PeerPubKey,
				uint8(terms.MaxClearance), terms.ExpiresAt, terms.AllowedDomains, terms.AllowedDepts, terms.Status); err != nil {
				return "", 0, err
			}
			return "tx-terms", 1, nil
		}
		secondSeen <- terms
		return "tx-sharing", 2, nil
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := node.mgr.broadcastCrossFedSet(&tx.CrossFedTerms{
			RemoteChainID: "chain-peer", Endpoint: "https://new.example:8444", PeerPubKey: pin,
			MaxClearance: tx.ClearanceSecret, AllowedDomains: []string{"old"},
			AllowedDepts: []string{"new-dept"}, ExpiresAt: 1_900_000_000, Status: "active",
		})
		firstDone <- err
	}()
	<-firstEntered

	updateDone := make(chan error, 1)
	go func() {
		_, err := node.mgr.UpdateAgreementSharing("chain-peer", []string{"shared"})
		updateDone <- err
	}()
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first terms update: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("sharing update: %v", err)
	}
	second := <-secondSeen
	if second.Endpoint != "https://new.example:8444" || second.MaxClearance != tx.ClearanceSecret ||
		second.ExpiresAt != 1_900_000_000 || strings.Join(second.AllowedDepts, ",") != "new-dept" {
		t.Fatalf("sharing update read stale treaty terms: %+v", second)
	}
}

func TestUpdateAgreementSharingCannotReactivateConcurrentRevoke(t *testing.T) {
	node := newTestChain(t, "chain-local")
	if err := node.badger.SetCrossFed("chain-peer", "https://peer.example:8444", bytes.Repeat([]byte{7}, 32),
		2, 0, []string{"old"}, nil, "active"); err != nil {
		t.Fatal(err)
	}

	revokeEntered := make(chan struct{})
	releaseRevoke := make(chan struct{})
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err != nil {
			return "", 0, err
		}
		if parsed.Type != tx.TxTypeCrossFedRevoke {
			return "", 0, errors.New("sharing update raced past revoke")
		}
		close(revokeEntered)
		<-releaseRevoke
		if err := node.badger.UpdateCrossFedStatus("chain-peer", "revoked"); err != nil {
			return "", 0, err
		}
		return "tx-revoke", 1, nil
	}

	revokeDone := make(chan error, 1)
	go func() {
		_, err := node.mgr.RevokeAgreement("chain-peer")
		revokeDone <- err
	}()
	<-revokeEntered
	updateDone := make(chan error, 1)
	go func() {
		_, err := node.mgr.UpdateAgreementSharing("chain-peer", []string{"shared"})
		updateDone <- err
	}()
	close(releaseRevoke)
	if err := <-revokeDone; err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := <-updateDone; err == nil || !strings.Contains(err.Error(), "status \"revoked\"") {
		t.Fatalf("post-revoke sharing update error = %v, want revoked agreement", err)
	}
}

func TestUpdateAgreementSharingNarrowingWaitsForLegacyQueryResponse(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x68}, 32)
	if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", pin,
		4, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	oldAgreement, err := m.ActiveAgreement("chain-peer")
	if err != nil {
		t.Fatal(err)
	}
	seedCommitted(t, ss, "legacy-leased-memory", "legacy.shared", "legacy leased response")
	if classificationErr := bs.SetMemoryClassification("legacy-leased-memory", 0); classificationErr != nil {
		t.Fatal(classificationErr)
	}

	body, err := json.Marshal(QueryRequest{
		Mode: ModeText, Query: "leased", DomainTag: "legacy.shared", TopK: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/query", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: oldAgreement,
	}))
	bw := newBlockingResponseWriter()
	queryDone := make(chan struct{})
	go func() {
		m.handleQuery(bw, req)
		close(queryDone)
	}()
	select {
	case <-bw.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy query did not reach its response write")
	}

	broadcastEntered := make(chan struct{})
	m.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, decodeErr := tx.DecodeTx(encoded)
		if decodeErr != nil {
			return "", 0, decodeErr
		}
		terms := parsed.CrossFedTerms
		close(broadcastEntered)
		if setErr := bs.SetCrossFed(terms.RemoteChainID, terms.Endpoint, terms.PeerPubKey,
			uint8(terms.MaxClearance), terms.ExpiresAt, terms.AllowedDomains, terms.AllowedDepts, terms.Status); setErr != nil {
			return "", 0, setErr
		}
		return "tx-narrow", 2, nil
	}
	updateDone := make(chan error, 1)
	go func() {
		_, updateErr := m.UpdateAgreementSharing("chain-peer", []string{"other"})
		updateDone <- updateErr
	}()

	earlyBroadcast := false
	select {
	case <-broadcastEntered:
		earlyBroadcast = true
	case <-time.After(100 * time.Millisecond):
	}
	earlyReturn := false
	select {
	case <-updateDone:
		earlyReturn = true
	case <-time.After(100 * time.Millisecond):
	}
	close(bw.release)
	select {
	case <-queryDone:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy query did not finish after its response was released")
	}
	var updateErr error
	if !earlyReturn {
		select {
		case updateErr = <-updateDone:
		case <-time.After(5 * time.Second):
			t.Fatal("agreement narrowing did not resume after the old response completed")
		}
	}
	if earlyBroadcast || earlyReturn {
		t.Fatalf("agreement narrowing crossed the policy barrier while the old query response was in flight: broadcast=%v return=%v",
			earlyBroadcast, earlyReturn)
	}
	if updateErr != nil {
		t.Fatalf("agreement narrowing: %v", updateErr)
	}
	if bw.status != http.StatusOK {
		t.Fatalf("in-flight legacy query status=%d, want 200", bw.status)
	}

	newAgreement, err := m.ActiveAgreement("chain-peer")
	if err != nil {
		t.Fatal(err)
	}
	denied := peerRBACQuery(t, m, newAgreement, "chain-peer", peerID, "legacy.shared", "leased")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("post-narrow legacy query status=%d want 403; body=%s", denied.Code, denied.Body.String())
	}
}

func TestBroadcastCrossFedSetWaitsForPolicyReaders(t *testing.T) {
	m, ss, _ := newDrainTestManager(t)
	broadcastEntered := make(chan struct{})
	m.broadcastFn = func([]byte) (string, int64, error) {
		close(broadcastEntered)
		return "tx-join", 3, nil
	}

	readUnlock := ss.LockSyncPolicyRead()
	broadcastDone := make(chan error, 1)
	go func() {
		_, err := m.broadcastCrossFedSet(&tx.CrossFedTerms{
			RemoteChainID: "chain-peer", Endpoint: "https://peer.example:8444",
			PeerPubKey: bytes.Repeat([]byte{0x69}, 32), MaxClearance: tx.ClearanceTopSecret,
			AllowedDomains: nil, Status: "active",
		})
		broadcastDone <- err
	}()

	earlyBroadcast := false
	select {
	case <-broadcastEntered:
		earlyBroadcast = true
	case <-time.After(100 * time.Millisecond):
	}
	earlyReturn := false
	select {
	case <-broadcastDone:
		earlyReturn = true
	case <-time.After(100 * time.Millisecond):
	}
	readUnlock()
	var broadcastErr error
	if !earlyReturn {
		select {
		case broadcastErr = <-broadcastDone:
		case <-time.After(5 * time.Second):
			t.Fatal("tx-33 broadcast did not resume after policy reader released")
		}
	}
	if earlyBroadcast || earlyReturn {
		t.Fatalf("tx-33 broadcast used by JOIN crossed the policy barrier: broadcast=%v return=%v", earlyBroadcast, earlyReturn)
	}
	if broadcastErr != nil {
		t.Fatal(broadcastErr)
	}
}

func TestHandleStatusAdvertisesServersDirectionalGrant(t *testing.T) {
	node := newTestChain(t, "chain-b")
	pin := bytes.Repeat([]byte{0x73}, 32)
	if err := node.badger.SetCrossFed("chain-a", "https://chain-a.example:8444", pin, 3, 0,
		[]string{"b.shared", "b.public"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	agreement, err := node.mgr.ActiveAgreement("chain-a")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-a", AgentID: newPeerOperatorID(t), Agreement: agreement,
	}))
	rec := httptest.NewRecorder()
	node.mgr.handleStatus(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status code = %d, body=%s", rec.Code, rec.Body.String())
	}
	var status StatusResponse
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.SharingGrant == nil {
		t.Fatal("peer omitted sharing_grant")
	}
	if got, want := strings.Join(status.SharingGrant.AllowedDomains, ","), "b.shared,b.public"; got != want {
		t.Fatalf("peer grant domains = %q, want %q", got, want)
	}
	if status.SharingGrant.MaxClearance != 3 {
		t.Fatalf("peer grant clearance = %d, want 3", status.SharingGrant.MaxClearance)
	}
}

func TestHandleStatusDistinguishesEmptyGrantFromLegacyPeer(t *testing.T) {
	node := newTestChain(t, "chain-b")
	if err := node.badger.SetCrossFed("chain-a", "https://chain-a.example:8444", bytes.Repeat([]byte{0x74}, 32),
		0, 0, nil, nil, "active"); err != nil {
		t.Fatal(err)
	}
	agreement, err := node.mgr.ActiveAgreement("chain-a")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-a", AgentID: newPeerOperatorID(t), Agreement: agreement,
	}))
	rec := httptest.NewRecorder()
	node.mgr.handleStatus(rec, req)

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(rec.Body).Decode(&raw); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	grantJSON, present := raw["sharing_grant"]
	if !present {
		t.Fatal("empty grant was omitted and became indistinguishable from a legacy peer")
	}
	var grant SharingGrant
	if err := json.Unmarshal(grantJSON, &grant); err != nil {
		t.Fatalf("decode sharing_grant: %v", err)
	}
	if grant.AllowedDomains == nil || len(grant.AllowedDomains) != 0 || grant.MaxClearance != 0 {
		t.Fatalf("empty sharing grant = %#v, want present [] at public clearance", grant)
	}
}
