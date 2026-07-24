package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

type blockingStatusWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
}

func (w *blockingStatusWriter) Header() http.Header { return w.header }
func (w *blockingStatusWriter) WriteHeader(int)     {}
func (w *blockingStatusWriter) Write(p []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(p), nil
}

func TestStatusPipeContactsDeriveEffectiveOwnerContacts(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	m.SetNetworkName("Amy's SAGE")
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)

	activeOwner := newPeerOperatorID(t)
	inactiveOwner := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
		AgentID: activeOwner, Name: "tii-sentinel", RegisteredName: "tii-sentinel", Status: "active",
	}))
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
		AgentID: inactiveOwner, Name: "ops-agent", RegisteredName: "ops-agent", Status: "inactive",
	}))
	require.NoError(t, bs.RegisterDomain("tii", activeOwner, "", 1))
	require.NoError(t, bs.RegisterDomain("ops", inactiveOwner, "", 2))
	require.NoError(t, bs.RegisterDomain("open.shared", activeOwner, "", 3))
	require.NoError(t, bs.SetSharedDomain("open.shared"))

	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "general", Read: true},
		{Domain: "open.shared", Read: true},
		{Domain: "ops.alerts", Read: true},
		{Domain: "tii.findings", Copy: true},
		{Domain: "tii.security", Read: true},
	})
	require.NoError(t, err)

	status := statusForPeer(t, m, "chain-peer", peerID, agreement)
	require.NotNil(t, status.PipeContacts)
	require.Equal(t, PipeContactVersion, status.PipeContacts.Version)
	require.NotEmpty(t, status.PipeContacts.AgreementID)
	require.NotEmpty(t, status.PipeContacts.Revision)
	require.False(t, status.PipeContacts.Paused)
	require.Len(t, status.PipeContacts.Contacts, 2, "ownerless and open-shared domains must not create contacts")

	contacts := make(map[string]PipeContact, len(status.PipeContacts.Contacts))
	for _, contact := range status.PipeContacts.Contacts {
		contacts[contact.AgentID] = contact
	}
	active := contacts[activeOwner]
	require.Equal(t, "tii-sentinel", active.DisplayName)
	require.True(t, active.Available)
	require.False(t, active.Accepting, "discovery must not silently authorize inbound pipes")
	require.NotEmpty(t, active.ContactID)
	require.Equal(t, activeOwner+"@chain-local", active.Address)
	require.True(t, strings.HasPrefix(active.Handle, "#amy-s-sage-"), active.Handle)
	require.True(t, strings.HasSuffix(active.Handle, "/"+activeOwner[:pipeContactMinAgentPrefix]), active.Handle)
	require.Equal(t, []PipeContactDomain{
		{Domain: "tii.findings", OwningDomain: "tii", OwnerHeight: 1},
		{Domain: "tii.security", OwningDomain: "tii", OwnerHeight: 1},
	}, active.Domains)

	inactive := contacts[inactiveOwner]
	require.Equal(t, "ops-agent", inactive.DisplayName)
	require.False(t, inactive.Available)
	require.False(t, inactive.Accepting)
	require.Equal(t, []PipeContactDomain{{Domain: "ops.alerts", OwningDomain: "ops", OwnerHeight: 2}}, inactive.Domains)
	require.Contains(t, status.Capabilities, CapabilityFederatedPipeline)
}

func TestPipeContactsIncludeActiveAgentsWithSharedDomainAccess(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	// The shared policy targets a child while the grants sit on its owner
	// ancestor. This pins the live app-v8 ancestor-aware access semantics used
	// by the node wiring, rather than only testing an exact grant coincidence.
	m.postV8ForAccess = func() bool { return true }
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)

	owner := newPeerOperatorID(t)
	reader := newPeerOperatorID(t)
	writer := newPeerOperatorID(t)
	unrelated := newPeerOperatorID(t)
	inactive := newPeerOperatorID(t)
	for _, agent := range []struct {
		id             string
		name           string
		registeredName string
		provider       string
		status         string
	}{
		{owner, "domain-owner", "domain-owner", "local", "active"},
		{reader, "Research worker", "innovium", "claude-code", "active"},
		{writer, "domain-writer", "domain-writer", "local", "active"},
		{unrelated, "unrelated", "unrelated", "local", "active"},
		{inactive, "inactive-reader", "inactive-reader", "local", "inactive"},
	} {
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
			AgentID: agent.id, Name: agent.name, RegisteredName: agent.registeredName,
			Provider: agent.provider, Status: agent.status,
		}))
	}
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	require.NoError(t, bs.SetAccessGrant("research", reader, 1, 0, owner))
	require.NoError(t, bs.SetAccessGrant("research", writer, 2, 0, owner))
	require.NoError(t, bs.SetAccessGrant("research", inactive, 1, 0, owner))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.findings", Read: true}})
	require.NoError(t, err)

	grant := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.NotNil(t, grant)
	contacts := make(map[string]PipeContact, len(grant.Contacts))
	for _, contact := range grant.Contacts {
		contacts[contact.AgentID] = contact
	}
	require.Contains(t, contacts, owner, "the effective domain owner stays eligible")
	require.Contains(t, contacts, reader, "read grant recipients get a shared-domain inbox")
	require.Contains(t, contacts, writer, "write grants imply the required read capability")
	require.NotContains(t, contacts, unrelated)
	require.NotContains(t, contacts, inactive, "inactive agents never become new federated inbox contacts")
	readerContact := contacts[reader]
	require.Equal(t, []PipeContactDomain{{Domain: "research.findings", OwningDomain: "research", OwnerHeight: 10}}, readerContact.Domains)
	require.Equal(t, "Research worker", readerContact.DisplayName)
	require.Equal(t, "innovium", readerContact.RegisteredName)
	require.Equal(t, "claude-code", readerContact.Provider)

	updated, err := m.SetPipeContactAcceptance(ctx, "chain-peer", reader, readerContact.ContactID, true)
	require.NoError(t, err)
	var accepted PipeContact
	for _, contact := range updated.Contacts {
		if contact.AgentID == reader {
			accepted = contact
			break
		}
	}
	require.True(t, accepted.Accepting)
	policy, err := m.GetPeerRBACPolicy(ctx, "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, policy)
	event := &PipeEvent{
		PolicyEpoch:     policy.PolicyEpoch,
		AgreementID:     updated.AgreementID,
		ContactID:       accepted.ContactID,
		ContactRevision: pipeContactAuthorizationRevision(updated, &accepted),
		TargetAgentID:   reader,
	}
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: peerID, Agreement: agreement}
	resolved, err := m.authorizeInboundPipeContact(ctx, peer, event)
	require.NoError(t, err)
	require.Equal(t, reader, resolved.AgentID)

	// A grant revoke removes the recipient from the live projection, so an
	// already queued event cannot be accepted with a stale contact revision.
	require.NoError(t, bs.DeleteAccessGrant("research", reader))
	_, err = m.authorizeInboundPipeContact(ctx, peer, event)
	require.ErrorIs(t, err, ErrFederatedPipeInvalid)
}

func TestLargeSharedRecipientProjectionUsesTargetedLookup(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	owner := newPeerOperatorID(t)
	target := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
		AgentID: target, Name: "Innovium", RegisteredName: "innovium", Provider: "claude-code", Status: "active",
	}))
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	require.NoError(t, bs.SetAccessGrant("research", target, 1, 0, owner))
	// A valid shared-domain recipient set can exceed the legacy v1 status
	// envelope. Every recipient remains discoverable by the new lookup route;
	// none are silently dropped by a cache or snapshot cap.
	for i := 0; i < maxPipeContactStatusContacts; i++ {
		reader := newPeerOperatorID(t)
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: reader, Name: fmt.Sprintf("reader-%04d", i), Status: "active"}))
		require.NoError(t, bs.SetAccessGrant("research", reader, 1, 0, owner))
	}
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research", Read: true}})
	require.NoError(t, err)
	admin, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	require.LessOrEqual(t, len(admin.Contacts), maxPipeContactStatusCandidates+1,
		"the CEREBRUM contact endpoint must not construct a full recipient roster")
	// The browser's default administrative view is intentionally bounded. A
	// consent change asks for its exact target in addition to that sample, so
	// an operator can still enable any authorized recipient without a full
	// roster scan under revocation locks.
	grant, err := m.localPipeContacts(ctx, "chain-peer", target)
	require.NoError(t, err)
	var targetContact PipeContact
	for _, contact := range grant.Contacts {
		if contact.AgentID == target {
			targetContact = contact
			break
		}
	}
	require.NotEmpty(t, targetContact.ContactID)
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", target, targetContact.ContactID, true)
	require.NoError(t, err)

	status := statusForPeer(t, m, "chain-peer", peerID, agreement)
	require.NotNil(t, status.PipeContacts, "legacy peers retain a valid bounded status snapshot")
	require.LessOrEqual(t, len(status.PipeContacts.Contacts), maxPipeContactStatusContacts)
	require.LessOrEqual(t, len(status.PipeContacts.Contacts), maxPipeContactStatusCandidates+1,
		"legacy status construction must not scan the entire recipient roster")
	require.True(t, fitsPipeContactStatusSnapshot(status.PipeContacts))
	require.NoError(t, ValidateRemotePipeContactGrant("chain-local", status.PipeContacts))
	require.Contains(t, status.Capabilities, CapabilityFederatedPipelineContactLookup)
	compactReq := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	compactReq.Header.Set(HeaderClientCapabilities, CapabilityFederatedPipelineContactLookup)
	compactReq = compactReq.WithContext(context.WithValue(compactReq.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	compactRec := httptest.NewRecorder()
	m.handleStatus(compactRec, compactReq)
	require.Equal(t, http.StatusOK, compactRec.Code, compactRec.Body.String())
	var compact StatusResponse
	require.NoError(t, json.NewDecoder(compactRec.Body).Decode(&compact))
	require.Nil(t, compact.PipeContacts, "lookup-capable clients must not force the legacy roster projection")

	body, err := json.Marshal(PipeContactLookupRequest{Name: "innovium", Limit: 20})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/pipe/contacts/lookup", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	rec := httptest.NewRecorder()
	m.handlePipeContactLookup(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response PipeContactLookupResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	require.Equal(t, 1, response.Total)
	require.False(t, response.Truncated)
	require.Len(t, response.Grant.Contacts, 1)
	require.Equal(t, target, response.Grant.Contacts[0].AgentID)
	require.True(t, response.Grant.Contacts[0].Accepting)

	oversizedBody, err := json.Marshal(PipeContactLookupRequest{Name: "innovium", Limit: maxPipeContactLookupResults + 1})
	require.NoError(t, err)
	oversizedReq := httptest.NewRequest(http.MethodPost, "/fed/v1/pipe/contacts/lookup", bytes.NewReader(oversizedBody))
	oversizedReq = oversizedReq.WithContext(context.WithValue(oversizedReq.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	oversizedRec := httptest.NewRecorder()
	m.handlePipeContactLookup(oversizedRec, oversizedReq)
	require.Equal(t, http.StatusBadRequest, oversizedRec.Code, oversizedRec.Body.String())
}

func TestLegacyStatusLoadsAnOwnerOutsideItsCandidateSample(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)

	// Status samples the lowest agent IDs first. Put the owner above every
	// sampled ID to prove that the owner-specific metadata load, rather than
	// incidental candidate ordering, makes legacy routing viable.
	owner := strings.Repeat("f", 64)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{
		AgentID: owner, Name: "out-of-sample-owner", RegisteredName: "owner", Provider: "local", Status: "active",
	}))
	for i := 1; i <= maxPipeContactStatusCandidates; i++ {
		agentID := fmt.Sprintf("%064x", i)
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: agentID, Name: fmt.Sprintf("sample-%03d", i), Status: "active"}))
	}
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research", Read: true}})
	require.NoError(t, err)

	status := statusForPeer(t, m, "chain-peer", peerID, agreement)
	require.NotNil(t, status.PipeContacts)
	var contact *PipeContact
	for i := range status.PipeContacts.Contacts {
		if status.PipeContacts.Contacts[i].AgentID == owner {
			contact = &status.PipeContacts.Contacts[i]
			break
		}
	}
	require.NotNil(t, contact, "effective owners remain visible beyond the status candidate sample")
	require.Equal(t, "out-of-sample-owner", contact.DisplayName)
	require.True(t, contact.Available)
	require.NotEmpty(t, contact.Address)
}

func TestPipeContactStatusSnapshotHasByteLimit(t *testing.T) {
	wide := strings.Repeat("domain-", 36) // 252 bytes, representative max-width names
	grant := &PipeContactGrant{Contacts: []PipeContact{{}, {}}}
	for i := range grant.Contacts {
		grant.Contacts[i].Domains = make([]PipeContactDomain, maxPipeContactStatusContacts)
		for j := range grant.Contacts[i].Domains {
			grant.Contacts[i].Domains[j] = PipeContactDomain{Domain: wide, OwningDomain: wide}
		}
	}
	require.False(t, fitsPipeContactStatusSnapshot(grant), "a status contact envelope must leave room below the federated response limit")
	bounded := boundedPipeContactStatusSnapshot(grant)
	require.NotNil(t, bounded)
	require.NotEmpty(t, bounded.Contacts)
	require.Less(t, len(bounded.Contacts), len(grant.Contacts))
	require.True(t, fitsPipeContactStatusSnapshot(bounded))
}

func TestPipeContactLookupCapsTargetMatchesAndResponseBytes(t *testing.T) {
	grant := &PipeContactGrant{Contacts: make([]PipeContact, 0, maxPipeContactLookupResults+1)}
	for i := 0; i < maxPipeContactLookupResults+1; i++ {
		grant.Contacts = append(grant.Contacts, PipeContact{
			AgentID: fmt.Sprintf("agent-%02d", i), DisplayName: "worker",
		})
	}
	filtered, total := filterPipeContactLookup(grant, PipeContactLookupRequest{Target: "worker", Limit: maxPipeContactLookupResults})
	require.Equal(t, maxPipeContactLookupResults+1, total)
	require.Len(t, filtered.Contacts, maxPipeContactLookupResults)
	for _, invalidLimit := range []int{0, -1, maxPipeContactLookupResults + 1} {
		filtered, total = filterPipeContactLookup(grant, PipeContactLookupRequest{Target: "worker", Limit: invalidLimit})
		require.Equal(t, maxPipeContactLookupResults+1, total)
		require.Len(t, filtered.Contacts, maxPipeContactLookupResults)
	}

	// Twenty matching contacts can still be too wide if each one has a large
	// but valid shared-domain basis. The handler must return a byte-bounded
	// subset and preserve that the result was truncated.
	wide := strings.Repeat("x", 512)
	wideGrant := &PipeContactGrant{Contacts: make([]PipeContact, 0, maxPipeContactLookupResults)}
	for i := 0; i < maxPipeContactLookupResults; i++ {
		contact := PipeContact{AgentID: fmt.Sprintf("wide-%02d", i), DisplayName: "wide-worker"}
		for j := 0; j < maxPipeContactStatusContacts; j++ {
			contact.Domains = append(contact.Domains, PipeContactDomain{Domain: wide, OwningDomain: wide})
		}
		wideGrant.Contacts = append(wideGrant.Contacts, contact)
	}
	response, err := boundedPipeContactLookupResponse(wideGrant, len(wideGrant.Contacts))
	require.NoError(t, err)
	require.NotEmpty(t, response.Grant.Contacts)
	require.Less(t, len(response.Grant.Contacts), len(wideGrant.Contacts))
	require.True(t, response.Truncated)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), maxPipeContactLookupBytes)
}

func TestPipeContactAcceptanceDefaultsOffPersistsPauseAndInvalidatesChanges(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	owner := newPeerOperatorID(t)
	replacement := newPeerOperatorID(t)
	for _, agentID := range []string{owner, replacement} {
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: agentID, Name: "owner", Status: "active"}))
	}
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)

	initial := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.Len(t, initial.Contacts, 1)
	contact := initial.Contacts[0]
	require.False(t, contact.Accepting)
	updated, err := m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
	require.NoError(t, err)
	require.True(t, updated.Contacts[0].Accepting)
	require.Equal(t, contact.ContactID, updated.Contacts[0].ContactID)

	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", true)
	require.NoError(t, err)
	paused := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.True(t, paused.Paused)
	require.True(t, paused.Contacts[0].Accepting, "pause must preserve the operator's stored choice")
	require.Equal(t, contact.ContactID, paused.Contacts[0].ContactID)
	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", false)
	require.NoError(t, err)
	resumed := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.False(t, resumed.Paused)
	require.True(t, resumed.Contacts[0].Accepting)

	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)
	replacedPolicy := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.False(t, replacedPolicy.Contacts[0].Accepting, "full domain replacement must require an explicit re-enable")
	require.NotEqual(t, contact.ContactID, replacedPolicy.Contacts[0].ContactID)
	require.ErrorIs(t, func() error {
		_, setErr := m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
		return setErr
	}(), ErrPipeContactChanged)

	current := replacedPolicy.Contacts[0]
	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, current.ContactID, true)
	require.NoError(t, err)
	require.NoError(t, bs.TransferDomain("research", replacement, "", 11))
	transferred := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.Equal(t, replacement, transferred.Contacts[0].AgentID)
	require.False(t, transferred.Contacts[0].Accepting, "ownership transfer must not inherit consent")
}

func TestPipeContactRevisionTracksOwnerAvailabilityAndPause(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	m.SetNetworkName("Owner Lab")
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)

	ownerA := newPeerOperatorID(t)
	ownerB := newPeerOperatorID(t)
	for _, agent := range []*store.AgentEntry{
		{AgentID: ownerA, Name: "owner-a", Status: "active"},
		{AgentID: ownerB, Name: "owner-b", Status: "active"},
	} {
		require.NoError(t, ss.CreateAgent(ctx, agent))
	}
	require.NoError(t, bs.RegisterDomain("research", ownerA, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.child", Read: true}})
	require.NoError(t, err)

	first := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.Len(t, first.Contacts, 1)
	require.Equal(t, ownerA, first.Contacts[0].AgentID)

	m.SetNetworkName("Renamed Owner Lab")
	networkRenamed := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.NotEqual(t, first.Contacts[0].Handle, networkRenamed.Contacts[0].Handle)
	require.Equal(t, first.Revision, networkRenamed.Revision, "a cosmetic network rename is not an authorization change")
	agent, err := ss.GetAgent(ctx, ownerA)
	require.NoError(t, err)
	agent.Name = "renamed-owner-a"
	require.NoError(t, ss.UpdateAgent(ctx, agent))
	agentRenamed := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.Equal(t, "renamed-owner-a", agentRenamed.Contacts[0].DisplayName)
	require.Equal(t, first.Revision, agentRenamed.Revision, "a cosmetic agent rename is not an authorization change")

	require.NoError(t, bs.TransferDomain("research", ownerB, "", 11))
	transferred := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.Len(t, transferred.Contacts, 1)
	require.Equal(t, ownerB, transferred.Contacts[0].AgentID)
	require.NotEqual(t, first.Revision, transferred.Revision)
	require.False(t, transferred.Contacts[0].Accepting, "new owner must never inherit acceptance")

	require.NoError(t, ss.UpdateAgentStatus(ctx, ownerB, "inactive"))
	inactive := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.False(t, inactive.Contacts[0].Available)
	require.NotEqual(t, transferred.Revision, inactive.Revision)

	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", true)
	require.NoError(t, err)
	paused := statusForPeer(t, m, "chain-peer", peerID, agreement).PipeContacts
	require.True(t, paused.Paused)
	require.NotEqual(t, inactive.Revision, paused.Revision)
}

func TestPipeContactAuthorizationRevisionIgnoresUnrelatedReversibleState(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	ownerX, ownerY := newPeerOperatorID(t), newPeerOperatorID(t)
	for _, owner := range []string{ownerX, ownerY} {
		require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: owner[:8], Status: "active"}))
	}
	require.NoError(t, bs.RegisterDomain("exact-x", ownerX, "", 10))
	require.NoError(t, bs.RegisterDomain("unrelated-y", ownerY, "", 11))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "exact-x.work", Read: true},
		{Domain: "unrelated-y.work", Read: true},
	})
	require.NoError(t, err)

	find := func(t *testing.T, grant *PipeContactGrant, agentID string) PipeContact {
		t.Helper()
		for _, contact := range grant.Contacts {
			if contact.AgentID == agentID {
				return contact
			}
		}
		t.Fatalf("contact %s not found", agentID)
		return PipeContact{}
	}
	grant, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	for _, owner := range []string{ownerX, ownerY} {
		contact := find(t, grant, owner)
		_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
		require.NoError(t, err)
		grant, err = m.LocalPipeContacts(ctx, "chain-peer")
		require.NoError(t, err)
	}
	baseline := grant
	xBaseline, yBaseline := find(t, baseline, ownerX), find(t, baseline, ownerY)
	xRevision := pipeContactAuthorizationRevision(baseline, &xBaseline)
	yRevision := pipeContactAuthorizationRevision(baseline, &yBaseline)

	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", ownerY, yBaseline.ContactID, false)
	require.NoError(t, err)
	yOff, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	xAfterYOff, yAfterOff := find(t, yOff, ownerX), find(t, yOff, ownerY)
	require.NotEqual(t, baseline.Revision, yOff.Revision, "aggregate cache revision must see Y's state")
	require.Equal(t, xRevision, pipeContactAuthorizationRevision(yOff, &xAfterYOff), "Y must not invalidate exact X work")
	require.NotEqual(t, yRevision, pipeContactAuthorizationRevision(yOff, &yAfterOff))

	_, err = m.SetPipeContactAcceptance(ctx, "chain-peer", ownerY, yBaseline.ContactID, true)
	require.NoError(t, err)
	restored, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	xRestored, yRestored := find(t, restored, ownerX), find(t, restored, ownerY)
	require.Equal(t, xRevision, pipeContactAuthorizationRevision(restored, &xRestored))
	require.Equal(t, yRevision, pipeContactAuthorizationRevision(restored, &yRestored))

	require.NoError(t, ss.UpdateAgentStatus(ctx, ownerY, "inactive"))
	yInactive, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	xWhileYInactive, yWhileInactive := find(t, yInactive, ownerX), find(t, yInactive, ownerY)
	require.Equal(t, xRevision, pipeContactAuthorizationRevision(yInactive, &xWhileYInactive), "Y availability must not invalidate X")
	require.NotEqual(t, yRevision, pipeContactAuthorizationRevision(yInactive, &yWhileInactive))

	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", true)
	require.NoError(t, err)
	paused, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	xPaused := find(t, paused, ownerX)
	require.NotEqual(t, xRevision, pipeContactAuthorizationRevision(paused, &xPaused))
	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", false)
	require.NoError(t, err)
	require.NoError(t, ss.UpdateAgentStatus(ctx, ownerY, "active"))
	resumed, err := m.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	xResumed := find(t, resumed, ownerX)
	require.Equal(t, xRevision, pipeContactAuthorizationRevision(resumed, &xResumed), "Resume must restore unchanged X work")
}

func TestStatusReleasesContactSnapshotBeforePeerSocketWrite(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	owner := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "owner", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	w := &blockingStatusWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		m.handleStatus(w, req)
		close(done)
	}()
	<-w.started

	updated := make(chan error, 1)
	go func() { updated <- ss.UpdateAgentStatus(ctx, owner, "inactive") }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("a slow status response retained the contact snapshot lease")
	}
	close(w.release)
	<-done
}

func TestPipeContactLookupReleasesSnapshotBeforePeerSocketWrite(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	owner := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: owner, Name: "owner", Status: "active"}))
	require.NoError(t, bs.RegisterDomain("research", owner, "", 10))
	_, err := m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{Domain: "research.work", Read: true}})
	require.NoError(t, err)

	body, err := json.Marshal(PipeContactLookupRequest{Name: "owner", Limit: 1})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/pipe/contacts/lookup", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	w := &blockingStatusWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		m.handlePipeContactLookup(w, req)
		close(done)
	}()
	<-w.started

	updated := make(chan error, 1)
	go func() { updated <- ss.UpdateAgentStatus(ctx, owner, "inactive") }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("a slow lookup response retained the contact snapshot lease")
	}
	close(w.release)
	<-done
}

func TestUniqueAgentPrefixesLengthenCollisions(t *testing.T) {
	a := "aaaaaaaa0" + strings.Repeat("0", 55)
	b := "aaaaaaaa1" + strings.Repeat("0", 55)
	require.Len(t, a, 64)
	require.Len(t, b, 64)
	prefixes := uniqueAgentPrefixes([]string{a, b})
	require.Equal(t, 12, len(prefixes[a]))
	require.Equal(t, 12, len(prefixes[b]))
	require.NotEqual(t, prefixes[a], prefixes[b])
	require.Empty(t, uniqueAgentPrefixes([]string{"legacy-agent"})["legacy-agent"])
}

func statusForPeer(t *testing.T, m *Manager, chainID, peerAgentID string, agreement *store.CrossFedRecord) StatusResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: chainID, AgentID: peerAgentID, Agreement: agreement,
	}))
	rec := httptest.NewRecorder()
	m.handleStatus(rec, req)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	var status StatusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&status))
	return status
}
