package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	circuitrelay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	sagep2p "github.com/l33tdawg/sage/internal/p2p"
	"github.com/l33tdawg/sage/internal/store"
)

type restartablePipeServer struct {
	address string
	server  *http.Server
	done    chan error
}

func startRestartablePipeServer(t *testing.T, c *testChain, address string) *restartablePipeServer {
	t.Helper()
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	tlsConfig, err := c.mgr.ServerTLSConfig()
	require.NoError(t, err)
	server := &http.Server{Handler: c.mgr.Router(), TLSConfig: tlsConfig}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(listener, "", "") }()
	return &restartablePipeServer{address: listener.Addr().String(), server: server, done: done}
}

func (s *restartablePipeServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.server == nil {
		return
	}
	require.NoError(t, s.server.Close())
	select {
	case err := <-s.done:
		require.True(t, errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed), "server stop: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("federation server did not stop")
	}
	s.server = nil
}

func restartPipeManager(c *testChain) {
	c.mgr = NewManager(Config{
		LocalChainID: c.chainID,
		CertsDir:     c.certsDir,
		AgentKey:     c.agentKey,
		Badger:       c.badger,
		MemStore:     c.mem,
		Logger:       c.mgr.logger,
	})
}

func pipeSQLite(t *testing.T, c *testChain) *store.SQLiteStore {
	t.Helper()
	ss, ok := c.mem.(*store.SQLiteStore)
	require.True(t, ok)
	return ss
}

func activatePipePeer(t *testing.T, local, remote *testChain, role string) {
	t.Helper()
	agreement, err := local.mgr.ActiveAgreement(remote.chainID)
	require.NoError(t, err)
	left, right := local.chainID, remote.chainID
	if left > right {
		left, right = right, left
	}
	control := store.SyncControl{
		RemoteChainID: remote.chainID,
		Role:          role,
		PeerAgentID:   hex.EncodeToString(remote.agentPub),
		PolicyEpoch:   "pipe-e2e-" + left + "-" + right,
		RemoteCAPin:   hex.EncodeToString(agreement.PeerPubKey),
	}
	if role == "guest" {
		control.ControllerChainID = remote.chainID
		control.ControllerAgentID = control.PeerAgentID
	} else {
		control.ControllerChainID = local.chainID
		control.ControllerAgentID = hex.EncodeToString(local.agentPub)
	}
	ss := pipeSQLite(t, local)
	require.NoError(t, ss.PrepareSyncControl(context.Background(), control))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), remote.chainID, control.PolicyEpoch))
}

type pipeFaultFixture struct {
	sourceAgent      string
	sourceKey        ed25519.PrivateKey
	targetAgent      string
	targetKey        ed25519.PrivateKey
	unrelatedAgent   string
	unrelatedContact string
	target           *RemotePipeTarget
}

func configurePipeFaultFixture(t *testing.T, source, destination *testChain) pipeFaultFixture {
	t.Helper()
	ctx := context.Background()
	sourcePub, sourceKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetPub, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	unrelatedPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fixture := pipeFaultFixture{
		sourceAgent: hex.EncodeToString(sourcePub), sourceKey: sourceKey,
		targetAgent: hex.EncodeToString(targetPub), targetKey: targetKey,
		unrelatedAgent: hex.EncodeToString(unrelatedPub),
	}
	require.NoError(t, pipeSQLite(t, source).CreateAgent(ctx, &store.AgentEntry{
		AgentID: fixture.sourceAgent, Name: "source-agent", Status: "active",
	}))
	require.NoError(t, pipeSQLite(t, destination).CreateAgent(ctx, &store.AgentEntry{
		AgentID: fixture.targetAgent, Name: "target-agent", Status: "active",
	}))
	require.NoError(t, pipeSQLite(t, destination).CreateAgent(ctx, &store.AgentEntry{
		AgentID: fixture.unrelatedAgent, Name: "unrelated-agent", Status: "active",
	}))
	require.NoError(t, destination.badger.RegisterDomain("fault-gate", fixture.targetAgent, "", 10))
	require.NoError(t, destination.badger.RegisterDomain("fault-unrelated", fixture.unrelatedAgent, "", 11))
	_, err = destination.mgr.ReplacePeerRBACPolicy(ctx, source.chainID, []store.PeerRBACDomainPermission{
		{Domain: "fault-gate.work", Read: true},
		{Domain: "fault-unrelated.work", Read: true},
	})
	require.NoError(t, err)
	grant, err := destination.mgr.LocalPipeContacts(ctx, source.chainID)
	require.NoError(t, err)
	require.Len(t, grant.Contacts, 2)
	contactID := func(agentID string) string {
		for _, contact := range grant.Contacts {
			if contact.AgentID == agentID {
				return contact.ContactID
			}
		}
		t.Fatalf("contact %s not found", agentID)
		return ""
	}
	_, err = destination.mgr.SetPipeContactAcceptance(ctx, source.chainID, fixture.targetAgent, contactID(fixture.targetAgent), true)
	require.NoError(t, err)
	fixture.unrelatedContact = contactID(fixture.unrelatedAgent)
	_, err = destination.mgr.SetPipeContactAcceptance(ctx, source.chainID, fixture.unrelatedAgent, fixture.unrelatedContact, true)
	require.NoError(t, err)
	fixture.target, err = source.mgr.ResolveRemotePipeTarget(ctx, fixture.targetAgent+"@"+destination.chainID)
	require.NoError(t, err)
	return fixture
}

func enqueueFaultGateSend(t *testing.T, source, destination *testChain, fixture pipeFaultFixture) (*store.PipelineMessage, *store.PipelineTransportOutbox) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	request := signedPipeSendRequest{
		ToAgent: fixture.targetAgent, SourceChainID: source.chainID, DestinationChainID: destination.chainID,
		Intent: "release-gate", Payload: "survive transport restart exactly once", TTLMinutes: 60,
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)
	proof := signedPipeProof(t, fixture.sourceKey, fixture.sourceAgent, http.MethodPost, "/v1/pipe/send", body, now.Unix())
	msg := &store.PipelineMessage{
		PipeID: "pipe-fault-" + source.chainID, FromAgent: fixture.sourceAgent, ToAgent: fixture.targetAgent,
		DestinationChainID: destination.chainID, FederationPolicyEpoch: fixture.target.PolicyEpoch,
		FederationAgreementID: fixture.target.AgreementID, FederationContactID: fixture.target.ContactID,
		FederationContactRevision: fixture.target.ContactRevision, Intent: request.Intent, Payload: request.Payload,
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID(source.chainID, "send", proof), PipeID: msg.PipeID,
		RemoteChainID: destination.chainID, EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: fixture.sourceAgent,
		TargetAgentID: fixture.targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, pipeSQLite(t, source).InsertPipelineWithTransport(context.Background(), msg, outbox))
	return msg, outbox
}

func forcePipeRetryNow(t *testing.T, c *testChain, eventID string) {
	t.Helper()
	require.NoError(t, pipeSQLite(t, c).RecordPipelineTransportFailure(
		context.Background(), eventID, "release-gate immediate retry", time.Now().Add(-time.Second), false,
	))
}

func waitPipePeerStatus(t *testing.T, local, remote *testChain) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := local.mgr.PeerStatus(context.Background(), remote.chainID)
		if err == nil && status != nil && status.ChainID == remote.chainID {
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer %s did not recover after restart: %v", remote.chainID, lastErr)
}

func pendingPipeEvent(t *testing.T, c *testChain, eventID string) (*store.PipelineTransportOutbox, *PipeEvent) {
	t.Helper()
	outbox, err := pipeSQLite(t, c).GetPipelineTransport(context.Background(), eventID)
	require.NoError(t, err)
	event, terminal, err := c.mgr.buildPipelineEvent(context.Background(), pipeSQLite(t, c), outbox)
	require.NoError(t, err)
	require.False(t, terminal)
	return outbox, event
}

func exercisePipeDisconnectRestart(t *testing.T, source, destination *testChain, stopSource, restartSource, stopDestination, restartDestination func()) {
	t.Helper()
	ctx := context.Background()
	fixture := configurePipeFaultFixture(t, source, destination)
	sourceMsg, sendOutbox := enqueueFaultGateSend(t, source, destination, fixture)

	// The receiver is unreachable. The durable row must remain pending.
	stopDestination()
	source.mgr.pipelineDrain(ctx, pipeSQLite(t, source))
	failedSend, err := pipeSQLite(t, source).GetPipelineTransport(ctx, sendOutbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", failedSend.State)
	require.NotEmpty(t, failedSend.LastError)

	// An unrelated visible agent changing its reversible acceptance while this
	// exact-target request is offline must not poison the queued X authorization.
	_, err = destination.mgr.SetPipeContactAcceptance(ctx, source.chainID,
		fixture.unrelatedAgent, fixture.unrelatedContact, false)
	require.NoError(t, err)

	// The receiver returns and commits the send, but the sender crashes before
	// recording the acknowledgement. Restart the receiver once more: the retry
	// must be a durable duplicate, not a second local inbox row.
	restartDestination()
	forcePipeRetryNow(t, source, sendOutbox.EventID)
	_, sendEvent := pendingPipeEvent(t, source, sendOutbox.EventID)
	accepted, err := source.mgr.PushPipeEvent(ctx, destination.chainID, sendEvent)
	require.NoError(t, err)
	require.Equal(t, "accepted", accepted.Status)
	stopDestination()
	restartDestination()
	source.mgr.pipelineDrain(ctx, pipeSQLite(t, source))
	deliveredSend, err := pipeSQLite(t, source).GetPipelineTransport(ctx, sendOutbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "delivered", deliveredSend.State)
	imports, err := pipeSQLite(t, destination).ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, imports, 1)
	imported := imports[0]
	require.Equal(t, sourceMsg.Payload, imported.Payload)
	require.Equal(t, sendOutbox.EventID, imported.SourcePipeID)
	require.NoError(t, destination.mgr.WithAuthorizedImportedPipe(ctx, imported, func() error {
		return pipeSQLite(t, destination).ClaimPipeline(ctx, imported.PipeID, fixture.targetAgent)
	}))

	// Return the result through the same offline -> commit -> lost-ack ->
	// receiver-restart sequence in the opposite direction.
	now := time.Now().UTC().Truncate(time.Second)
	result := "completed exactly once after restart"
	resultBody, err := json.Marshal(signedPipeResultRequest{
		Result: result, SourcePipeID: imported.SourcePipeID, SourceChainID: destination.chainID,
	})
	require.NoError(t, err)
	resultProof := signedPipeProof(t, fixture.targetKey, fixture.targetAgent, http.MethodPut,
		"/v1/pipe/"+imported.PipeID+"/result", resultBody, now.Unix())
	resultOutbox := &store.PipelineTransportOutbox{
		EventID: PipelineProofEventID(destination.chainID, "result", resultProof), PipeID: imported.PipeID,
		RemoteChainID: source.chainID, EventKind: "result", PolicyEpoch: imported.FederationPolicyEpoch,
		AgreementID: imported.FederationAgreementID, ContactID: imported.FederationContactID,
		ContactRevision: imported.FederationContactRevision, SourceAgentID: fixture.targetAgent,
		TargetAgentID: fixture.sourceAgent, Proof: resultProof, CreatedAt: now,
		ExpiresAt: now.Add(pipeEventResultLifetime),
	}
	require.NoError(t, pipeSQLite(t, destination).CompleteFederatedPipelineWithTransport(
		ctx, imported.PipeID, fixture.targetAgent, result, resultOutbox,
	))
	stopSource()
	destination.mgr.pipelineDrain(ctx, pipeSQLite(t, destination))
	failedResult, err := pipeSQLite(t, destination).GetPipelineTransport(ctx, resultOutbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", failedResult.State)
	require.NotEmpty(t, failedResult.LastError)
	restartSource()
	forcePipeRetryNow(t, destination, resultOutbox.EventID)
	_, resultEvent := pendingPipeEvent(t, destination, resultOutbox.EventID)
	require.NoError(t, destination.mgr.preflightPipelineResultPeer(ctx, imported, resultOutbox))
	resultEvent.Result = result
	accepted, err = destination.mgr.PushPipeEvent(ctx, source.chainID, resultEvent)
	require.NoError(t, err)
	require.Equal(t, "accepted", accepted.Status)
	stopSource()
	restartSource()
	destination.mgr.pipelineDrain(ctx, pipeSQLite(t, destination))
	deliveredResult, err := pipeSQLite(t, destination).GetPipelineTransport(ctx, resultOutbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "delivered", deliveredResult.State)
	completed, err := pipeSQLite(t, source).GetPipeline(ctx, sourceMsg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "completed", completed.Status)
	require.Equal(t, result, completed.Result)
	origins, err := pipeSQLite(t, source).ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, origins, 1)
}

func TestFederatedPipelineDirectMTLSDisconnectRestartExactlyOnce(t *testing.T) {
	source := newTestChain(t, "pipe-direct-source")
	destination := newTestChain(t, "pipe-direct-destination")
	sourceServer := startRestartablePipeServer(t, source, "")
	destinationServer := startRestartablePipeServer(t, destination, "")
	defer func() { sourceServer.stop(t); destinationServer.stop(t) }()
	federate(t, source, destination, "https://"+destinationServer.address, nil, 4, 0)
	federate(t, destination, source, "https://"+sourceServer.address, nil, 4, 0)
	activatePipePeer(t, source, destination, "host")
	activatePipePeer(t, destination, source, "guest")

	stopSource := func() { sourceServer.stop(t) }
	restartSource := func() {
		restartPipeManager(source)
		sourceServer = startRestartablePipeServer(t, source, sourceServer.address)
	}
	stopDestination := func() { destinationServer.stop(t) }
	restartDestination := func() {
		restartPipeManager(destination)
		destinationServer = startRestartablePipeServer(t, destination, destinationServer.address)
	}
	exercisePipeDisconnectRestart(t, source, destination, stopSource, restartSource, stopDestination, restartDestination)
}

func TestFederatedPipelineExactTargetQueuesFromAuthenticatedCacheWhileOffline(t *testing.T) {
	source := newTestChain(t, "pipe-cache-source")
	destination := newTestChain(t, "pipe-cache-destination")
	sourceServer := startRestartablePipeServer(t, source, "")
	destinationServer := startRestartablePipeServer(t, destination, "")
	defer func() { sourceServer.stop(t); destinationServer.stop(t) }()
	federate(t, source, destination, "https://"+destinationServer.address, nil, 4, 0)
	federate(t, destination, source, "https://"+sourceServer.address, nil, 4, 0)
	activatePipePeer(t, source, destination, "host")
	activatePipePeer(t, destination, source, "guest")
	fixture := configurePipeFaultFixture(t, source, destination)
	// Targeted lookup is live-only. A separate authenticated legacy status
	// snapshot provides the exact-address offline routing hint.
	_, err := source.mgr.PeerStatus(context.Background(), destination.chainID)
	require.NoError(t, err)
	// A live exact miss must not overwrite the known recipient's encrypted
	// offline hint for this peer.
	_, err = source.mgr.ResolveRemotePipeTarget(context.Background(), strings.Repeat("f", 64)+"@"+destination.chainID)
	require.ErrorIs(t, err, ErrRemotePipeTargetNotFound)

	// The online authenticated resolution above populated the encrypted cache.
	// Stop the peer and rebuild this manager to prove the hint survives process
	// restart rather than existing only in memory.
	destinationServer.stop(t)
	restartPipeManager(source)
	exact := fixture.targetAgent + "@" + destination.chainID
	cached, err := source.mgr.ResolveRemotePipeTarget(context.Background(), exact)
	require.NoError(t, err)
	require.Equal(t, fixture.target.AgentID, cached.AgentID)
	require.Equal(t, fixture.target.ContactRevision, cached.ContactRevision)

	_, err = source.mgr.ResolveRemotePipeTarget(context.Background(), fixture.target.Handle)
	require.ErrorIs(t, err, ErrRemotePipeResolutionIncomplete,
		"friendly labels need a complete live peer scan and must never route from stale cache")
	_, err = source.mgr.resolveRemotePipeTargetLive(context.Background(), exact)
	require.ErrorIs(t, err, ErrRemotePipeResolutionIncomplete,
		"the outbox must require fresh status before attaching payload bytes")
}

type relayPipeServer struct {
	transport *sagep2p.Transport
	server    *http.Server
	done      chan error
	target    string
}

func (s *relayPipeServer) stop(t *testing.T) {
	t.Helper()
	if s == nil || s.transport == nil {
		return
	}
	require.NoError(t, s.server.Close())
	select {
	case err := <-s.done:
		require.True(t, errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) || errors.Is(err, network.ErrReset), "relay server stop: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("relay federation server did not stop")
	}
	require.NoError(t, s.transport.Close())
	s.transport = nil
}

func startRelayPipeServer(t *testing.T, ctx context.Context, c *testChain, identityPath string, relayBootstrap []string, relayAddr ma.Multiaddr) *relayPipeServer {
	t.Helper()
	transport, err := sagep2p.New(ctx, sagep2p.Config{
		IdentityKeyPath: identityPath, ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
		RelayAddrs: relayBootstrap, AcceptInbound: true, EnforcePeerAllowlist: true, ForcePrivate: true,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, addr := range transport.Addrs() {
			if strings.Contains(addr, "/p2p-circuit/") {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "relay reservation did not become ready")
	peerPart := ma.StringCast("/p2p/" + transport.Host().ID().String())
	target := relayAddr.Encapsulate(ma.StringCast("/p2p-circuit")).Encapsulate(peerPart).String()
	tlsConfig, err := c.mgr.ServerTLSConfig()
	require.NoError(t, err)
	server := &http.Server{Handler: c.mgr.Router(), TLSConfig: tlsConfig}
	done := make(chan error, 1)
	go func() { done <- server.ServeTLS(transport.Listener(), "", "") }()
	return &relayPipeServer{transport: transport, server: server, done: done, target: target}
}

func TestFederatedPipelineCircuitRelayDisconnectRestartExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	relayHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			out := append([]ma.Multiaddr(nil), addrs...)
			for _, addr := range addrs {
				if raw := addr.String(); strings.HasPrefix(raw, "/ip4/127.0.0.1/") {
					out = append(out, ma.StringCast("/dns/libp2p.internal"+strings.TrimPrefix(raw, "/ip4/127.0.0.1")))
				}
			}
			return out
		}),
	)
	require.NoError(t, err)
	defer relayHost.Close()
	resources := circuitrelay.DefaultResources()
	resources.Limit = &circuitrelay.RelayLimit{Duration: 2 * time.Minute, Data: 8 << 20}
	relayService, err := circuitrelay.New(relayHost, circuitrelay.WithResources(resources),
		circuitrelay.WithReservationAddressFilter(func(ma.Multiaddr) bool { return true }))
	require.NoError(t, err)
	defer relayService.Close()
	relayPeer := ma.StringCast("/p2p/" + relayHost.ID().String())
	var relayAddr ma.Multiaddr
	relayBootstrap := make([]string, 0, len(relayHost.Addrs()))
	for _, addr := range relayHost.Addrs() {
		relayBootstrap = append(relayBootstrap, addr.Encapsulate(relayPeer).String())
		if relayAddr == nil && strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/") {
			relayAddr = addr.Encapsulate(relayPeer)
		}
	}
	require.NotNil(t, relayAddr)

	source := newTestChain(t, "pipe-relay-source")
	destination := newTestChain(t, "pipe-relay-destination")
	federate(t, source, destination, "https://127.0.0.1:1", nil, 4, 0)
	federate(t, destination, source, "https://127.0.0.1:1", nil, 4, 0)
	activatePipePeer(t, source, destination, "host")
	activatePipePeer(t, destination, source, "guest")
	sourceIdentity := filepath.Join(t.TempDir(), "source-p2p.key")
	destinationIdentity := filepath.Join(t.TempDir(), "destination-p2p.key")
	sourceRelay := startRelayPipeServer(t, ctx, source, sourceIdentity, relayBootstrap, relayAddr)
	destinationRelay := startRelayPipeServer(t, ctx, destination, destinationIdentity, relayBootstrap, relayAddr)
	defer func() { sourceRelay.stop(t); destinationRelay.stop(t) }()

	wire := func() {
		require.NoError(t, sourceRelay.transport.AddAllowedPeer([]string{destinationRelay.target}))
		require.NoError(t, destinationRelay.transport.AddAllowedPeer([]string{sourceRelay.target}))
		source.mgr.SetPeerDialFunc(func(dialCtx context.Context, remoteChainID string) (net.Conn, bool, error) {
			if remoteChainID != destination.chainID {
				return nil, false, nil
			}
			conn, dialErr := sourceRelay.transport.DialContext(dialCtx, destinationRelay.target)
			return conn, true, dialErr
		})
		destination.mgr.SetPeerDialFunc(func(dialCtx context.Context, remoteChainID string) (net.Conn, bool, error) {
			if remoteChainID != source.chainID {
				return nil, false, nil
			}
			conn, dialErr := destinationRelay.transport.DialContext(dialCtx, sourceRelay.target)
			return conn, true, dialErr
		})
	}
	wire()

	stopSource := func() {
		peerID := sourceRelay.transport.Host().ID()
		sourceRelay.stop(t)
		if destinationRelay.transport != nil {
			_ = destinationRelay.transport.Host().Network().ClosePeer(peerID)
		}
	}
	restartSource := func() {
		restartPipeManager(source)
		sourceRelay = startRelayPipeServer(t, ctx, source, sourceIdentity, relayBootstrap, relayAddr)
		wire()
		waitPipePeerStatus(t, destination, source)
	}
	stopDestination := func() {
		peerID := destinationRelay.transport.Host().ID()
		destinationRelay.stop(t)
		if sourceRelay.transport != nil {
			_ = sourceRelay.transport.Host().Network().ClosePeer(peerID)
		}
	}
	restartDestination := func() {
		restartPipeManager(destination)
		destinationRelay = startRelayPipeServer(t, ctx, destination, destinationIdentity, relayBootstrap, relayAddr)
		wire()
		waitPipePeerStatus(t, source, destination)
	}
	exercisePipeDisconnectRestart(t, source, destination, stopSource, restartSource, stopDestination, restartDestination)

	connected := false
	for _, conn := range sourceRelay.transport.Host().Network().ConnsToPeer(destinationRelay.transport.Host().ID()) {
		connected = connected || conn.Stat().Limited
	}
	require.True(t, connected, "pipeline gate did not traverse a limited circuit-relay connection")
}
