package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/orgpolicy"
)

type federationJoinKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newFederationJoinKV() *federationJoinKV {
	return &federationJoinKV{m: make(map[string][]byte)}
}

func (k *federationJoinKV) GetState(key string) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	raw := k.m[key]
	if raw == nil {
		return nil, nil
	}
	return append([]byte(nil), raw...), nil
}

func (k *federationJoinKV) SetStatesAtomic(writes []orgpolicy.KVWrite) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.setLocked(writes)
	return nil
}

func (k *federationJoinKV) setLocked(writes []orgpolicy.KVWrite) {
	for _, write := range writes {
		k.m[write.Key] = append([]byte(nil), write.Value...)
	}
}

func (k *federationJoinKV) CompareAndSetStatesAtomic(conditions []orgpolicy.KVCondition, writes []orgpolicy.KVWrite) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, condition := range conditions {
		current, ok := k.m[condition.Key]
		if condition.Value == nil {
			if ok {
				return false, nil
			}
			continue
		}
		if !ok || !bytes.Equal(current, condition.Value) {
			return false, nil
		}
	}
	k.setLocked(writes)
	return true, nil
}

func (k *federationJoinKV) ListKeys(prefix string) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]string, 0)
	for key := range k.m {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func newerJoinPolicy(t *testing.T, orgKey ed25519.PrivateKey, revision uint64, allows ...string) *orgpolicy.Policy {
	t.Helper()
	now := time.Now().Unix()
	policy := &orgpolicy.Policy{
		Schema: orgpolicy.SchemaIDV2, OrgID: "org-acme", Scope: "fleet",
		Revision: revision, KeyEpoch: "epoch-1",
		IssuedAt: now - 60, NotBefore: now - 60, Expiry: now + 3600,
		Payload: orgpolicy.Payload{
			Judge: orgpolicy.Judge{
				ModelTag:   "sage-memory-judge:v16",
				GGUFSHA256: strings.Repeat("ab", 32),
				BlobSHA256: strings.Repeat("cd", 32),
			},
			ChecksVersion: "sage-lasting/1+supported/1",
			PeerAllowList: append([]string(nil), allows...),
		},
	}
	require.NoError(t, policy.Sign(orgKey))
	return policy
}

func seedManagedJoinOrg(t *testing.T, node *ceremonyNode, allows ...string) (*orgpolicy.Store, ed25519.PrivateKey) {
	t.Helper()
	_, orgKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	policy := newerJoinPolicy(t, orgKey, 1, allows...)
	store := orgpolicy.NewStore(newFederationJoinKV())
	outcome := orgpolicy.NewEnrollmentOutcome(
		"token-"+node.mgr.localChainID,
		hex.EncodeToString(node.mgr.agentPub),
		"owner/node/member",
		policy,
		time.Now().Unix(),
	)
	require.NoError(t, store.CommitFirstEnrollment(outcome))
	return store, orgKey
}

func installJoinGate(mgr *Manager, store *orgpolicy.Store, rootKey ed25519.PrivateKey) {
	gate := orgpolicy.JoinGate{
		Store: store,
		Now:   func() int64 { return time.Now().Unix() },
	}
	if rootKey != nil {
		rootPub := rootKey.Public().(ed25519.PublicKey)
		rootID := hex.EncodeToString(rootPub)
		gate.CurrentRoot = func() (orgpolicy.CurrentRoot, error) {
			return orgpolicy.CurrentRoot{CredentialID: rootID, PublicKey: rootPub}, nil
		}
	}
	mgr.SetJoinPeerEvaluator(func(_ context.Context, check JoinPeerCheck) (JoinPeerDecision, error) {
		decision, err := gate.Evaluate(orgpolicy.JoinGateCheck{
			PeerAgentPub: check.PeerAgentPub,
			PeerChainID:  check.PeerChainID,
			SessionID:    check.SessionID,
			Role:         check.Role,
			Phase:        check.Phase,
		})
		if err != nil {
			return JoinPeerDecision{}, err
		}
		return JoinPeerDecision{
			Managed:          decision.Managed,
			Allowed:          decision.Allowed,
			Reason:           decision.Reason,
			OrgID:            decision.OrgID,
			OverrideRevision: decision.OverrideRevision,
		}, nil
	})
}

func startJoinHost(t *testing.T, host *ceremonyNode) *httptest.Server {
	t.Helper()
	hostTLS, err := host.mgr.ServerTLSConfig()
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestRFC443FleetJoinEligibilityRefusedOnHostAndGuest(t *testing.T) {
	t.Run("host refuses ineligible guest", func(t *testing.T) {
		host := newCeremonyNode(t, "host-aaaaaa")
		guest := newCeremonyNode(t, "guest-bbbbbb")
		hostStore, _ := seedManagedJoinOrg(t, host)
		guestStore, _ := seedManagedJoinOrg(t, guest, hex.EncodeToString(host.mgr.agentPub))
		installJoinGate(host.mgr, hostStore, nil)
		installJoinGate(guest.mgr, guestStore, nil)
		srv := startJoinHost(t, host)

		create, err := host.mgr.HostCreate(srv.URL)
		require.NoError(t, err)
		scan, err := guest.mgr.GuestScan(context.Background(), create.OTPAuthURI, "https://127.0.0.1:8444")
		require.NoError(t, err)
		require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
		_, err = guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in the org policy allow-list")
		require.Zero(t, host.count())
		require.Zero(t, guest.count())
	})

	t.Run("guest refuses ineligible host and aborts host request", func(t *testing.T) {
		host := newCeremonyNode(t, "host-cccccc")
		guest := newCeremonyNode(t, "guest-dddddd")
		hostStore, _ := seedManagedJoinOrg(t, host, hex.EncodeToString(guest.mgr.agentPub))
		guestStore, _ := seedManagedJoinOrg(t, guest)
		installJoinGate(host.mgr, hostStore, nil)
		installJoinGate(guest.mgr, guestStore, nil)
		srv := startJoinHost(t, host)

		create, err := host.mgr.HostCreate(srv.URL)
		require.NoError(t, err)
		scan, err := guest.mgr.GuestScan(context.Background(), create.OTPAuthURI, "https://127.0.0.1:8444")
		require.NoError(t, err)
		require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
		_, err = guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in the org policy allow-list")
		view, statusErr := host.mgr.HostSessionStatus(create.SessionID)
		require.NoError(t, statusErr)
		require.Equal(t, JoinAborted, view.State)
		require.Zero(t, host.count())
		require.Zero(t, guest.count())
	})
}

func TestRFC443FleetJoinBothEligibleCompletes(t *testing.T) {
	host := newCeremonyNode(t, "host-mmmmmm")
	guest := newCeremonyNode(t, "guest-nnnnnn")
	hostStore, _ := seedManagedJoinOrg(t, host, hex.EncodeToString(guest.mgr.agentPub))
	guestStore, _ := seedManagedJoinOrg(t, guest, hex.EncodeToString(host.mgr.agentPub))
	installJoinGate(host.mgr, hostStore, nil)
	installJoinGate(guest.mgr, guestStore, nil)

	_, err := rfc443Join(t, host, guest)
	require.NoError(t, err)
	require.Equal(t, 1, host.count())
	require.Equal(t, 1, guest.count())
}

func TestRFC443FleetJoinPolicySwapBetweenRequestAndApproval(t *testing.T) {
	host := newCeremonyNode(t, "host-eeeeee")
	guest := newCeremonyNode(t, "guest-ffffff")
	hostStore, orgKey := seedManagedJoinOrg(t, host, hex.EncodeToString(guest.mgr.agentPub))
	other, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	installJoinGate(host.mgr, hostStore, nil)
	installJoinGate(guest.mgr, orgpolicy.NewStore(newFederationJoinKV()), nil)
	srv := startJoinHost(t, host)

	create, err := host.mgr.HostCreate(srv.URL)
	require.NoError(t, err)
	scan, err := guest.mgr.GuestScan(context.Background(), create.OTPAuthURI, "https://127.0.0.1:8444")
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
	request, err := guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
	require.NoError(t, err)

	next := newerJoinPolicy(t, orgKey, 2, hex.EncodeToString(other))
	_, decision, _, err := hostStore.ApplyPolicy(next, orgKey.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	require.Equal(t, orgpolicy.DecisionApplied, decision)
	err = host.mgr.HostApprove(create.SessionID, request.CodeG, trustOnlyJoinScope)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the org policy allow-list")
	require.Zero(t, host.count())
	require.Zero(t, guest.count())
}

func TestRFC443FleetJoinGuestPolicySwapRechecksUnderAgreementLease(t *testing.T) {
	host := newCeremonyNode(t, "host-gggggg")
	guest := newCeremonyNode(t, "guest-hhhhhh")
	guestStore, guestOrgKey := seedManagedJoinOrg(t, guest, hex.EncodeToString(host.mgr.agentPub))
	installJoinGate(host.mgr, orgpolicy.NewStore(newFederationJoinKV()), nil)
	installJoinGate(guest.mgr, guestStore, nil)
	srv := startJoinHost(t, host)

	create, err := host.mgr.HostCreate(srv.URL)
	require.NoError(t, err)
	scan, err := guest.mgr.GuestScan(context.Background(), create.OTPAuthURI, "https://127.0.0.1:8444")
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
	request, err := guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostApprove(create.SessionID, request.CodeG, trustOnlyJoinScope))

	next := newerJoinPolicy(t, guestOrgKey, 2)
	_, decision, _, err := guestStore.ApplyPolicy(next, guestOrgKey.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	require.Equal(t, orgpolicy.DecisionApplied, decision)
	_, err = guest.mgr.GuestConfirm(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the org policy allow-list")
	require.Zero(t, guest.count(), "guest tx-33 must not run after the lease re-check")
	require.Zero(t, host.count())
}

func TestRFC443FleetJoinHostPolicySwapRechecksBeforeHostTx33(t *testing.T) {
	host := newCeremonyNode(t, "host-kkkkkk")
	guest := newCeremonyNode(t, "guest-llllll")
	hostStore, hostOrgKey := seedManagedJoinOrg(t, host, hex.EncodeToString(guest.mgr.agentPub))
	installJoinGate(host.mgr, hostStore, nil)
	installJoinGate(guest.mgr, orgpolicy.NewStore(newFederationJoinKV()), nil)
	srv := startJoinHost(t, host)

	create, err := host.mgr.HostCreate(srv.URL)
	require.NoError(t, err)
	scan, err := guest.mgr.GuestScan(context.Background(), create.OTPAuthURI, "https://127.0.0.1:8444")
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
	request, err := guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostApprove(create.SessionID, request.CodeG, trustOnlyJoinScope))

	next := newerJoinPolicy(t, hostOrgKey, 2)
	_, decision, _, err := hostStore.ApplyPolicy(next, hostOrgKey.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	require.Equal(t, orgpolicy.DecisionApplied, decision)
	_, err = guest.mgr.GuestConfirm(context.Background(), create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not in the org policy allow-list")
	require.Zero(t, host.count(), "host tx-33 must not run after the lease re-check")
	require.Equal(t, 1, guest.count(), "guest-first tx-33 is the existing documented one-sided window")
}

func TestRFC443FleetJoinCurrentRootOverrideCompletesAndRecordsUsage(t *testing.T) {
	host := newCeremonyNode(t, "host-iiiiii")
	guest := newCeremonyNode(t, "guest-jjjjjj")
	hostStore, _ := seedManagedJoinOrg(t, host)
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x70}, ed25519.SeedSize))
	rootPub := rootKey.Public().(ed25519.PublicKey)
	now := time.Now().Unix()
	peerAgent := hex.EncodeToString(guest.mgr.agentPub)
	override := orgpolicy.JoinOverride{
		OrgID: "org-acme", PeerAgentPub: peerAgent, PeerChainID: guest.mgr.localChainID,
		Reason: "owner-approved replacement node", IssuedAt: now - 60,
		ExpiresAt: now + 3600, Revision: 1,
	}
	require.NoError(t, override.Sign(rootKey))
	_, decision, err := hostStore.ApplyJoinOverride(override, rootPub)
	require.NoError(t, err)
	require.Equal(t, orgpolicy.DecisionApplied, decision)
	installJoinGate(host.mgr, hostStore, rootKey)
	installJoinGate(guest.mgr, orgpolicy.NewStore(newFederationJoinKV()), nil)

	_, err = rfc443Join(t, host, guest)
	require.NoError(t, err)
	usage, err := hostStore.LoadJoinOverrideUsage("org-acme", peerAgent, guest.mgr.localChainID)
	require.NoError(t, err)
	require.Len(t, usage, 3)
	require.Equal(t, []string{"request", "approval", "commit"}, []string{usage[0].Phase, usage[1].Phase, usage[2].Phase})
	require.Equal(t, 1, host.count())
	require.Equal(t, 1, guest.count())
}
