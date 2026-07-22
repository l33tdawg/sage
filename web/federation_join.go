package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/netguard"
	"github.com/l33tdawg/sage/internal/store"
)

// v11 real-TOTP federation JOIN ceremony - the dashboard (cookie-authed) proxy
// for the guided guest/host wizards. The browser holds a dashboard session, not
// the node operator's signing key, so it cannot call the agent-signed REST
// endpoints; these routes run behind the dashboard auth middleware and drive the
// federation Manager directly. Everything is OFF-consensus (the only chain
// writes are the two operators' own tx-33/tx-34, fired inside the Manager after
// each human confirmation).

// FederationJoinDriver is the slice of the federation Manager the dashboard
// consumes. An interface so this package need not depend on the concrete type
// beyond its method set.
type FederationJoinDriver interface {
	HostCreate(hostEndpoint string) (*federation.HostCreateResult, error)
	HostScanReturn(sessionID, returnURI string) error
	HostSessionStatus(sessionID string) (*federation.HostSessionView, error)
	HostApprove(sessionID, typedCode string, grant federation.ScopeWire) error
	HostAbort(sessionID string) error
	GuestScan(ctx context.Context, uri, guestEndpoint string) (*federation.GuestScanResult, error)
	GuestRequest(ctx context.Context, sessionID, guestEndpoint string, scope federation.ScopeWire) (*federation.GuestRequestResult, error)
	GuestPollStatus(ctx context.Context, sessionID string) (*federation.JoinStatusResp, error)
	GuestConfirm(ctx context.Context, sessionID, guestEndpoint string, hostScope federation.ScopeWire) (string, error)
	RevokeAgreement(remoteChainID string) (string, error)
	PeerStatus(ctx context.Context, remoteChainID string) (*federation.StatusResponse, error)
	LocalChainID() string
	NetworkName() string
	SetNetworkName(name string)
	SyncReconcileInfo(remoteChainID string) (federation.SyncReconcileStatus, bool)
}

// SetFederation wires the JOIN ceremony driver (call before RegisterRoutes).
func (h *DashboardHandler) SetFederation(f FederationJoinDriver) { h.Federation = f }

const (
	fedCallTimeout = 25 * time.Second
)

func fedWriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fedWriteErr(w http.ResponseWriter, status int, msg string) {
	fedWriteJSON(w, status, map[string]string{"error": msg})
}

// fedReady guards every route: 501 when the transport is not wired.
func (h *DashboardHandler) fedReady(w http.ResponseWriter) bool {
	if h.Federation == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Federation is not enabled on this node.")
		return false
	}
	return true
}

// registerFederationRoutes mounts the JOIN proxy inside the dashboard's
// authenticated group (called from RegisterRoutes).
func (h *DashboardHandler) registerFederationRoutes(r chi.Router) {
	// Dashboard authentication deliberately accepts independently signed local
	// agents for ordinary memory work. Federation is node-wide trust and policy,
	// so put the entire surface behind the stricter operator boundary: a local
	// CEREBRUM session or this node's exact operator signature. This also keeps
	// ceremony state and peer grants out of unrelated agents' reach.
	fr := r.With(h.federationOperatorGate)
	fr.Get("/v1/dashboard/federation/network-name", h.handleGetNetworkName)
	fr.Put("/v1/dashboard/federation/network-name", h.handleSetNetworkName)
	fr.Get("/v1/dashboard/federation/shareable-domains", h.handleFedShareableDomains)
	fr.Get("/v1/dashboard/federation/connections", h.handleFedConnections)
	fr.Get("/v1/dashboard/federation/connections/{chain_id}/permissions", h.handleFedPermissionsGet)
	fr.Put("/v1/dashboard/federation/connections/{chain_id}/permissions", h.handleFedPermissionsPut)
	fr.Put("/v1/dashboard/federation/connections/{chain_id}/pause", h.handleFedPause)
	fr.Get("/v1/dashboard/federation/connections/{chain_id}/pipe-contacts", h.handleFedPipeContactsGet)
	fr.Put("/v1/dashboard/federation/connections/{chain_id}/pipe-contacts", h.handleFedPipeContactsPut)
	fr.Post("/v1/dashboard/federation/connections/{chain_id}/revoke", h.handleFedRevoke)
	fr.Get("/v1/dashboard/federation/connections/{chain_id}/status", h.handleFedPeerStatus)

	fr.Get("/v1/dashboard/federation/lan-endpoint", h.handleFedLanEndpoint)
	fr.Get("/v1/dashboard/federation/readiness", h.handleFedReadiness)

	// v11.6 host-controlled domain sync + status (operator-only surface, but the
	// dashboard IS the operator here — cookie-authed local control plane).
	fr.Get("/v1/dashboard/federation/connections/{chain_id}/sync", h.handleFedSyncGet)
	fr.Put("/v1/dashboard/federation/connections/{chain_id}/sync", h.handleFedSyncSet)
	fr.Get("/v1/dashboard/federation/connections/{chain_id}/sync/status", h.handleFedSyncStatus)
	fr.Post("/v1/dashboard/federation/connections/{chain_id}/sync/resend", h.handleFedSyncResend)

	fr.Post("/v1/dashboard/federation/join/host/create", h.handleFedHostCreate)
	fr.Post("/v1/dashboard/federation/join/host/scan-return", h.handleFedHostScanReturn)
	fr.Get("/v1/dashboard/federation/join/host/{session_id}", h.handleFedHostStatus)
	fr.Post("/v1/dashboard/federation/join/host/{session_id}/approve", h.handleFedHostApprove)
	fr.Post("/v1/dashboard/federation/join/host/{session_id}/abort", h.handleFedHostAbort)

	fr.Post("/v1/dashboard/federation/join/guest/scan", h.handleFedGuestScan)
	fr.Post("/v1/dashboard/federation/join/guest/request", h.handleFedGuestRequest)
	fr.Get("/v1/dashboard/federation/join/guest/{session_id}/status", h.handleFedGuestStatus)
	fr.Post("/v1/dashboard/federation/join/guest/{session_id}/abort", h.handleFedGuestAbort)
	fr.Post("/v1/dashboard/federation/join/guest/confirm", h.handleFedGuestConfirm)

	// v11.8 sync-group management (INT1): the local operator's authoring surface
	// over the group-journal EMIT layer. Split by the §8 authorization model —
	// owner-unilateral (domain add/remove on MY scope, MY own role) and
	// controller-affecting (roster control). Everything is OFF-consensus and
	// locally authored; the emit self-check refuses an entry whose resolver-pinned
	// key is not this node's, so a caller can never author for another owner.
	fr.Get("/v1/dashboard/federation/groups", h.handleFedGroupList)
	fr.Post("/v1/dashboard/federation/groups", h.handleFedGroupCreate)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/domains", h.handleFedGroupDomainAdd)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/domains/remove", h.handleFedGroupDomainRemove)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/self-role", h.handleFedGroupSelfRole)
	fr.Put("/v1/dashboard/federation/groups/{group_id}/name", h.handleFedGroupRename)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/roster", h.handleFedGroupRosterControl)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/members/invite", h.handleFedGroupMemberInvite)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/epoch-rotate", h.handleFedGroupEpochRotate)
	fr.Post("/v1/dashboard/federation/groups/{group_id}/dissolve", h.handleFedGroupDissolve)
}

// --- LAN endpoint suggestion (fix the localhost-in-join-code footgun) -------

// fedDefaultPort is the standard federation listener port (node.go default
// 0.0.0.0:8444). The dashboard normally receives the effective configured
// listener address from node.go; this constant is only the safe legacy/test
// fallback when the handler is embedded without that wiring.
const fedDefaultPort = 8444

func (h *DashboardHandler) federationListenerBind() (string, int) {
	host, port, ok := parseMCPTLSAddr(h.FederationAddr)
	if !ok || port < 1 || port > 65535 {
		return "", fedDefaultPort
	}
	return host, port
}

// FedLanCandidate is one address a JOINING peer on another machine could use to
// reach this node's federation listener.
type FedLanCandidate struct {
	Endpoint  string `json:"endpoint"`
	IP        string `json:"ip"`
	Iface     string `json:"iface"`
	IsPrivate bool   `json:"is_private"`
}

// handleFedLanEndpoint suggests the federation endpoint to advertise in a join
// code. The browser only knows location.hostname — usually "localhost" when the
// dashboard is opened on the same machine — which a DIFFERENT laptop can never
// route to (that was the v11.4.0 "dial tcp 127.0.0.1:8444: connection refused"
// on the guest: the host had baked localhost into its code). So we enumerate
// this host's routable LAN addresses server-side (physical-LAN-private first)
// and let the wizard default to the most-likely-reachable one.
func (h *DashboardHandler) handleFedLanEndpoint(w http.ResponseWriter, r *http.Request) {
	bindHost, port := h.federationListenerBind()
	var out []FedLanCandidate
	wildcardBind := bindHost == "" || bindHost == "0.0.0.0" || bindHost == "::"
	if !wildcardBind {
		// An explicit bind accepts traffic only for that exact host. Advertising
		// another interface would mint a signed but unreachable JOIN endpoint. The
		// LAN ceremony deliberately excludes public/CGNAT/overlay endpoints; those
		// use the Internet/libp2p path instead.
		ip := net.ParseIP(bindHost)
		if netguard.LocalLANHost(bindHost) {
			out = append(out, FedLanCandidate{
				Endpoint:  "https://" + net.JoinHostPort(bindHost, strconv.Itoa(port)),
				IP:        bindHost,
				Iface:     "configured listener",
				IsPrivate: ip != nil && ip.IsPrivate(),
			})
		}
	} else {
		cands := directIPv4Candidates()
		out = make([]FedLanCandidate, 0, len(cands))
		for _, c := range cands {
			if !netguard.LocalLANHost(c.IP) {
				continue
			}
			out = append(out, FedLanCandidate{
				Endpoint:  fmt.Sprintf("https://%s:%d", c.IP, port),
				IP:        c.IP,
				Iface:     c.Iface,
				IsPrivate: c.IsPrivate,
			})
		}
		// A wildcard listener may also be reachable through the exact private
		// address used to open CEREBRUM even when interface enumeration is empty
		// (for example inside a constrained container). Resolve that fallback on
		// the server, where the listener mode and LAN guard are both known. The
		// browser must never fabricate it for an explicit public/overlay bind.
		if len(out) == 0 {
			if candidate, ok := privateFederationDashboardCandidate(r, port); ok {
				out = append(out, candidate)
			}
		}
	}
	suggested := ""
	if len(out) > 0 {
		suggested = out[0].Endpoint // ranked: physical-LAN-private wins
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"port":               port,
		"suggested_endpoint": suggested,
		"candidates":         out,
	})
}

func privateFederationDashboardCandidate(r *http.Request, port int) (FedLanCandidate, bool) {
	if r == nil {
		return FedLanCandidate{}, false
	}
	requestHost := strings.TrimSpace(r.Host)
	if host, _, splitErr := net.SplitHostPort(requestHost); splitErr == nil {
		requestHost = host
	} else {
		requestHost = strings.TrimPrefix(strings.TrimSuffix(requestHost, "]"), "[")
	}
	ip := net.ParseIP(requestHost)
	if ip == nil || !ip.IsPrivate() || !netguard.LocalLANHost(requestHost) {
		return FedLanCandidate{}, false
	}
	return FedLanCandidate{
		Endpoint:  "https://" + net.JoinHostPort(requestHost, strconv.Itoa(port)),
		IP:        requestHost,
		Iface:     "CEREBRUM address",
		IsPrivate: true,
	}, true
}

// --- Federation readiness (fork-ladder warm-up) -----------------------------

// federationMinAppVersion is the app version at which cross_fed agreements
// (tx-33 CrossFedSet / tx-34 CrossFedRevoke) become valid — app-v15. A freshly
// minted chain starts at app_version 1 and the auto-advance watchdog walks the
// fork ladder up to the binary ceiling (~200 blocks per fork), so a brand-new
// node cannot complete a JOIN (the final tx-33 broadcast is rejected as
// "unknown tx type") until it reaches this version. The Federation page shows a
// warm-up countdown until then.
const federationMinAppVersion = 15

// forkDelayBlocks is the hard per-fork activation delay floor
// (defaultUpgradeDelayBlocks in the ABCI app). Used only to ESTIMATE the
// remaining warm-up — the real timing also depends on block rate, which the
// frontend measures live.
const forkDelayBlocks = 200

// handleFedReadiness reports whether federation agreements can be created yet
// (app_version >= federationMinAppVersion) plus the current app version and
// block height, so the Federation page can render a warm-up estimate.
func (h *DashboardHandler) handleFedReadiness(w http.ResponseWriter, r *http.Request) {
	cometRPC := h.CometBFTRPC
	if cometRPC == "" {
		cometRPC = "http://127.0.0.1:26657"
	}
	appVersion, height := 0, 0
	client := &http.Client{Timeout: 4 * time.Second}
	// LIVE app version comes from /abci_info: /status's node_info.protocol_version
	// is frozen at the last handshake/restart and does NOT advance as forks
	// activate, so it would report a stale (lower) version and keep the warm-up
	// panel up forever after a restart.
	if req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, cometRPC+"/abci_info", nil); req != nil {
		if resp, err := client.Do(req); err == nil {
			defer func() { _ = resp.Body.Close() }()
			var ai struct {
				Result struct {
					Response struct {
						AppVersion string `json:"app_version"`
					} `json:"response"`
				} `json:"result"`
			}
			if json.NewDecoder(resp.Body).Decode(&ai) == nil {
				appVersion, _ = strconv.Atoi(ai.Result.Response.AppVersion)
			}
		}
	}
	// Block height (live) from /status.
	if req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, cometRPC+"/status", nil); req != nil {
		if resp, err := client.Do(req); err == nil {
			defer func() { _ = resp.Body.Close() }()
			var st struct {
				Result struct {
					SyncInfo struct {
						LatestBlockHeight string `json:"latest_block_height"`
					} `json:"sync_info"`
				} `json:"result"`
			}
			if json.NewDecoder(resp.Body).Decode(&st) == nil {
				height, _ = strconv.Atoi(st.Result.SyncInfo.LatestBlockHeight)
			}
		}
	}
	ready := appVersion >= federationMinAppVersion
	remainingForks := federationMinAppVersion - appVersion
	if remainingForks < 0 {
		remainingForks = 0
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"ready":            ready,
		"app_version":      appVersion,
		"required_version": federationMinAppVersion,
		"block_height":     height,
		// A ROUGH block-count estimate to the enabling fork; the frontend turns
		// this into a time estimate using the block rate it measures live.
		"estimated_blocks_remaining": remainingForks * forkDelayBlocks,
	})
}

// --- Federation on/off (Settings surface) -----------------------------------

// handleGetFederationSetting reports whether the inbound federation listener is
// enabled. Off means the node can still reach OUT (recall, receipt delivery)
// but won't accept inbound connections — no one can join or reach this node.
func (h *DashboardHandler) handleGetFederationSetting(w http.ResponseWriter, _ *http.Request) {
	writeJSONResp(w, http.StatusOK, map[string]any{
		"enabled":      h.FederationEnabled,
		"configurable": h.SetFederationEnabledFn != nil,
	})
}

// handleSetFederationSetting persists federation.enabled and restarts the node
// so the inbound listener starts/stops. Mirrors network-mode's re-exec:
// non-destructive (chain + memories preserved), operator re-unlocks the vault
// after restart. A no-op restart when the value is unchanged.
func (h *DashboardHandler) handleSetFederationSetting(w http.ResponseWriter, r *http.Request) {
	if h.SetFederationEnabledFn == nil {
		writeError(w, http.StatusServiceUnavailable, "federation toggle not available on this node")
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "expected {\"enabled\": bool}")
		return
	}
	if body.Enabled == h.FederationEnabled {
		writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "enabled": body.Enabled, "restarting": false})
		return
	}
	if err := h.SetFederationEnabledFn(body.Enabled); err != nil {
		writeError(w, http.StatusInternalServerError, "save federation setting: "+err.Error())
		return
	}
	h.FederationEnabled = body.Enabled
	// Where in-process re-exec is unsupported (Windows), the setting is already
	// persisted — it just needs a manual restart to take effect. Say so plainly
	// instead of promising a restart that will fail and silently self-revert.
	if !restartInProcessSupported() {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "enabled": body.Enabled, "restarting": false,
			"message": "Saved. Restart SAGE to apply the federation change.",
		})
		return
	}
	if h.RequestRestart == nil {
		_ = h.SetFederationEnabledFn(!body.Enabled)
		h.FederationEnabled = !body.Enabled
		writeError(w, http.StatusServiceUnavailable, "clean restart coordinator is unavailable; the setting was not changed")
		return
	}
	if err := h.RequestRestart(); err != nil {
		_ = h.SetFederationEnabledFn(!body.Enabled)
		h.FederationEnabled = !body.Enabled
		writeError(w, http.StatusServiceUnavailable, "could not begin a clean restart; the setting was not changed: "+err.Error())
		return
	}
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"ok": true, "enabled": body.Enabled, "restarting": true,
		"status": "draining", "message": "Saving and restarting cleanly to apply the federation change…",
	})
}

// --- v11.6 host-controlled domain sync + status -----------------------------

// syncStore returns the SQLite store the sync tables live on, or nil on any
// other backend (sync is SQLite-only).
func (h *DashboardHandler) syncStore() *store.SQLiteStore {
	ss, _ := h.store.(*store.SQLiteStore)
	return ss
}

// findAgreement returns the active cross_fed record for a chain, or nil.
func (h *DashboardHandler) findAgreement(chain string) *store.CrossFedRecord {
	if h.BadgerStore == nil {
		return nil
	}
	records, err := h.BadgerStore.ListCrossFed()
	if err != nil {
		return nil
	}
	now := time.Now().Unix()
	for i := range records {
		r := records[i]
		if r.RemoteChainID == chain {
			if r.Status != "active" || (r.ExpiresAt != 0 && now >= r.ExpiresAt) {
				return nil
			}
			return &r
		}
	}
	return nil
}

func (h *DashboardHandler) handleFedSyncGet(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Domain sync requires the SQLite store backend.")
		return
	}
	domains, err := ss.GetSyncDomains(r.Context(), chain)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read sync domains.")
		return
	}
	if domains == nil {
		domains = []string{}
	}
	role := "legacy"
	policyVersion := 1
	revision := int64(0)
	delivered := int64(0)
	control, controlErr := ss.GetSyncControl(r.Context(), chain)
	if controlErr != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read sync policy ownership.")
		return
	}
	if control != nil {
		role, policyVersion, revision, delivered = control.Role, control.PolicyVersion, control.Revision, control.DeliveredRevision
	}
	publish := append([]string(nil), domains...)
	subscribe := append([]string(nil), domains...)
	remotePublish := []string{}
	remoteSubscribe := []string{}
	v3Marker := control != nil && (control.PolicyVersion >= federation.SyncPolicyVersionPeerRBAC ||
		control.RemotePolicyVersion >= federation.SyncPolicyVersionPeerRBAC)
	if v3Marker && (control.BindingState != "active" || control.PeerAgentID == "" ||
		control.PolicyEpoch == "" || control.RemoteCAPin == "") {
		fedWriteErr(w, http.StatusConflict, "This v3 sync binding is incomplete; re-pair before reading it.")
		return
	}
	if v3Marker {
		var readErr error
		publish, readErr = ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionLocalPublish)
		if readErr == nil {
			subscribe, readErr = ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionLocalSubscribe)
		}
		if readErr == nil {
			remotePublish, readErr = ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionRemotePublish)
		}
		if readErr == nil {
			remoteSubscribe, readErr = ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionRemoteSubscribe)
		}
		if readErr != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to read directional sync policy.")
			return
		}
	}
	for _, values := range []*[]string{&publish, &subscribe, &remotePublish, &remoteSubscribe} {
		if *values == nil {
			*values = []string{}
		}
	}
	response := map[string]any{
		"remote_chain_id": chain,
		"publish_domains": publish, "subscribe_domains": subscribe,
		"remote_publish_domains": remotePublish, "remote_subscribe_domains": remoteSubscribe,
		"sync_role": role, "policy_version": policyVersion, "revision": revision, "delivered_revision": delivered,
	}
	if !v3Marker || slices.Equal(publish, subscribe) {
		// The compatibility field describes one bilateral set. It is truthful for
		// legacy links and for a v3 link only when both local lanes match.
		response["sync_domains"] = publish
	}
	fedWriteJSON(w, http.StatusOK, response)
}

type hostSyncPolicyDriver interface {
	SetHostSyncPolicy(context.Context, string, []string) (*federation.HostSyncPolicyResult, error)
}

func (h *DashboardHandler) handleFedSyncSet(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Changing federation sync requires the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Domain sync requires the SQLite store backend.")
		return
	}
	agreement := h.findAgreement(chain)
	if agreement == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	var body struct {
		Domains          *[]string `json:"domains"`
		PublishDomains   *[]string `json:"publish_domains"`
		SubscribeDomains *[]string `json:"subscribe_domains"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected publish_domains and/or subscribe_domains arrays.")
		return
	}
	if body.Domains == nil && body.PublishDomains == nil && body.SubscribeDomains == nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected publish_domains and/or subscribe_domains arrays.")
		return
	}
	if body.Domains != nil && (body.PublishDomains != nil || body.SubscribeDomains != nil) {
		fedWriteErr(w, http.StatusBadRequest, "Legacy domains cannot be combined with directional fields.")
		return
	}
	validateDomains := func(values *[]string) bool {
		if values == nil {
			return true
		}
		if len(*values) > 100 {
			fedWriteErr(w, http.StatusBadRequest, "A sync permission set is capped at 100 domains.")
			return false
		}
		for _, d := range *values {
			if d == "" {
				fedWriteErr(w, http.StatusBadRequest, "Sync domains must be non-empty.")
				return false
			}
			if d == "*" {
				fedWriteErr(w, http.StatusBadRequest, "Sync domains must be concrete (no \"*\").")
				return false
			}
		}
		return true
	}
	if !validateDomains(body.Domains) || !validateDomains(body.PublishDomains) || !validateDomains(body.SubscribeDomains) {
		return
	}
	control, controlErr := ss.GetSyncControl(r.Context(), chain)
	if controlErr != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Could not verify host/guest sync policy ownership.")
		return
	}
	v3Marker := control != nil && (control.PolicyVersion >= federation.SyncPolicyVersionPeerRBAC ||
		control.RemotePolicyVersion >= federation.SyncPolicyVersionPeerRBAC)
	frozenPeer := control != nil && control.BindingState == "active" && control.PeerAgentID != "" &&
		control.PolicyEpoch != "" && control.RemoteCAPin != ""
	if v3Marker && !frozenPeer {
		fedWriteErr(w, http.StatusConflict, "This v3 sync binding is incomplete; re-pair before changing it.")
		return
	}
	if v3Marker && body.Domains != nil {
		fedWriteErr(w, http.StatusConflict, "This connection uses directional publish_domains and subscribe_domains, not legacy domains.")
		return
	}
	// Either role may author its own independent v3 Publish/Subscribe lanes once
	// the peer identity and ceremony generation are frozen. Omitted lanes are
	// preserved; an explicit empty array clears only that lane. A frozen v2 link
	// can migrate by making its first directional update, but legacy {domains}
	// is never silently reinterpreted as v3 authority.
	if body.PublishDomains != nil || body.SubscribeDomains != nil {
		if !frozenPeer {
			fedWriteErr(w, http.StatusConflict, "This legacy connection has no frozen peer identity; re-pair before using directional sync.")
			return
		}
		driver, ok := h.Federation.(interface {
			UpdateDirectionalSyncPolicy(context.Context, string, *[]string, *[]string) (*federation.DirectionalSyncPolicyResult, error)
		})
		if !ok {
			fedWriteErr(w, http.StatusNotImplemented, "Directional sync policy is unavailable.")
			return
		}
		result, err := driver.UpdateDirectionalSyncPolicy(r.Context(), chain, body.PublishDomains, body.SubscribeDomains)
		if err != nil {
			fedWriteErr(w, http.StatusConflict, err.Error())
			return
		}
		remotePublish, _ := ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionRemotePublish)
		remoteSubscribe, _ := ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionRemoteSubscribe)
		if remotePublish == nil {
			remotePublish = []string{}
		}
		if remoteSubscribe == nil {
			remoteSubscribe = []string{}
		}
		response := map[string]any{
			"remote_chain_id": chain,
			"publish_domains": result.PublishDomains, "subscribe_domains": result.SubscribeDomains,
			"remote_publish_domains": remotePublish, "remote_subscribe_domains": remoteSubscribe,
			"sync_role": control.Role, "policy_version": result.Version,
			"revision": result.Revision, "state": result.State,
		}
		if slices.Equal(result.PublishDomains, result.SubscribeDomains) {
			response["sync_domains"] = result.PublishDomains
		}
		fedWriteJSON(w, http.StatusOK, response)
		return
	}
	legacyDomains := []string{}
	if body.Domains != nil {
		legacyDomains = *body.Domains
	}
	if control != nil {
		if control.Role == "guest" {
			fedWriteErr(w, http.StatusConflict, "This legacy guest connection must be re-paired before changing sync policy.")
			return
		}
		driver, ok := h.Federation.(hostSyncPolicyDriver)
		if !ok {
			fedWriteErr(w, http.StatusNotImplemented, "Host-managed legacy sync policy is unavailable.")
			return
		}
		result, err := driver.SetHostSyncPolicy(r.Context(), chain, legacyDomains)
		if err != nil {
			fedWriteErr(w, http.StatusConflict, err.Error())
			return
		}
		fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "sync_domains": result.Domains,
			"publish_domains": result.Domains, "subscribe_domains": result.Domains,
			"remote_publish_domains": []string{}, "remote_subscribe_domains": []string{},
			"sync_role": "host", "policy_version": result.Version, "revision": result.Revision, "state": result.State})
		return
	}
	// Legacy links retain the old bilateral rule. Host-managed v2 policies are
	// allowed to name a topic shared by either side; each sender's own agreement
	// remains the authoritative egress gate.
	for _, d := range legacyDomains {
		if !federation.DomainAllowed(agreement.AllowedDomains, d) {
			fedWriteErr(w, http.StatusBadRequest, "Domain "+strconv.Quote(d)+" is not covered by this legacy agreement's shared domains.")
			return
		}
	}
	if err := ss.SetSyncDomains(r.Context(), chain, legacyDomains); err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to save sync domains.")
		return
	}
	saved, _ := ss.GetSyncDomains(r.Context(), chain)
	if saved == nil {
		saved = []string{}
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "sync_domains": saved,
		"publish_domains": saved, "subscribe_domains": saved,
		"remote_publish_domains": []string{}, "remote_subscribe_domains": []string{}})
}

// handleFedSyncResend requeues rejected/failed outbox rows back to pending so
// the operator can retry after fixing the cause (peer widened consent, content
// changed, etc.). Body {"memory_id":"..."} resends one; empty body resends all
// rejected+failed for the connection.
func (h *DashboardHandler) handleFedSyncResend(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Domain sync requires the SQLite store backend.")
		return
	}
	var body struct {
		MemoryID string `json:"memory_id"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body) // optional
	n, err := ss.RequeueSyncOutbox(r.Context(), chain, body.MemoryID)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to requeue.")
		return
	}
	// Nudge the drainer so it retries promptly instead of waiting for the tick.
	if fn, ok := h.Federation.(interface{ NudgeSync() }); ok {
		fn.NudgeSync()
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"requeued": n})
}

func (h *DashboardHandler) handleFedSyncStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Domain sync requires the SQLite store backend.")
		return
	}
	ctx := r.Context()
	counts, err := ss.CountSyncOutboxByState(ctx, chain)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read sync status.")
		return
	}
	domains, _ := ss.GetSyncDomains(ctx, chain)
	if domains == nil {
		domains = []string{}
	}
	rejected, _ := ss.ListSyncOutbox(ctx, chain, store.SyncStateRejected, 50)
	failed, _ := ss.ListSyncOutbox(ctx, chain, store.SyncStateFailed, 50)
	pending, _ := ss.ListSyncOutbox(ctx, chain, store.SyncStatePending, 50)
	type row struct {
		MemoryID string `json:"memory_id"`
		Reason   string `json:"reason,omitempty"`
	}
	toRows := func(items []store.SyncOutboxItem) []row {
		out := make([]row, 0, len(items))
		for _, it := range items {
			out = append(out, row{MemoryID: it.MemoryID, Reason: it.LastError})
		}
		return out
	}
	resp := map[string]any{
		"remote_chain_id": chain,
		"sync_domains":    domains,
		"outbox_counts":   counts,
		"rejected":        toRows(rejected),
		"failed":          toRows(failed),
		"pending":         toRows(pending),
	}
	if st, ok := h.Federation.SyncReconcileInfo(chain); ok {
		resp["peer_consented_domains"] = st.PeerConsented
		resp["peer_unsupported"] = st.PeerUnsupported
		if !st.LastReconcile.IsZero() {
			resp["last_reconcile"] = st.LastReconcile.UTC().Format(time.RFC3339)
		}
	}
	fedWriteJSON(w, http.StatusOK, resp)
}

// --- Connections list / revoke / status ------------------------------------

// FedConnection is one cross_fed agreement for the Connections view.
type FedConnection struct {
	RemoteChainID  string   `json:"remote_chain_id"`
	PeerName       string   `json:"peer_name,omitempty"`     // friendly label the peer chose (cosmetic)
	PeerAgentID    string   `json:"peer_agent_id,omitempty"` // frozen JOIN operator key; group invite only
	LocalRole      string   `json:"local_role,omitempty"`    // host or guest in the original JOIN ceremony
	Endpoint       string   `json:"endpoint"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Status         string   `json:"status"`
	Expired        bool     `json:"expired"`
	SharingPaused  bool     `json:"sharing_paused"`
	EndedBy        string   `json:"ended_by,omitempty"`
	EndedMessage   string   `json:"ended_message,omitempty"`
	EndedAt        string   `json:"ended_at,omitempty"`
}

// handleGetNetworkName returns the local network's friendly label + the raw
// chain id (the immutable technical identity shown alongside it), and whether
// renaming is available on this build.
func (h *DashboardHandler) handleGetNetworkName(w http.ResponseWriter, _ *http.Request) {
	if !h.fedReady(w) {
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"name":         h.Federation.NetworkName(),
		"chain_id":     h.Federation.LocalChainID(),
		"configurable": h.SetNetworkNameFn != nil,
	})
}

// handleSetNetworkName renames the local network: sanitizes the label, persists
// it to config.yaml, and pushes it to the live federation Manager so the next
// join ceremony carries it (no restart). The label is cosmetic + unauthenticated
// — it never affects trust or the chain id.
func (h *DashboardHandler) handleSetNetworkName(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	if h.SetNetworkNameFn == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Renaming the network is not available on this build.")
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request body.")
		return
	}
	name := federation.SanitizeNetworkName(body.Name)
	if err := h.SetNetworkNameFn(name); err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Could not save the network name.")
		return
	}
	h.Federation.SetNetworkName(name)
	fedWriteJSON(w, http.StatusOK, map[string]any{"name": name, "chain_id": h.Federation.LocalChainID()})
}

func (h *DashboardHandler) handleFedConnections(w http.ResponseWriter, _ *http.Request) {
	if !h.fedReady(w) {
		return
	}
	out := map[string]any{
		"local_chain_id":     h.Federation.LocalChainID(),
		"local_network_name": h.Federation.NetworkName(),
		"connections":        []FedConnection{},
	}
	if h.BadgerStore != nil {
		records, err := h.BadgerStore.ListCrossFed()
		if err != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to list connections.")
			return
		}
		// Best-effort friendly labels for the connections list (cosmetic; a
		// missing/Postgres store just falls back to the raw chain id in the UI).
		var peerNames map[string]string
		if ss := h.syncStore(); ss != nil {
			peerNames, _ = ss.GetPeerNames(context.Background())
		}
		now := time.Now().Unix()
		conns := make([]FedConnection, 0, len(records))
		for _, rec := range records {
			conn := FedConnection{
				RemoteChainID:  rec.RemoteChainID,
				PeerName:       peerNames[rec.RemoteChainID],
				Endpoint:       rec.Endpoint,
				MaxClearance:   int(rec.MaxClearance),
				AllowedDomains: rec.AllowedDomains,
				Status:         rec.Status,
				Expired:        rec.ExpiresAt != 0 && now >= rec.ExpiresAt,
			}
			if rec.Status == "active" && !conn.Expired {
				if driver, ok := h.Federation.(peerRBACPolicyDriver); ok {
					if policy, policyErr := driver.GetPeerRBACPolicy(context.Background(), rec.RemoteChainID); policyErr == nil && policy != nil {
						conn.SharingPaused = policy.Paused
					}
				}
			}
			if ss := h.syncStore(); ss != nil {
				if control, controlErr := ss.GetSyncControl(context.Background(), rec.RemoteChainID); controlErr == nil && control != nil {
					conn.PeerAgentID = control.PeerAgentID
					conn.LocalRole = control.Role
				}
				if event, eventErr := ss.GetFederationConnectionEvent(context.Background(), rec.RemoteChainID); eventErr == nil && event != nil {
					conn.EndedBy = event.Event
					conn.EndedMessage = event.Message
					conn.EndedAt = event.CreatedAt
				}
			}
			conns = append(conns, conn)
		}
		out["connections"] = conns
	}
	fedWriteJSON(w, http.StatusOK, out)
}

func (h *DashboardHandler) handleFedRevoke(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	var hash string
	var notifyResult *federation.RevokeAgreementResult
	var err error
	if driver, ok := h.Federation.(interface {
		RevokeAgreementNotifying(string) (*federation.RevokeAgreementResult, error)
	}); ok {
		notifyResult, err = driver.RevokeAgreementNotifying(chain)
		if notifyResult != nil {
			hash = notifyResult.TxHash
		}
	} else {
		hash, err = h.Federation.RevokeAgreement(chain)
	}
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	if _, notifying := h.Federation.(interface {
		RevokeAgreementNotifying(string) (*federation.RevokeAgreementResult, error)
	}); notifying && notifyResult == nil {
		fedWriteErr(w, http.StatusBadGateway, "Federation revoke returned no result.")
		return
	}
	out := map[string]any{"remote_chain_id": chain, "status": "revoked", "tx_hash": hash}
	if notifyResult != nil {
		out["peer_notified"] = notifyResult.PeerNotified
		if notifyResult.NoticeError != "" {
			out["notification_warning"] = notifyResult.NoticeError
		}
	}
	if cleanupErr := h.ReconcileFederationManagedGrants(r.Context()); cleanupErr != nil {
		// Trust is already revoked and the peer-RBAC policy denies immediately.
		// Keep the durable ledger for the background tx-7 retry and report that
		// distinction instead of pretending the on-chain cleanup finished.
		out["grant_cleanup_pending"] = true
		out["warning"] = "Connection revoked; a managed write-grant cleanup will retry in the background."
	}
	fedWriteJSON(w, http.StatusOK, out)
}

func (h *DashboardHandler) handleFedPeerStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	chain := chi.URLParam(r, "chain_id")
	st, err := h.Federation.PeerStatus(ctx, chain)
	if err != nil {
		fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "reachable": false, "error": err.Error()})
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "reachable": true, "peer_time": st.Time, "network_name": st.NetworkName})
}

// --- Host wizard ------------------------------------------------------------

func (h *DashboardHandler) handleFedHostCreate(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		Endpoint  string `json:"endpoint"`
		Transport string `json:"transport"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	var res *federation.HostCreateResult
	var err error
	if body.Transport == "internet" {
		if driver, ok := h.Federation.(interface {
			HostCreateMode(string, bool) (*federation.HostCreateResult, error)
		}); ok {
			res, err = driver.HostCreateMode(body.Endpoint, true)
		} else {
			err = fmt.Errorf("internet join is unavailable")
		}
	} else {
		res, err = h.Federation.HostCreate(body.Endpoint)
	}
	if err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedHostScanReturn(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
		ReturnURI string `json:"return_uri"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	if err := h.Federation.HostScanReturn(body.SessionID, body.ReturnURI); err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "scanned"})
}

func (h *DashboardHandler) handleFedHostStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	view, err := h.Federation.HostSessionStatus(chi.URLParam(r, "session_id"))
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, view)
}

func (h *DashboardHandler) handleFedHostApprove(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		TypedCode      string   `json:"typed_code"`
		MaxClearance   int      `json:"max_clearance"`
		AllowedDomains []string `json:"allowed_domains"`
		Mode           string   `json:"mode"`
		Direction      string   `json:"direction"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	err := h.Federation.HostApprove(chi.URLParam(r, "session_id"), body.TypedCode, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func (h *DashboardHandler) handleFedHostAbort(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	if err := h.Federation.HostAbort(chi.URLParam(r, "session_id")); err != nil {
		if errors.Is(err, federation.ErrJoinSessionNotFound) {
			fedWriteErr(w, http.StatusNotFound, "This connection setup no longer exists.")
		} else {
			fedWriteErr(w, http.StatusConflict, "This connection is already being confirmed. Check its status in Federation before revoking it.")
		}
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

// --- Guest wizard -----------------------------------------------------------

func (h *DashboardHandler) handleFedGuestScan(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		URI      string `json:"uri"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	res, err := h.Federation.GuestScan(ctx, body.URI, body.Endpoint)
	if err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedGuestRequest(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID      string   `json:"session_id"`
		Endpoint       string   `json:"endpoint"`
		MaxClearance   int      `json:"max_clearance"`
		AllowedDomains []string `json:"allowed_domains"`
		Mode           string   `json:"mode"`
		Direction      string   `json:"direction"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	res, err := h.Federation.GuestRequest(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedGuestStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	resp, err := h.Federation.GuestPollStatus(ctx, chi.URLParam(r, "session_id"))
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, resp)
}

func (h *DashboardHandler) handleFedGuestAbort(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	driver, ok := h.Federation.(interface {
		GuestAbort(context.Context, string) error
	})
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Guest-side connection cancellation is unavailable.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	if err := driver.GuestAbort(ctx, chi.URLParam(r, "session_id")); err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

func (h *DashboardHandler) handleFedGuestConfirm(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
		Endpoint  string `json:"endpoint"`
		HostScope struct {
			MaxClearance   int      `json:"max_clearance"`
			AllowedDomains []string `json:"allowed_domains"`
			Mode           string   `json:"mode"`
			Direction      string   `json:"direction"`
		} `json:"host_scope"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	// Final confirmation can wait for two sequential consensus commits: the
	// guest's local tx-33, then the host's tx-33 over the peer listener.
	ctx, cancel := context.WithTimeout(r.Context(), federation.JoinConfirmationOperationTimeout())
	defer cancel()
	txHash, err := h.Federation.GuestConfirm(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.HostScope.MaxClearance,
		AllowedDomains: body.HostScope.AllowedDomains,
		Mode:           body.HostScope.Mode,
		Direction:      body.HostScope.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "active", "tx_hash": txHash})
}

// --- v11.8 sync-group management (INT1) -------------------------------------

// groupManagementDriver is the slice of the federation Manager that AUTHORS
// v11.8 group-journal entries. It is reached by a type assertion on h.Federation
// (the same optional-capability idiom as hostSyncPolicyDriver / NudgeSync) so the
// core FederationJoinDriver interface stays unchanged; the concrete *Manager wired
// by SetFederation always satisfies it. Every method is OFF-consensus (journal +
// SQLite overlay only) and locally operator-authored — the emit self-check in the
// federation layer refuses an entry whose resolver-pinned key is not this node's,
// so a REST caller can never author on behalf of another owner or controller, and
// none of this routes into the consensus WRITE gate (write-never-widens).
type groupManagementDriver interface {
	EmitDomainAdd(ctx context.Context, groupID, domainTag string, maxClearance int) (store.SyncGroupLogEntry, error)
	EmitDomainRemove(ctx context.Context, groupID, domainTag string) (store.SyncGroupLogEntry, error)
	EmitSelfRoleChange(ctx context.Context, groupID, role string, selectedDomains []string) (store.SyncGroupLogEntry, error)
	EmitGroupRename(ctx context.Context, groupID, name string) (store.SyncGroupLogEntry, error)
	CreateSyncGroup(ctx context.Context, name string) (string, error)
	DissolveSyncGroup(ctx context.Context, groupID string) (int, error)
	EmitRosterControl(ctx context.Context, groupID, entryType string, payload map[string]string) (store.SyncGroupLogEntry, error)
	EmitMemberInvite(ctx context.Context, groupID, memberChain, memberPubkey, role string, selectedDomains, ownedDomains []string) (store.SyncGroupLogEntry, error)
	EmitEpochRotate(ctx context.Context, groupID, newEpoch, incomingChain, incomingPubkey string) (store.SyncGroupLogEntry, error)
}

func (h *DashboardHandler) isSyncGroupOperatorRequest(r *http.Request) bool {
	operatorID := strings.TrimSpace(h.NodeOperatorAgentID)
	return operatorID != "" && verifiedDashboardAgentID(r.Context()) == operatorID
}

// isFederationMutationOperatorRequest admits either the authenticated local
// CEREBRUM operator or an agent-signed request from this node's exact operator
// identity. Dashboard authentication also admits ordinary agents, so callers
// that make the Manager sign or persist node-wide federation policy must use
// this stricter boundary.
func (h *DashboardHandler) isFederationMutationOperatorRequest(r *http.Request) bool {
	return h.isCEREBRUMOperatorRequest(r) || h.isSyncGroupOperatorRequest(r)
}

func (h *DashboardHandler) federationOperatorGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.isFederationMutationOperatorRequest(r) {
			fedWriteErr(w, http.StatusForbidden, "Federation control requires the local node operator.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// groupDriver resolves the emit surface after the fedReady guard, or writes the
// canonical 501/not-implemented envelope and returns false.
func (h *DashboardHandler) groupDriver(w http.ResponseWriter, r *http.Request) (groupManagementDriver, bool) {
	// Group journal methods sign with the node operator key.  Dashboard auth also
	// admits independently signed agents, so those requests must present the
	// exact operator identity. Authenticated local CEREBRUM is also the operator
	// surface and must remain able to drive the dedicated Sharing & Sync UI.
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Sync-group management requires the local node operator.")
		return nil, false
	}
	if !h.fedReady(w) {
		return nil, false
	}
	d, ok := h.Federation.(groupManagementDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Sync-group management is unavailable on this node.")
		return nil, false
	}
	return d, true
}

// groupEmitResult is the compact receipt for an authored journal entry (NEVER any
// memory content — the payload is roster/domain metadata only).
func groupEmitResult(e store.SyncGroupLogEntry) map[string]any {
	return map[string]any{
		"group_id":   e.GroupID,
		"subchain":   e.Subchain,
		"seq":        e.Seq,
		"entry_type": e.EntryType,
		"entry_hash": e.EntryHash,
	}
}

func (h *DashboardHandler) handleFedGroupCreate(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"name\": \"...\"}.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	groupID, err := d.CreateSyncGroup(ctx, body.Name)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusCreated, map[string]string{"group_id": groupID})
}

// handleFedGroupDissolve lets only the group owner end a shared space. The
// federation layer emits signed terminal removals for all guests before locally
// retiring the owner's card; it intentionally leaves each trusted connection
// and its independent direct-sharing policy untouched.
func (h *DashboardHandler) handleFedGroupDissolve(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	groupID := chi.URLParam(r, "group_id")
	removed, err := d.DissolveSyncGroup(ctx, groupID)
	if err != nil {
		body := map[string]any{
			"error": err.Error(), "group_id": groupID,
			"removed_members": removed,
		}
		if ss := h.syncStore(); ss != nil {
			if state, stateErr := ss.SyncGroupLifecycleState(ctx, groupID); stateErr == nil && state == store.GroupLifecycleDissolving {
				body["status"] = state
			}
		}
		fedWriteJSON(w, http.StatusConflict, body)
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"group_id": groupID, "removed_members": removed, "status": "dissolved",
	})
}

type syncGroupMemberView struct {
	ChainID                 string                        `json:"chain_id"`
	DisplayName             string                        `json:"display_name,omitempty"`
	Role                    string                        `json:"role"`
	State                   string                        `json:"state"`
	JoinedRevision          int64                         `json:"joined_revision"`
	LastAckedRosterRevision int64                         `json:"last_acked_roster_revision"`
	LastSeenJournalHead     string                        `json:"last_seen_journal_head,omitempty"`
	LastSyncAt              string                        `json:"last_sync_at,omitempty"`
	RosterRevisionLag       int64                         `json:"roster_revision_lag"`
	RosterHeadCurrent       bool                          `json:"roster_head_current"`
	CatchUpState            string                        `json:"catch_up_state"`
	Health                  string                        `json:"health"`
	PeerDelivery            *store.SyncPeerDeliveryStatus `json:"peer_delivery,omitempty"`
	// ConsentDomains is the member's full selective-sync SELECTOR set, read from
	// the pending table rather than the promoted one. Those differ: a selector is
	// promoted into the active set only once its domain is inside a live shared
	// root, and a group is seeded before any domain_add, so a freshly joined
	// selective-sync member's ACTIVE set is empty by construction while its
	// selectors sit in pending. Seeding the dashboard from the active set would
	// therefore still render blank for exactly the case this field exists to fix.
	//
	// Pending is also what round-trips: ReplaceGroupMemberConsentDomains deletes
	// every pending row before re-inserting the submitted set, so an "Apply role"
	// carrying the active subset would silently destroy the unpromoted selectors.
	//
	// Always serialized, never omitempty: absent and empty must stay
	// distinguishable, since the dashboard treats undefined as "seed me" and an
	// empty array as "the operator really has no selectors".
	ConsentDomains []string `json:"consent_domains"`
	// ActiveConsentDomains is the promoted subset -- the selectors whose domain is
	// already inside a live shared root and is therefore actually being delivered.
	// A selector stays pending until its owner shares that domain, so without this
	// the dashboard cannot distinguish "syncing" from "waiting", and an operator
	// who selected a domain before it was shared sees their selector listed while
	// receiving nothing, with no explanation anywhere in the UI.
	ActiveConsentDomains []string `json:"active_consent_domains"`
}

// syncGroupMemberProgress converts durable journal cursors into display state.
// It deliberately does not invent a staleness timeout: an active remote member
// with no successful journal observation is "unknown", while exact persisted
// revision/head equality is current. The outbox projection beside this state is
// separately peer-wide and records real delivery timestamps.
func syncGroupMemberProgress(group store.SyncGroup, member store.SyncGroupMember, localChainID string) syncGroupMemberView {
	futureRevision := member.LastAckedRosterRevision > group.RosterRevision
	lag := group.RosterRevision - member.LastAckedRosterRevision
	if lag < 0 {
		lag = 0
	}
	headCurrent := member.LastSeenJournalHead == group.RosterJournalHead
	view := syncGroupMemberView{
		ChainID: member.MemberChainID, Role: member.Role, State: member.MemberState,
		JoinedRevision:          member.JoinedRevision,
		LastAckedRosterRevision: member.LastAckedRosterRevision,
		LastSeenJournalHead:     member.LastSeenJournalHead,
		LastSyncAt:              member.LastSyncAt,
		RosterRevisionLag:       lag,
		RosterHeadCurrent:       headCurrent,
	}
	switch member.MemberState {
	case store.GroupMemberInvited:
		view.CatchUpState, view.Health = "not_active", "invited"
	case store.GroupMemberLeft, store.GroupMemberRemoved:
		view.CatchUpState, view.Health = "not_active", "inactive"
	case store.GroupMemberResyncing:
		view.CatchUpState, view.Health = "resyncing", "recovering"
	case store.GroupMemberActive:
		if futureRevision {
			// A cursor beyond the controller's durable revision is corrupt or
			// incompatible. Never clamp it into a false healthy/current display.
			view.CatchUpState, view.Health = "unknown", "unknown"
		} else if lag > 0 || !headCurrent {
			view.CatchUpState, view.Health = "catching_up", "catching_up"
		} else if member.MemberChainID != localChainID && member.LastSyncAt == "" {
			view.CatchUpState, view.Health = "current", "unknown"
		} else {
			view.CatchUpState, view.Health = "current", "healthy"
		}
	default:
		// The store rejects unknown states, but a fail-safe display result keeps a
		// future value from being mistaken for healthy by an older dashboard.
		view.CatchUpState, view.Health = "unknown", "unknown"
	}
	return view
}

// handleFedGroupList enumerates the local node's sync groups with their roster and
// active owner-signed shared domains — the operator's read view of the group plane.
func (h *DashboardHandler) handleFedGroupList(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Sync-group metadata requires the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Sync-group management requires the SQLite store backend.")
		return
	}
	ctx := r.Context()
	groups, err := ss.ListSyncGroups(ctx)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to list sync groups.")
		return
	}
	local := h.Federation.LocalChainID()
	// A group is not a second kind of connection. A member consumes group policy
	// through its controller/owner relationship, not through a mesh to every
	// other member. A non-controller whose owner relationship was revoked must
	// not see the group; the controller retains it for other valid members. A
	// temporary outage retains the active agreement and therefore still shows it.
	activePeers := map[string]bool{}
	if h.BadgerStore != nil {
		records, listErr := h.BadgerStore.ListCrossFed()
		if listErr != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to check federation relationships.")
			return
		}
		now := time.Now().Unix()
		for _, rec := range records {
			activePeers[rec.RemoteChainID] = rec.Status == "active" && (rec.ExpiresAt == 0 || now < rec.ExpiresAt)
		}
	}
	type domainView struct {
		DomainTag    string `json:"domain_tag"`
		OwnerChainID string `json:"owner_chain_id"`
		MaxClearance int    `json:"max_clearance"`
	}
	type groupView struct {
		GroupID           string                `json:"group_id"`
		DisplayName       string                `json:"display_name,omitempty"`
		Controller        string                `json:"controller_chain_id"`
		ControllerName    string                `json:"controller_display_name,omitempty"`
		Epoch             string                `json:"epoch"`
		RosterRevision    int64                 `json:"roster_revision"`
		RosterJournalHead string                `json:"roster_journal_head,omitempty"`
		LifecycleState    string                `json:"lifecycle_state,omitempty"`
		IsController      bool                  `json:"is_controller"`
		LocalRole         string                `json:"local_role,omitempty"`
		Members           []syncGroupMemberView `json:"members"`
		SharedDomains     []domainView          `json:"shared_domains"`
	}
	peerDelivery := make(map[string]store.SyncPeerDeliveryStatus)
	peerNames, _ := ss.GetPeerNames(ctx)
	if peerNames == nil {
		peerNames = make(map[string]string)
	}
	if localName := strings.TrimSpace(h.Federation.NetworkName()); localName != "" {
		peerNames[local] = localName
	}
	out := make([]groupView, 0, len(groups))
	for i := range groups {
		g := groups[i]
		members, mErr := ss.ListSyncGroupMembers(ctx, g.GroupID)
		if mErr != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to read group members.")
			return
		}
		// This node must still be an active participant. A controller's
		// member_remove journal entry is deliberately retained locally for audit,
		// but it must immediately remove the group from the evicted member's
		// dashboard. Checking only the controller relationship leaves that member
		// looking at a stale group forever, despite its own roster row being
		// removed and all delivery consent already revoked.
		localActive := false
		for _, member := range members {
			if member.MemberChainID == local && (member.MemberState == store.GroupMemberActive || member.MemberState == store.GroupMemberResyncing) {
				localActive = true
				break
			}
		}
		if !localActive {
			continue
		}
		if h.BadgerStore != nil && g.ControllerChainID != local && !activePeers[g.ControllerChainID] {
			// A member needs its controller/owner link, not links to every other
			// member. Requiring a mesh made a valid host-and-many-guests group vanish.
			continue
		}
		gv := groupView{
			GroupID: g.GroupID, DisplayName: g.DisplayName,
			Controller: g.ControllerChainID, Epoch: g.Epoch,
			ControllerName: peerNames[g.ControllerChainID],
			RosterRevision: g.RosterRevision, RosterJournalHead: g.RosterJournalHead,
			IsController:  g.ControllerChainID == local,
			Members:       []syncGroupMemberView{},
			SharedDomains: []domainView{},
		}
		gv.LifecycleState, err = ss.SyncGroupLifecycleState(ctx, g.GroupID)
		if err != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to read group lifecycle.")
			return
		}
		for _, mem := range members {
			// A removed/left member is retained in the local journal projection for
			// audit and terminal delivery, not as a current group participant. Do
			// not make the remaining people see an inflated member count or a stuck
			// "setup in progress" card; omitting it also lets the owner issue the
			// fresh signed invite required for a later rejoin.
			if mem.MemberState == store.GroupMemberRemoved || mem.MemberState == store.GroupMemberLeft {
				continue
			}
			member := syncGroupMemberProgress(g, mem, local)
			member.DisplayName = peerNames[mem.MemberChainID]
			consent, cErr := ss.ListPendingGroupMemberConsentDomains(ctx, g.GroupID, mem.MemberChainID)
			if cErr != nil {
				fedWriteErr(w, http.StatusInternalServerError, "Failed to read member consent domains.")
				return
			}
			if consent == nil {
				consent = []string{}
			}
			member.ConsentDomains = consent
			active, aErr := ss.ListGroupMemberConsentDomains(ctx, g.GroupID, mem.MemberChainID)
			if aErr != nil {
				fedWriteErr(w, http.StatusInternalServerError, "Failed to read active member consent domains.")
				return
			}
			if active == nil {
				active = []string{}
			}
			member.ActiveConsentDomains = active
			if mem.MemberChainID != local {
				delivery, found := peerDelivery[mem.MemberChainID]
				if !found {
					delivery, mErr = ss.GetSyncPeerDeliveryStatus(ctx, mem.MemberChainID)
					if mErr != nil {
						fedWriteErr(w, http.StatusInternalServerError, "Failed to read member delivery status.")
						return
					}
					peerDelivery[mem.MemberChainID] = delivery
				}
				member.PeerDelivery = &delivery
			}
			gv.Members = append(gv.Members, member)
			if mem.MemberChainID == local {
				gv.LocalRole = mem.Role
			}
		}
		domains, dErr := ss.ListSyncGroupDomains(ctx, g.GroupID, true)
		if dErr != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to read group domains.")
			return
		}
		for _, d := range domains {
			gv.SharedDomains = append(gv.SharedDomains, domainView{
				DomainTag: d.DomainTag, OwnerChainID: d.OwnerChainID, MaxClearance: d.MaxClearance,
			})
		}
		out = append(out, gv)
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"local_chain_id": local, "local_network_name": peerNames[local], "groups": out,
	})
}

// handleFedGroupDomainAdd authors an owner-unilateral domain_add (EmitDomainAdd):
// the local node must be an active member AND the on-chain owner/admin of the tag.
func (h *DashboardHandler) handleFedGroupDomainAdd(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	groupID := chi.URLParam(r, "group_id")
	var body struct {
		DomainTag    string `json:"domain_tag"`
		MaxClearance int    `json:"max_clearance"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"domain_tag\": \"...\", \"max_clearance\": 0-4}.")
		return
	}
	if strings.TrimSpace(body.DomainTag) == "" {
		fedWriteErr(w, http.StatusBadRequest, "domain_tag is required.")
		return
	}
	if body.MaxClearance < 0 || body.MaxClearance > 4 {
		fedWriteErr(w, http.StatusBadRequest, "max_clearance must be 0..4.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitDomainAdd(ctx, groupID, body.DomainTag, body.MaxClearance)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

// handleFedGroupDomainRemove authors an owner-unilateral domain_remove
// (EmitDomainRemove): the group must already record THIS node as the tag's owner.
func (h *DashboardHandler) handleFedGroupDomainRemove(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	groupID := chi.URLParam(r, "group_id")
	var body struct {
		DomainTag string `json:"domain_tag"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"domain_tag\": \"...\"}.")
		return
	}
	if strings.TrimSpace(body.DomainTag) == "" {
		fedWriteErr(w, http.StatusBadRequest, "domain_tag is required.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitDomainRemove(ctx, groupID, body.DomainTag)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

// handleFedGroupSelfRole authors the local member's OWN role_change
// (EmitSelfRoleChange), optionally carrying the selective-sync consent subset. It
// only ever changes THIS node's role; a controller changing ANOTHER member's role
// goes through the roster-control surface below.
func (h *DashboardHandler) handleFedGroupSelfRole(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	groupID := chi.URLParam(r, "group_id")
	var body struct {
		Role            string   `json:"role"`
		SelectedDomains []string `json:"selected_domains"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"role\": \"...\", \"selected_domains\": [...]}.")
		return
	}
	switch body.Role {
	case store.GroupRoleFullSync, store.GroupRoleSelectiveSync, store.GroupRoleEnrolledNoSync:
	default:
		fedWriteErr(w, http.StatusBadRequest, "role must be full-sync, selective-sync, or enrolled-no-sync.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitSelfRoleChange(ctx, groupID, body.Role, body.SelectedDomains)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

// handleFedGroupRename publishes a controller-signed friendly label inside a
// backward-compatible roster manifest extension. The group ID remains immutable
// and auditable while v11.11.1 peers safely ignore the optional label field.
func (h *DashboardHandler) handleFedGroupRename(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"name\": \"...\"}.")
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" || utf8.RuneCountInString(name) > 96 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		fedWriteErr(w, http.StatusBadRequest, "name must be 1..96 characters.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitGroupRename(ctx, chi.URLParam(r, "group_id"), name)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

// handleFedGroupRosterControl authors a controller-affecting roster entry
// (EmitRosterControl): group_create, member_invite/activate, member_remove(other),
// role_change(other), epoch_rotate, manifest. Gated by authorizeControllerAffecting
// (local == controller; a passed tx-24/25 governance proposal on multi-validator).
// The emitter validates the entry_type and authority, so the operator supplies the
// already-built payload (member/role/epoch fields as the journal apply expects).
func (h *DashboardHandler) handleFedGroupRosterControl(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	groupID := chi.URLParam(r, "group_id")
	var body struct {
		EntryType string            `json:"entry_type"`
		Payload   map[string]string `json:"payload"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"entry_type\": \"...\", \"payload\": {...}}.")
		return
	}
	if strings.TrimSpace(body.EntryType) == "" {
		fedWriteErr(w, http.StatusBadRequest, "entry_type is required.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitRosterControl(ctx, groupID, body.EntryType, body.Payload)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

func (h *DashboardHandler) handleFedGroupMemberInvite(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	var body struct {
		MemberChain  string   `json:"member_chain"`
		MemberPubkey string   `json:"member_pubkey"`
		Role         string   `json:"role"`
		Selected     []string `json:"selected_domains"`
		OwnedDomains []string `json:"owned_domains"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body) != nil || strings.TrimSpace(body.MemberChain) == "" || strings.TrimSpace(body.MemberPubkey) == "" {
		fedWriteErr(w, http.StatusBadRequest, "member_chain, member_pubkey, and role are required.")
		return
	}
	switch body.Role {
	case store.GroupRoleFullSync, store.GroupRoleSelectiveSync, store.GroupRoleEnrolledNoSync:
	default:
		fedWriteErr(w, http.StatusBadRequest, "role must be full-sync, selective-sync, or enrolled-no-sync.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitMemberInvite(ctx, chi.URLParam(r, "group_id"), body.MemberChain, body.MemberPubkey, body.Role, body.Selected, body.OwnedDomains)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}

// handleFedGroupEpochRotate runs the outgoing-controller/incoming-controller
// two-party countersign ceremony. It is deliberately separate from the generic
// roster payload endpoint so a caller cannot omit or synthesize the incoming
// controller proof.
func (h *DashboardHandler) handleFedGroupEpochRotate(w http.ResponseWriter, r *http.Request) {
	d, ok := h.groupDriver(w, r)
	if !ok {
		return
	}
	var body struct {
		Epoch               string `json:"epoch"`
		IncomingChain       string `json:"incoming_chain"`
		IncomingAgentPubkey string `json:"incoming_agent_pubkey"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil ||
		strings.TrimSpace(body.Epoch) == "" || strings.TrimSpace(body.IncomingChain) == "" || strings.TrimSpace(body.IncomingAgentPubkey) == "" {
		fedWriteErr(w, http.StatusBadRequest, "epoch, incoming_chain, and incoming_agent_pubkey are required.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	entry, err := d.EmitEpochRotate(ctx, chi.URLParam(r, "group_id"), body.Epoch, body.IncomingChain, body.IncomingAgentPubkey)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, groupEmitResult(entry))
}
