package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
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
	HostAbort(sessionID string)
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

const fedCallTimeout = 25 * time.Second

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
	r.Get("/v1/dashboard/federation/network-name", h.handleGetNetworkName)
	r.Put("/v1/dashboard/federation/network-name", h.handleSetNetworkName)
	r.Get("/v1/dashboard/federation/connections", h.handleFedConnections)
	r.Post("/v1/dashboard/federation/connections/{chain_id}/revoke", h.handleFedRevoke)
	r.Get("/v1/dashboard/federation/connections/{chain_id}/status", h.handleFedPeerStatus)

	r.Get("/v1/dashboard/federation/lan-endpoint", h.handleFedLanEndpoint)
	r.Get("/v1/dashboard/federation/readiness", h.handleFedReadiness)

	// v11.6 host-controlled domain sync + status (operator-only surface, but the
	// dashboard IS the operator here — cookie-authed local control plane).
	r.Get("/v1/dashboard/federation/connections/{chain_id}/sync", h.handleFedSyncGet)
	r.Put("/v1/dashboard/federation/connections/{chain_id}/sync", h.handleFedSyncSet)
	r.Get("/v1/dashboard/federation/connections/{chain_id}/sync/status", h.handleFedSyncStatus)
	r.Post("/v1/dashboard/federation/connections/{chain_id}/sync/resend", h.handleFedSyncResend)

	r.Post("/v1/dashboard/federation/join/host/create", h.handleFedHostCreate)
	r.Post("/v1/dashboard/federation/join/host/scan-return", h.handleFedHostScanReturn)
	r.Get("/v1/dashboard/federation/join/host/{session_id}", h.handleFedHostStatus)
	r.Post("/v1/dashboard/federation/join/host/{session_id}/approve", h.handleFedHostApprove)
	r.Post("/v1/dashboard/federation/join/host/{session_id}/abort", h.handleFedHostAbort)

	r.Post("/v1/dashboard/federation/join/guest/scan", h.handleFedGuestScan)
	r.Post("/v1/dashboard/federation/join/guest/request", h.handleFedGuestRequest)
	r.Get("/v1/dashboard/federation/join/guest/{session_id}/status", h.handleFedGuestStatus)
	r.Post("/v1/dashboard/federation/join/guest/confirm", h.handleFedGuestConfirm)

	// v11.8 sync-group management (INT1): the local operator's authoring surface
	// over the group-journal EMIT layer. Split by the §8 authorization model —
	// owner-unilateral (domain add/remove on MY scope, MY own role) and
	// controller-affecting (roster control). Everything is OFF-consensus and
	// locally authored; the emit self-check refuses an entry whose resolver-pinned
	// key is not this node's, so a caller can never author for another owner.
	r.Get("/v1/dashboard/federation/groups", h.handleFedGroupList)
	r.Post("/v1/dashboard/federation/groups/{group_id}/domains", h.handleFedGroupDomainAdd)
	r.Post("/v1/dashboard/federation/groups/{group_id}/domains/remove", h.handleFedGroupDomainRemove)
	r.Post("/v1/dashboard/federation/groups/{group_id}/self-role", h.handleFedGroupSelfRole)
	r.Post("/v1/dashboard/federation/groups/{group_id}/roster", h.handleFedGroupRosterControl)
	r.Post("/v1/dashboard/federation/groups/{group_id}/members/invite", h.handleFedGroupMemberInvite)
	r.Post("/v1/dashboard/federation/groups/{group_id}/epoch-rotate", h.handleFedGroupEpochRotate)
}

// --- LAN endpoint suggestion (fix the localhost-in-join-code footgun) -------

// fedDefaultPort is the standard federation listener port (node.go default
// 0.0.0.0:8444, and the placeholder in every wizard field). An operator who
// runs a custom port edits the suggested endpoint in the wizard.
const fedDefaultPort = 8444

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
func (h *DashboardHandler) handleFedLanEndpoint(w http.ResponseWriter, _ *http.Request) {
	cands := directIPv4Candidates()
	out := make([]FedLanCandidate, 0, len(cands))
	for _, c := range cands {
		out = append(out, FedLanCandidate{
			Endpoint:  fmt.Sprintf("https://%s:%d", c.IP, fedDefaultPort),
			IP:        c.IP,
			Iface:     c.Iface,
			IsPrivate: c.IsPrivate,
		})
	}
	suggested := ""
	if len(out) > 0 {
		suggested = out[0].Endpoint // ranked: physical-LAN-private wins
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"port":               fedDefaultPort,
		"suggested_endpoint": suggested,
		"candidates":         out,
	})
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
	revision := int64(0)
	delivered := int64(0)
	control, controlErr := ss.GetSyncControl(r.Context(), chain)
	if controlErr != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read sync policy ownership.")
		return
	}
	if control != nil {
		role, revision, delivered = control.Role, control.Revision, control.DeliveredRevision
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "sync_domains": domains,
		"sync_role": role, "revision": revision, "delivered_revision": delivered})
}

type hostSyncPolicyDriver interface {
	SetHostSyncPolicy(context.Context, string, []string) (*federation.HostSyncPolicyResult, error)
}

func (h *DashboardHandler) handleFedSyncSet(w http.ResponseWriter, r *http.Request) {
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
		Domains []string `json:"domains"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"domains\": [...]}.")
		return
	}
	if len(body.Domains) > 100 {
		fedWriteErr(w, http.StatusBadRequest, "A sync consent set is capped at 100 domains.")
		return
	}
	for _, d := range body.Domains {
		if d == "" {
			fedWriteErr(w, http.StatusBadRequest, "Sync domains must be non-empty.")
			return
		}
		if d == "*" {
			fedWriteErr(w, http.StatusBadRequest, "Sync domains must be concrete (no \"*\").")
			return
		}
		if !federation.DomainAllowed(agreement.AllowedDomains, d) {
			fedWriteErr(w, http.StatusBadRequest, "Domain "+strconv.Quote(d)+" is not covered by the agreement's shared domains.")
			return
		}
	}
	control, controlErr := ss.GetSyncControl(r.Context(), chain)
	if controlErr != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Could not verify host/guest sync policy ownership.")
		return
	}
	if control != nil {
		if control.Role == "guest" {
			fedWriteErr(w, http.StatusConflict, "Memory sync for this connection is managed by the host node.")
			return
		}
		driver, ok := h.Federation.(hostSyncPolicyDriver)
		if !ok {
			fedWriteErr(w, http.StatusNotImplemented, "Host-managed sync policy is unavailable.")
			return
		}
		result, err := driver.SetHostSyncPolicy(r.Context(), chain, body.Domains)
		if err != nil {
			fedWriteErr(w, http.StatusConflict, err.Error())
			return
		}
		fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "sync_domains": result.Domains,
			"sync_role": "host", "revision": result.Revision, "state": result.State})
		return
	}
	if err := ss.SetSyncDomains(r.Context(), chain, body.Domains); err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to save sync domains.")
		return
	}
	saved, _ := ss.GetSyncDomains(r.Context(), chain)
	if saved == nil {
		saved = []string{}
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "sync_domains": saved})
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
	PeerName       string   `json:"peer_name,omitempty"` // friendly label the peer chose (cosmetic)
	Endpoint       string   `json:"endpoint"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Status         string   `json:"status"`
	Expired        bool     `json:"expired"`
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
			conns = append(conns, FedConnection{
				RemoteChainID:  rec.RemoteChainID,
				PeerName:       peerNames[rec.RemoteChainID],
				Endpoint:       rec.Endpoint,
				MaxClearance:   int(rec.MaxClearance),
				AllowedDomains: rec.AllowedDomains,
				Status:         rec.Status,
				Expired:        rec.ExpiresAt != 0 && now >= rec.ExpiresAt,
			})
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
	hash, err := h.Federation.RevokeAgreement(chain)
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"remote_chain_id": chain, "status": "revoked", "tx_hash": hash})
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
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "reachable": true, "peer_time": st.Time})
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
	h.Federation.HostAbort(chi.URLParam(r, "session_id"))
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
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
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
	EmitRosterControl(ctx context.Context, groupID, entryType string, payload map[string]string) (store.SyncGroupLogEntry, error)
	EmitMemberInvite(ctx context.Context, groupID, memberChain, memberPubkey, role string, selectedDomains, ownedDomains []string) (store.SyncGroupLogEntry, error)
	EmitEpochRotate(ctx context.Context, groupID, newEpoch, incomingChain, incomingPubkey string) (store.SyncGroupLogEntry, error)
}

func (h *DashboardHandler) isSyncGroupOperatorRequest(r *http.Request) bool {
	operatorID := strings.TrimSpace(h.NodeOperatorAgentID)
	return operatorID != "" && verifiedDashboardAgentID(r.Context()) == operatorID
}

// groupDriver resolves the emit surface after the fedReady guard, or writes the
// canonical 501/not-implemented envelope and returns false.
func (h *DashboardHandler) groupDriver(w http.ResponseWriter, r *http.Request) (groupManagementDriver, bool) {
	// Group journal methods sign with the node operator key.  Dashboard auth also
	// admits independently signed agents, so it is not by itself an operator
	// boundary.  Never let an arbitrary agent turn its request into an
	// operator-authored roster/domain mutation.
	if !h.isSyncGroupOperatorRequest(r) {
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

// handleFedGroupList enumerates the local node's sync groups with their roster and
// active owner-signed shared domains — the operator's read view of the group plane.
func (h *DashboardHandler) handleFedGroupList(w http.ResponseWriter, r *http.Request) {
	if !h.isSyncGroupOperatorRequest(r) {
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
	type memberView struct {
		ChainID string `json:"chain_id"`
		Role    string `json:"role"`
		State   string `json:"state"`
	}
	type domainView struct {
		DomainTag    string `json:"domain_tag"`
		OwnerChainID string `json:"owner_chain_id"`
		MaxClearance int    `json:"max_clearance"`
	}
	type groupView struct {
		GroupID       string       `json:"group_id"`
		DisplayName   string       `json:"display_name,omitempty"`
		Controller    string       `json:"controller_chain_id"`
		Epoch         string       `json:"epoch"`
		IsController  bool         `json:"is_controller"`
		LocalRole     string       `json:"local_role,omitempty"`
		Members       []memberView `json:"members"`
		SharedDomains []domainView `json:"shared_domains"`
	}
	out := make([]groupView, 0, len(groups))
	for i := range groups {
		g := groups[i]
		gv := groupView{
			GroupID: g.GroupID, DisplayName: g.DisplayName,
			Controller: g.ControllerChainID, Epoch: g.Epoch,
			IsController:  g.ControllerChainID == local,
			Members:       []memberView{},
			SharedDomains: []domainView{},
		}
		members, mErr := ss.ListSyncGroupMembers(ctx, g.GroupID)
		if mErr != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to read group members.")
			return
		}
		for _, mem := range members {
			gv.Members = append(gv.Members, memberView{ChainID: mem.MemberChainID, Role: mem.Role, State: mem.MemberState})
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
	fedWriteJSON(w, http.StatusOK, map[string]any{"local_chain_id": local, "groups": out})
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
