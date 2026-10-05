package web

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/tx"
)

// The dashboard's app-version upgrade surface.
//
// Two different mechanisms live behind "governance" in SAGE, and the generic
// one cannot start an upgrade: post-app-v8 the governance-propose path refuses
// OpUpgrade outright ("must be created via UpgradePropose, not GovPropose") —
// the guard that stops app-version activation riding ordinary proposals. That
// is why CEREBRUM could list and vote on governance ballots but could not
// initiate an app upgrade at all. These two routes close that gap by speaking
// the dedicated UpgradePropose transaction, which is the same thing
// `sage-gui upgrade propose` sends.

const upgradeGovernanceStatusSchema = "sage-upgrade-governance-status/v1"

// RegisterUpgradeRoutes registers the app-version upgrade routes.
func (h *DashboardHandler) RegisterUpgradeRoutes(r interface {
	Get(string, http.HandlerFunc)
	Post(string, http.HandlerFunc)
}) {
	r.Get("/v1/dashboard/governance/upgrade-status", h.handleUpgradeStatus)
	r.Post("/v1/dashboard/governance/upgrade-propose", h.handleUpgradePropose)
}

// handleUpgradeStatus reports the authoritative upgrade governance status plus
// the two ceilings an operator has to reason about: what this binary can
// execute (compiled) and how far its auto-voter goes on its own. A target above
// the auto-vote ceiling is deliberately dormant, and the response says so —
// that is exactly the state in which an explicit vote is the only thing that
// can carry the plan.
func (h *DashboardHandler) handleUpgradeStatus(w http.ResponseWriter, r *http.Request) {
	status, err := readUpgradeStatusViaABCI(r.Context(), h.CometBFTRPC)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "read upgrade governance status: "+err.Error())
		return
	}
	maxCompiled := sageabci.MaxCompiledAppVersion()
	maxSupported := sageabci.MaxSupportedAppVersion()
	nextTarget := status.CurrentAppVersion + 1
	response := map[string]any{
		"status":                     status,
		"next_target_app_version":    nextTarget,
		"proposable":                 nextTarget <= maxCompiled && status.PendingPlan == nil,
		"auto_vote_ceiling":          maxSupported,
		"compiled_app_version":       maxCompiled,
		"dormant_below_ceiling_gap":  maxCompiled > maxSupported,
		"explicit_vote_required_now": status.ActiveProposal != nil && status.ActiveProposal.TargetAppVersion != nil && *status.ActiveProposal.TargetAppVersion > maxSupported,
	}
	if maxCompiled > maxSupported {
		response["dormant_note"] = fmt.Sprintf(
			"app-v%d is compiled but dormant: every node's upgrade auto-voter abstains on it until the readiness ceiling (app-v%d) is raised in the release that carries its activation evidence, so this plan can only reach quorum through explicit votes.",
			maxCompiled, maxSupported,
		)
	}
	writeJSONResp(w, http.StatusOK, response)
}

// handleUpgradePropose submits the dedicated UpgradePropose transaction.
//
// The target defaults to the chain's next rung and is refused if it is not
// exactly current+1, because the app activates gates one at a time: jumping
// would activate only the named fork and permanently strand the rungs below it.
func (h *DashboardHandler) handleUpgradePropose(w http.ResponseWriter, r *http.Request) {
	var appV23Actor *appV23ControlActor
	if h.appV23IsActive() {
		var ok bool
		appV23Actor, ok = h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
	} else if !h.requireDashboardGovernanceOperator(w, r) {
		return
	}
	if h.CometBFTRPC == "" || len(h.SigningKey) != ed25519.PrivateKeySize {
		writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
		return
	}

	var req struct {
		TargetAppVersion uint64 `json:"target_app_version,omitempty"`
		Reason           string `json:"reason,omitempty"`
	}
	rawBody, err := readDashboardGovernanceBody(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if len(rawBody) != 0 {
		if err = json.Unmarshal(rawBody, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}
	}

	status, err := readUpgradeStatusViaABCI(r.Context(), h.CometBFTRPC)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "read upgrade governance status: "+err.Error())
		return
	}
	if status.PendingPlan != nil {
		writeError(w, http.StatusConflict, fmt.Sprintf(
			"an upgrade plan is already pending (%s, target app-v%d, activation height %d); wait for it to activate before proposing another",
			status.PendingPlan.Name, status.PendingPlan.TargetAppVersion, status.PendingPlan.ActivationHeight,
		))
		return
	}

	target := req.TargetAppVersion
	if target == 0 {
		target = status.CurrentAppVersion + 1
	}
	if target <= status.CurrentAppVersion {
		writeError(w, http.StatusConflict, fmt.Sprintf(
			"the chain is already at app-v%d: a target of app-v%d would regress or no-op (the on-chain version-regression guard rejects it)",
			status.CurrentAppVersion, target,
		))
		return
	}
	if target != status.CurrentAppVersion+1 {
		writeError(w, http.StatusConflict, fmt.Sprintf(
			"forks activate one at a time: the chain is at app-v%d, so the next proposition must target app-v%d. Jumping to app-v%d would activate only that fork and permanently strand app-v%d",
			status.CurrentAppVersion, status.CurrentAppVersion+1, target, status.CurrentAppVersion+1,
		))
		return
	}
	if target > sageabci.MaxCompiledAppVersion() {
		writeError(w, http.StatusConflict, fmt.Sprintf(
			"app-v%d is above the highest app version this binary has a compiled gate for (app-v%d); upgrade the binary first",
			target, sageabci.MaxCompiledAppVersion(),
		))
		return
	}

	pub, ok := h.SigningKey.Public().(ed25519.PublicKey)
	if !ok {
		writeError(w, http.StatusInternalServerError, "signing key has no usable ed25519 public key")
		return
	}
	proposeTx := &tx.ParsedTx{
		Type:      tx.TxTypeUpgradePropose,
		Timestamp: time.Now(),
		UpgradePropose: &tx.UpgradePropose{
			Name:               tx.CanonicalUpgradeName(target),
			TargetAppVersion:   target,
			ProposerID:         hex.EncodeToString(pub),
			UpgradeDelayBlocks: 0, // the chain applies its own 200-block floor
		},
	}

	// Same authorization shape the dashboard's other governance writes use: a
	// consensus-timed governance proof binds this request to the operator (or
	// the current local Root), and the app-v23 elevation binds it to the acting
	// authority when access control is active.
	prepare := func(ptx *tx.ParsedTx) error {
		embedDashboardAgentProof(ptx, h.SigningKey)
		if h.AppV20ActiveFn != nil && h.AppV20ActiveFn() {
			operatorKey := h.AdminSigningKey
			if appV23Actor != nil {
				operatorKey = appV23Actor.Key
			}
			if len(operatorKey) != ed25519.PrivateKeySize {
				return errors.New("governance operator key not configured")
			}
			if proofErr := h.embedConsensusTimedGovernanceProof(
				ptx, operatorKey, r.Method, r.URL.RequestURI(), rawBody,
			); proofErr != nil {
				return proofErr
			}
		}
		if appV23Actor != nil {
			return h.appV23AttachElevation(ptx, appV23Actor)
		}
		return nil
	}
	txHash, _, _, err := h.signAndBroadcastCommitPrepared(r.Context(), proposeTx, h.SigningKey, prepare)
	if err != nil {
		writeError(w, http.StatusConflict, "upgrade propose failed: "+err.Error())
		return
	}

	activationHeight := int64(0)
	if refreshed, statusErr := readUpgradeStatusViaABCI(r.Context(), h.CometBFTRPC); statusErr == nil && refreshed.PendingPlan != nil {
		activationHeight = refreshed.PendingPlan.ActivationHeight
	}
	dormant := target > sageabci.MaxSupportedAppVersion()
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"tx_hash":           txHash,
		"name":              tx.CanonicalUpgradeName(target),
		"target_app_version": target,
		"activation_height": activationHeight,
		"dormant":           dormant,
		"vote_required":     dormant,
		"reason":            strings.TrimSpace(req.Reason),
	})
}

// readUpgradeStatusViaABCI reads the same authoritative query the CLI uses, so
// the dashboard and the CLI can never disagree about the chain's rung, the
// pending plan or the active ballot.
func readUpgradeStatusViaABCI(ctx context.Context, cometRPC string) (*sageabci.UpgradeGovernanceStatus, error) {
	if strings.TrimSpace(cometRPC) == "" {
		return nil, errors.New("CometBFT RPC endpoint is not configured")
	}
	queryURL := strings.TrimRight(cometRPC, "/") + "/abci_query?path=" + url.QueryEscape(`"/upgrade/governance-status"`)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("abci_query: HTTP %d", resp.StatusCode)
	}
	var envelope struct {
		Result struct {
			Response struct {
				Code  int    `json:"code"`
				Log   string `json:"log"`
				Value string `json:"value"`
			} `json:"response"`
		} `json:"result"`
	}
	if decodeErr := json.NewDecoder(resp.Body).Decode(&envelope); decodeErr != nil {
		return nil, fmt.Errorf("decode abci_query envelope: %w", decodeErr)
	}
	if envelope.Result.Response.Code != 0 {
		return nil, fmt.Errorf("abci_query code %d: %s", envelope.Result.Response.Code, envelope.Result.Response.Log)
	}
	raw, err := base64.StdEncoding.DecodeString(envelope.Result.Response.Value)
	if err != nil {
		return nil, fmt.Errorf("decode abci_query value: %w", err)
	}
	var status sageabci.UpgradeGovernanceStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, fmt.Errorf("decode upgrade governance status: %w", err)
	}
	if status.Schema != upgradeGovernanceStatusSchema {
		return nil, fmt.Errorf("unsupported upgrade governance status schema %q", status.Schema)
	}
	if status.CurrentAppVersion == 0 {
		return nil, errors.New("upgrade governance status returned zero current_app_version")
	}
	return &status, nil
}
