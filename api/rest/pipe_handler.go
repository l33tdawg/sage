package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

type federatedPipeTargetResolver interface {
	ResolveRemotePipeTarget(context.Context, string) (*federation.RemotePipeTarget, error)
}

type federatedPipeTransportNudger interface {
	NudgePipelineTransport()
}

type federatedPipeAdmissionAuthorizer interface {
	AuthorizeImportedPipe(context.Context, *store.PipelineMessage) error
	WithAuthorizedImportedPipe(context.Context, *store.PipelineMessage, func() error) error
}

const (
	// pipeQuotaProblemType marks the 429 returned when a requester (or the node
	// as a whole) has too many open pipes — kept distinct from the rate
	// limiter's status-derived 429 so clients can tell a storage quota breach
	// ("you have too many pipes in flight") from request-rate shaping.
	pipeQuotaProblemType = "https://sage.dev/errors/pipe-quota"
	// pipeTooLargeProblemType marks the 413 returned when a pipe payload,
	// intent, or result exceeds its size cap.
	pipeTooLargeProblemType = "https://sage.dev/errors/pipe-too-large"
)

// handlePipeResolve turns a human-friendly local name/provider or visible
// federated handle into the exact fields the agent must sign on /pipe/send.
// Resolution carries no work payload and never queues anything.
func (s *Server) handlePipeResolve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To string `json:"to"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	target := strings.TrimSpace(req.To)
	if target == "" {
		writeProblem(w, http.StatusBadRequest, "Missing target", "to is required")
		return
	}

	resolveRemote := func() (*federation.RemotePipeTarget, bool) {
		resolver, ok := s.federation.(federatedPipeTargetResolver)
		if !ok {
			if isQualifiedFederatedPipeTarget(target) {
				writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "This node does not support cross-network agent work yet.")
				return nil, true
			}
			return nil, false
		}
		resolved, err := resolver.ResolveRemotePipeTarget(r.Context(), target)
		if err != nil {
			if !errors.Is(err, federation.ErrRemotePipeTargetNotFound) || isQualifiedFederatedPipeTarget(target) {
				s.writeRemotePipeTargetError(w, err)
				return nil, true
			}
			return nil, false
		}
		return resolved, true
	}
	writeRemote := func(remote *federation.RemotePipeTarget) {
		writeJSON(w, http.StatusOK, map[string]any{
			"to_agent": remote.AgentID, "to_provider": "", "destination_chain_id": remote.ChainID,
			"source_chain_id": s.federation.LocalChainID(),
			"address":         remote.Address, "handle": remote.Handle, "display_name": remote.DisplayName,
		})
	}

	if isQualifiedFederatedPipeTarget(target) {
		if remote, handled := resolveRemote(); remote != nil {
			writeRemote(remote)
		} else if !handled {
			writeProblem(w, http.StatusNotFound, "Unknown target", fmt.Sprintf("no visible federated agent matches %q", target))
		}
		return
	}
	if s.agentStore != nil {
		if pub, err := auth.AgentIDToPublicKey(target); err == nil && auth.PublicKeyToAgentID(pub) == target {
			if _, err := s.agentStore.GetAgent(r.Context(), target); err == nil {
				writeJSON(w, http.StatusOK, map[string]any{"to_agent": target, "to_provider": "", "destination_chain_id": ""})
				return
			}
		} else {
			agents, err := s.agentStore.ListAgents(r.Context())
			if err != nil {
				writeProblem(w, http.StatusInternalServerError, "Target resolution failed", "Local agent registry is unavailable.")
				return
			}
			for _, agent := range agents {
				if agent != nil && agent.Provider == target {
					writeJSON(w, http.StatusOK, map[string]any{"to_agent": "", "to_provider": target, "destination_chain_id": ""})
					return
				}
			}
			if agent, err := s.agentStore.GetAgentByName(r.Context(), target); err == nil && agent != nil {
				writeJSON(w, http.StatusOK, map[string]any{"to_agent": agent.AgentID, "to_provider": "", "destination_chain_id": ""})
				return
			}
		}
	}
	if remote, handled := resolveRemote(); remote != nil {
		writeRemote(remote)
		return
	} else if handled {
		return
	}
	writeProblem(w, http.StatusNotFound, "Unknown target", fmt.Sprintf("no registered local or visible federated agent matches %q", target))
}

// handlePipeSend creates a pipeline message addressed to another agent/provider.
func (s *Server) handlePipeSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ToAgent            string `json:"to_agent"`
		ToProvider         string `json:"to_provider"`
		SourceChainID      string `json:"source_chain_id"`
		DestinationChainID string `json:"destination_chain_id"`
		Intent             string `json:"intent"`
		Payload            string `json:"payload"`
		TTLMinutes         int    `json:"ttl_minutes"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Payload == "" {
		writeProblem(w, http.StatusBadRequest, "Missing payload", "payload is required")
		return
	}
	if req.ToAgent == "" && req.ToProvider == "" {
		writeProblem(w, http.StatusBadRequest, "Missing target", "to_agent or to_provider is required")
		return
	}
	// Size caps (E8c) — fast-fail before the store; the store re-checks so the
	// MCP and dashboard paths are covered too.
	if len(req.Payload) > store.MaxPipeContentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Payload too large", fmt.Sprintf("payload exceeds the %d byte limit.", store.MaxPipeContentBytes))
		return
	}
	if len(req.Intent) > store.MaxPipeIntentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Intent too large", fmt.Sprintf("intent exceeds the %d byte limit.", store.MaxPipeIntentBytes))
		return
	}

	var remoteTarget *federation.RemotePipeTarget
	qualifiedTarget := ""
	if req.DestinationChainID != "" {
		if req.ToAgent == "" || req.ToProvider != "" || req.SourceChainID == "" {
			writeProblem(w, http.StatusBadRequest, "Invalid remote target", "destination_chain_id requires exact source_chain_id and to_agent with no to_provider")
			return
		}
		if s.federation == nil || req.SourceChainID != s.federation.LocalChainID() {
			writeProblem(w, http.StatusConflict, "Federated source changed", "Resolve the federated target again before sending.")
			return
		}
		qualifiedTarget = req.ToAgent + "@" + req.DestinationChainID
	} else if isQualifiedFederatedPipeTarget(req.ToProvider) {
		writeProblem(w, http.StatusBadRequest, "Federated target must be resolved", "Resolve the friendly handle first, then sign the exact to_agent and destination_chain_id returned by /v1/pipe/resolve.")
		return
	}
	if qualifiedTarget != "" {
		resolver, ok := s.federation.(federatedPipeTargetResolver)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "This node does not support cross-network agent work yet.")
			return
		}
		var err error
		remoteTarget, err = resolver.ResolveRemotePipeTarget(r.Context(), qualifiedTarget)
		if err != nil {
			s.writeRemotePipeTargetError(w, err)
			return
		}
		req.ToAgent = remoteTarget.AgentID
		req.ToProvider = ""
		req.DestinationChainID = remoteTarget.ChainID
	}

	// Validate the local target agent/provider exactly as before. Qualified
	// remote syntax never reaches this branch and can therefore never fall back
	// to a similarly named local provider.
	if s.agentStore != nil {
		if remoteTarget != nil {
			// The authenticated federation resolver already proved the finite
			// peer contact and its default-off acceptance state.
		} else if req.ToAgent != "" {
			// Direct agent_id — must exist
			if _, err := s.agentStore.GetAgent(r.Context(), req.ToAgent); err != nil {
				writeProblem(w, http.StatusNotFound, "Unknown target agent",
					fmt.Sprintf("agent_id %s is not registered locally; federated sends require destination_chain_id", req.ToAgent))
				return
			}
		} else if req.ToProvider != "" {
			// Provider — check if any active agent has this provider
			agents, err := s.agentStore.ListAgents(r.Context())
			if err == nil {
				found := false
				for _, a := range agents {
					if a.Provider == req.ToProvider {
						found = true
						break
					}
				}
				if !found {
					// Also try as agent name
					if agent, _ := s.agentStore.GetAgentByName(r.Context(), req.ToProvider); agent != nil {
						// Resolve name → agent_id for direct delivery
						req.ToAgent = agent.AgentID
						req.ToProvider = ""
					} else {
						writeProblem(w, http.StatusNotFound, "Unknown target",
							fmt.Sprintf("no registered local agent named %q; resolve federated targets before sending", req.ToProvider))
						return
					}
				}
			}
		}
	}

	ttl := req.TTLMinutes
	if ttl <= 0 {
		ttl = 60
	}
	if ttl > 1440 {
		ttl = 1440
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up sender's provider from agent registry
	fromProvider := ""
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil {
			fromProvider = agent.Provider
		}
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	now := time.Now().UTC()
	var transportProof store.PipelineAgentProof
	if remoteTarget != nil {
		proof := middleware.ContextAgentAuth(r.Context())
		if proof == nil || len(proof.Signature) == 0 || len(proof.CanonicalRequest) == 0 || len(proof.Nonce) < 8 {
			writeProblem(w, http.StatusForbidden, "Agent signature required", "Federated work must be sent by a fresh nonce-bound agent-signed request; bearer-only or legacy nonce-less identity is not sufficient.")
			return
		}
		pub, canonicalErr := auth.AgentIDToPublicKey(agentID)
		if canonicalErr != nil || auth.PublicKeyToAgentID(pub) != agentID {
			writeProblem(w, http.StatusForbidden, "Canonical agent identity required", "Federated work requires the lowercase 64-hex form of the signing agent ID.")
			return
		}
		transportProof = store.PipelineAgentProof{
			AgentID: agentID, Signature: append([]byte(nil), proof.Signature...), Timestamp: proof.Timestamp,
			Nonce: append([]byte(nil), proof.Nonce...), CanonicalRequest: append([]byte(nil), proof.CanonicalRequest...),
		}
		now = time.Unix(proof.Timestamp, 0).UTC()
	}
	msg := &store.PipelineMessage{
		PipeID:       generatePipeID(),
		FromAgent:    agentID,
		FromProvider: fromProvider,
		ToAgent:      req.ToAgent,
		ToProvider:   req.ToProvider,
		Intent:       req.Intent,
		Payload:      req.Payload,
		Status:       "pending",
		CreatedAt:    now,
		ExpiresAt:    now.Add(time.Duration(ttl) * time.Minute),
	}
	if remoteTarget != nil {
		msg.DestinationChainID = remoteTarget.ChainID
		msg.FederationPolicyEpoch = remoteTarget.PolicyEpoch
		msg.FederationAgreementID = remoteTarget.AgreementID
		msg.FederationContactID = remoteTarget.ContactID
		msg.FederationContactRevision = remoteTarget.ContactRevision
	}

	var insertErr error
	if remoteTarget != nil {
		transportStore, ok := pipeStore.(store.FederatedPipelineStore)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "The active pipeline store does not support cross-network delivery.")
			return
		}
		event := &store.PipelineTransportOutbox{
			EventID: federation.PipelineProofEventID(s.federation.LocalChainID(), "send", transportProof), PipeID: msg.PipeID,
			RemoteChainID: msg.DestinationChainID, EventKind: "send",
			PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
			ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
			SourceAgentID: msg.FromAgent, TargetAgentID: msg.ToAgent,
			Proof:     transportProof,
			CreatedAt: msg.CreatedAt, ExpiresAt: msg.ExpiresAt,
		}
		insertErr = transportStore.InsertPipelineWithTransport(r.Context(), msg, event)
	} else {
		insertErr = pipeStore.InsertPipeline(r.Context(), msg)
	}
	if insertErr != nil {
		switch {
		case errors.Is(insertErr, store.ErrPipePayloadTooLarge), errors.Is(insertErr, store.ErrPipeIntentTooLarge):
			writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType, "Pipeline content too large", insertErr.Error())
		case errors.Is(insertErr, store.ErrPipeQuotaPerAgent), errors.Is(insertErr, store.ErrPipeQuotaGlobal):
			// Storage quota, not chain backpressure: tell the caller to drain
			// their in-flight pipes before sending more.
			w.Header().Set("Retry-After", "5")
			writeProblemTyped(w, http.StatusTooManyRequests, pipeQuotaProblemType, "Too many open pipelines", insertErr.Error())
		default:
			writeProblem(w, http.StatusInternalServerError, "Pipeline insert failed", insertErr.Error())
		}
		return
	}
	if remoteTarget != nil {
		if nudger, ok := s.federation.(federatedPipeTransportNudger); ok {
			nudger.NudgePipelineTransport()
		}
	}

	if s.OnEvent != nil {
		target := req.ToProvider
		if target == "" && len(req.ToAgent) >= 16 {
			target = req.ToAgent[:16] + "..."
		}
		s.OnEvent("pipeline_send", msg.PipeID, "agent-pipeline",
			fmt.Sprintf("%s piped work to %s (intent: %s)", fromProvider, target, req.Intent), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"pipe_id":              msg.PipeID,
		"status":               msg.Status,
		"expires_at":           msg.ExpiresAt.Format(time.RFC3339),
		"destination_chain_id": msg.DestinationChainID,
	})
}

func isQualifiedFederatedPipeTarget(target string) bool {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, "#") {
		return true
	}
	at := strings.LastIndex(target, "@")
	return at == 64 && at < len(target)-1
}

func (s *Server) writeRemotePipeTargetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, federation.ErrRemotePipeTargetNotFound):
		writeProblem(w, http.StatusNotFound, "Unknown federated agent", "No visible federated agent matches that target.")
	case errors.Is(err, federation.ErrRemotePipeTargetAmbiguous):
		writeProblem(w, http.StatusConflict, "Ambiguous federated agent", err.Error())
	case errors.Is(err, federation.ErrRemotePipeTargetUnavailable):
		writeProblem(w, http.StatusConflict, "Federated agent unavailable", "That agent or connection is currently paused or unavailable.")
	case errors.Is(err, federation.ErrRemotePipeTargetNotAccepting):
		writeProblem(w, http.StatusForbidden, "Federated agent is not accepting work", "The receiving SAGE has not enabled work requests for that agent.")
	case errors.Is(err, federation.ErrRemotePipePeerUnsupported):
		writeProblem(w, http.StatusNotImplemented, "Peer update required", "The receiving SAGE does not support federated pipeline delivery.")
	case errors.Is(err, federation.ErrRemotePipeResolutionIncomplete):
		writeProblem(w, http.StatusServiceUnavailable, "Federated agent lookup incomplete", "One or more trusted SAGE peers could not be checked. Use the exact agent@chain address or try again.")
	default:
		writeProblem(w, http.StatusBadGateway, "Federated agent lookup failed", "The target could not be verified against the active federation connection.")
	}
}

// handlePipeInbox returns pending pipeline items for the authenticated agent.
func (s *Server) handlePipeInbox(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up agent's provider
	provider := ""
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil {
			provider = agent.Provider
		}
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	items, err := pipeStore.GetInbox(r.Context(), agentID, provider, limit)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Inbox query failed", err.Error())
		return
	}

	// Auto-claim returned items, but return ONLY CAS winners. Two concurrent
	// inbox reads may select the same pending row; ignoring ClaimPipeline errors
	// let both agents believe they owned it. The loser now omits that item.
	claimedItems := make([]*store.PipelineMessage, 0, len(items))
	for _, item := range items {
		if item.SourceChainID != "" {
			authorizer, ok := s.federation.(federatedPipeAdmissionAuthorizer)
			if !ok || authorizer.WithAuthorizedImportedPipe(r.Context(), item, func() error {
				return pipeStore.ClaimPipeline(r.Context(), item.PipeID, agentID)
			}) != nil {
				continue
			}
		} else if err := pipeStore.ClaimPipeline(r.Context(), item.PipeID, agentID); err != nil {
			continue
		}
		item.Status = "claimed"
		item.ClaimedBy = agentID
		claimedItems = append(claimedItems, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": claimedItems,
		"count": len(claimedItems),
	})
}

// handlePipeClaim atomically claims a pipeline item.
func (s *Server) handlePipeClaim(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")
	agentID := middleware.ContextAgentID(r.Context())

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	pipeNotFoundDetail := fmt.Sprintf("No pipeline message with id %s.", pipeID)
	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}
	if !s.callerCanClaimPipe(r.Context(), agentID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}
	if msg.SourceChainID != "" {
		authorizer, ok := s.federation.(federatedPipeAdmissionAuthorizer)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "This node cannot revalidate the foreign work request.")
			return
		}
		if err := authorizer.WithAuthorizedImportedPipe(r.Context(), msg, func() error {
			return pipeStore.ClaimPipeline(r.Context(), pipeID, agentID)
		}); err != nil {
			writeProblem(w, http.StatusConflict, "Federated pipeline suspended", "The connection, sharing grant, owner, or work-request permission changed.")
			return
		}
	} else if err := pipeStore.ClaimPipeline(r.Context(), pipeID, agentID); err != nil {
		writeProblem(w, http.StatusConflict, "Claim failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pipe_id": pipeID,
		"status":  "claimed",
	})
}

// handlePipeResult submits a result for a claimed pipeline item and triggers auto-journal.
func (s *Server) handlePipeResult(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")
	agentID := middleware.ContextAgentID(r.Context())

	var req struct {
		Result        string `json:"result"`
		SourcePipeID  string `json:"source_pipe_id"`
		SourceChainID string `json:"source_chain_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Result == "" {
		writeProblem(w, http.StatusBadRequest, "Missing result", "result is required")
		return
	}
	// Size cap (E8c) — fast-fail before the store re-checks.
	if len(req.Result) > store.MaxPipeContentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Result too large", fmt.Sprintf("result exceeds the %d byte limit.", store.MaxPipeContentBytes))
		return
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	// Use the same detail for genuinely missing and unauthorized IDs so this
	// mutation route does not become a pipeline-existence oracle.
	pipeNotFoundDetail := fmt.Sprintf("No pipeline message with id %s.", pipeID)
	// Get the pipe message for auto-journal context
	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}
	if !s.callerCanViewPipe(r.Context(), agentID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}
	if msg.SourceChainID != "" {
		if req.SourcePipeID == "" || req.SourcePipeID != msg.SourcePipeID {
			writeProblem(w, http.StatusConflict, "Federated pipeline changed", "Refresh this inbox item before returning its result.")
			return
		}
		if time.Now().UTC().After(msg.ExpiresAt) {
			writeProblem(w, http.StatusConflict, "Pipeline expired", "This work request expired before its result was submitted.")
			return
		}
		if _, ok := s.federation.(federatedPipeAdmissionAuthorizer); !ok {
			writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "This node cannot revalidate the foreign work request.")
			return
		}
		if req.SourceChainID == "" || req.SourceChainID != s.federation.LocalChainID() {
			writeProblem(w, http.StatusConflict, "Federated source changed", "Refresh this inbox item before returning its result.")
			return
		}
	} else if req.SourceChainID != "" {
		writeProblem(w, http.StatusBadRequest, "Invalid local result", "source_chain_id is reserved for federated work")
		return
	}
	if msg.ClaimedBy != agentID && (msg.ClaimedBy != "" || !s.callerCanClaimPipe(r.Context(), agentID, msg)) {
		writeProblem(w, http.StatusConflict, "Completion failed", fmt.Sprintf("pipeline message %s not available for completion by this agent", pipeID))
		return
	}

	// Foreign requests remain transient input. Never write their intent, result,
	// or previews into memory; the result transport is queued separately after
	// completion. Local pipeline behavior keeps its existing summary journal.
	journalID := ""
	summary := fmt.Sprintf("federated pipeline %s completed", pipeID)
	journaled := false
	if msg.SourceChainID == "" {
		resultPreview := req.Result
		if len(resultPreview) > 200 {
			resultPreview = resultPreview[:200] + "..."
		}

		fromName := msg.FromProvider
		if fromName == "" && len(msg.FromAgent) >= 16 {
			fromName = msg.FromAgent[:16] + "..."
		}
		toName := msg.ToProvider
		if toName == "" && len(msg.ToAgent) >= 16 {
			toName = msg.ToAgent[:16] + "..."
		}

		elapsed := ""
		if msg.ClaimedAt != nil {
			elapsed = fmt.Sprintf(" in %s", time.Since(*msg.ClaimedAt).Truncate(time.Second))
		}

		summary = fmt.Sprintf("[Pipeline] %s asked %s to %s. Result received (%d chars)%s. Preview: %s",
			fromName, toName, msg.Intent, len(req.Result), elapsed, resultPreview)
		journalID = s.autoJournalPipeline(r.Context(), summary)
		journaled = journalID != ""
	}

	// Complete the pipeline. Foreign work atomically writes the result and its
	// durable return event, so a crash cannot acknowledge locally and lose the
	// requesting agent's answer.
	var completeErr error
	if msg.SourceChainID != "" {
		transportStore, ok := pipeStore.(store.FederatedPipelineStore)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Federated pipeline unavailable", "The active pipeline store cannot return foreign results.")
			return
		}
		proof := middleware.ContextAgentAuth(r.Context())
		if proof == nil || len(proof.Signature) == 0 || len(proof.CanonicalRequest) == 0 || len(proof.Nonce) < 8 {
			writeProblem(w, http.StatusForbidden, "Agent signature required", "Federated results require a fresh nonce-bound agent-signed request.")
			return
		}
		pub, canonicalErr := auth.AgentIDToPublicKey(agentID)
		if canonicalErr != nil || auth.PublicKeyToAgentID(pub) != agentID {
			writeProblem(w, http.StatusForbidden, "Canonical agent identity required", "Federated results require the lowercase 64-hex form of the signing agent ID.")
			return
		}
		transportProof := store.PipelineAgentProof{
			AgentID: agentID, Signature: append([]byte(nil), proof.Signature...), Timestamp: proof.Timestamp,
			Nonce: append([]byte(nil), proof.Nonce...), CanonicalRequest: append([]byte(nil), proof.CanonicalRequest...),
		}
		created := time.Unix(proof.Timestamp, 0).UTC()
		event := &store.PipelineTransportOutbox{
			EventID: federation.PipelineProofEventID(s.federation.LocalChainID(), "result", transportProof), PipeID: pipeID,
			RemoteChainID: msg.SourceChainID, EventKind: "result", PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision, SourceAgentID: agentID,
			TargetAgentID: msg.FromAgent, Proof: transportProof, CreatedAt: created,
			ExpiresAt: created.Add(24 * time.Hour),
		}
		authorizer := s.federation.(federatedPipeAdmissionAuthorizer)
		completeErr = authorizer.WithAuthorizedImportedPipe(r.Context(), msg, func() error {
			return transportStore.CompleteFederatedPipelineWithTransport(r.Context(), pipeID, agentID, req.Result, event)
		})
	} else {
		completeErr = pipeStore.CompletePipeline(r.Context(), pipeID, agentID, req.Result, journalID)
	}
	if completeErr != nil {
		if errors.Is(completeErr, store.ErrPipeResultTooLarge) {
			writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType, "Result too large", completeErr.Error())
			return
		}
		writeProblem(w, http.StatusConflict, "Completion failed", completeErr.Error())
		return
	}
	if msg.SourceChainID != "" {
		if nudger, ok := s.federation.(federatedPipeTransportNudger); ok {
			nudger.NudgePipelineTransport()
		}
	}

	if s.OnEvent != nil {
		s.OnEvent("pipeline_complete", pipeID, "agent-pipeline", summary, nil)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "completed",
		"journal_id": journalID,
		"journaled":  journaled,
	})
}

// callerCanViewPipe reports whether callerID is a party to msg (sender,
// addressed recipient, or matching destination provider) or an operator/admin.
func (s *Server) callerCanViewPipe(ctx context.Context, callerID string, msg *store.PipelineMessage) bool {
	if callerID == "" || msg == nil {
		return false
	}
	if msg.SourceChainID == "" && callerID == msg.FromAgent {
		return true
	}
	if msg.DestinationChainID == "" && callerID == msg.ToAgent {
		return true
	}
	if msg.DestinationChainID == "" && msg.ToProvider != "" && s.agentStore != nil {
		if a, err := s.agentStore.GetAgent(ctx, callerID); err == nil && a != nil && a.Provider == msg.ToProvider {
			return true
		}
	}
	return s.callerIsOperatorOrAdmin(ctx, callerID)
}

// callerCanClaimPipe reports whether callerID is the addressed recipient for
// this pipe (direct agent or provider match) or an operator/admin. Senders can
// read their own pipe status/results, but they do not claim the work item.
func (s *Server) callerCanClaimPipe(ctx context.Context, callerID string, msg *store.PipelineMessage) bool {
	if callerID == "" || msg == nil {
		return false
	}
	if msg.DestinationChainID != "" {
		return false
	}
	if msg.SourceChainID != "" {
		return callerID == msg.ToAgent
	}
	if callerID == msg.ToAgent {
		return true
	}
	if msg.ToProvider != "" && msg.ToAgent == "" && s.agentStore != nil {
		if a, err := s.agentStore.GetAgent(ctx, callerID); err == nil && a != nil && a.Provider == msg.ToProvider {
			return true
		}
	}
	return s.callerIsOperatorOrAdmin(ctx, callerID)
}

// handlePipeStatus returns the current status of a pipeline message.
func (s *Server) handlePipeStatus(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	// pipeNotFoundDetail is shared by the genuine-not-found and the
	// exists-but-unauthorized branches so the two 404s are byte-for-byte
	// identical — an unrelated caller cannot distinguish "pipe exists but I'm not
	// a party" from "pipe does not exist" (the anti-enumeration goal below).
	pipeNotFoundDetail := fmt.Sprintf("No pipeline message with id %s.", pipeID)

	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}

	// Authorization: a pipe carries a private payload between two parties. Only
	// the sender, the addressed recipient (agent or provider), or an
	// operator/admin may read it. Return 404 (not 403) so the endpoint does not
	// confirm the existence of a pipe_id to an unrelated caller.
	callerID := middleware.ContextAgentID(r.Context())
	if !s.callerCanViewPipe(r.Context(), callerID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}

	replySourceChainID := ""
	if msg.SourceChainID != "" && s.federation != nil {
		replySourceChainID = s.federation.LocalChainID()
	}
	writeJSON(w, http.StatusOK, struct {
		*store.PipelineMessage
		ReplySourceChainID string `json:"reply_source_chain_id,omitempty"`
	}{PipelineMessage: msg, ReplySourceChainID: replySourceChainID})
}

// handlePipeResults returns completed pipeline items sent by this agent.
func (s *Server) handlePipeResults(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}

	agentID := middleware.ContextAgentID(r.Context())

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	items, err := pipeStore.GetCompletedForSender(r.Context(), agentID, limit)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Results query failed", err.Error())
		return
	}
	if items == nil {
		items = make([]*store.PipelineMessage, 0)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

// handlePipeUpdates atomically returns payload-free terminal delivery notices
// for federated sends/results signed by the authenticated local agent. The
// store marks returned notices reported, so sage_turn gives actionable feedback
// once instead of repeating the same peer failure on every turn.
func (s *Server) handlePipeUpdates(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}
	updatesStore, ok := s.store.(store.PipelineTransportUpdateStore)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "count": 0})
		return
	}
	updates, err := updatesStore.ListPipelineDeliveryUpdates(
		r.Context(), middleware.ContextAgentID(r.Context()), limit,
	)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Pipeline delivery updates unavailable", err.Error())
		return
	}
	if updates == nil {
		updates = make([]*store.PipelineDeliveryUpdate, 0)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": updates, "count": len(updates)})
}

// autoJournalPipeline inserts a journal entry as an observation memory directly
// into the off-chain store, bypassing CometBFT. The auto-validator goroutine
// will pick it up and commit it within seconds.
// Returns the memory_id of the journal entry (empty string on failure).
func (s *Server) autoJournalPipeline(ctx context.Context, summary string) string {
	offchain, ok := s.store.(store.OffchainStore)
	if !ok {
		s.logger.Warn().Msg("pipeline auto-journal: store does not support off-chain insert")
		return ""
	}

	memoryID := generateUUID()
	contentHash := sha256.Sum256([]byte(summary))

	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: "sage-system",
		Content:         summary,
		ContentHash:     contentHash[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       "agent-pipeline",
		Provider:        "sage-system",
		ConfidenceScore: 0.90,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}
	// Pipeline journals are inserted directly into the off-chain store rather
	// than through handleSubmitMemory. Generate and stamp their vector here so
	// semantic nodes do not accumulate false "needs fixing" counts over time.
	if s.embedder != nil {
		if emb, err := s.embedder.Embed(ctx, summary); err == nil {
			record.Embedding = emb
			record.EmbeddingProvider = s.embedderStampFor(emb)
		}
	}

	if err := offchain.InsertMemory(ctx, record); err != nil {
		s.logger.Warn().Err(err).Msg("pipeline auto-journal: failed to insert memory")
		return ""
	}

	return memoryID
}

// generatePipeID creates a random pipe ID with a "pipe-" prefix.
func generatePipeID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("pipe-%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
