package federation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	pipeDeliveryTimeout  = 20 * time.Second
	pipeRetryBase        = 5 * time.Second
	pipeRetryMax         = 2 * time.Minute
	pipeDrainLimit       = 4
	pipeDrainConcurrency = 4
)

var errFederatedPipeSourceDenied = errors.New("federated pipeline delivery is disabled for the source agent")

// NudgePipelineTransport wakes the existing federation outbox worker. Domain
// sync and pipeline transport share only the lifecycle/ticker, never their
// authorization state or storage state machines.
func (m *Manager) NudgePipelineTransport() { m.nudgeSync() }

func pipelineRetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	delay := pipeRetryBase
	for i := 0; i < attempts && delay < pipeRetryMax; i++ {
		delay *= 2
	}
	if delay > pipeRetryMax {
		return pipeRetryMax
	}
	return delay
}

func (m *Manager) pipelineDrain(ctx context.Context, ss *store.SQLiteStore) {
	if _, err := ss.PurgeExpiredPipelineTransport(ctx, time.Now().UTC()); err != nil {
		m.logger.Warn().Err(err).Msg("pipeline transport cleanup failed")
	}
	events, err := ss.ListPendingPipelineTransport(ctx, time.Now().UTC(), pipeDrainLimit)
	if err != nil {
		if !errors.Is(err, store.ErrPipeContentUnavailable) {
			m.logger.Warn().Err(err).Msg("pipeline transport outbox scan failed")
		}
		return
	}
	sem := make(chan struct{}, pipeDrainConcurrency)
	var wg sync.WaitGroup
	for _, event := range events {
		if ctx.Err() != nil {
			break
		}
		event := event
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			m.deliverPipelineEvent(ctx, ss, event)
		}()
	}
	wg.Wait()
	if len(events) == pipeDrainLimit {
		m.nudgeSync()
	}
}

func (m *Manager) receiptDrain(ctx context.Context, ss *store.SQLiteStore) {
	events, err := ss.ListPendingFederatedReceiptOutbox(ctx, time.Now().UTC(), pipeDrainLimit)
	if err != nil {
		if !errors.Is(err, store.ErrPipeContentUnavailable) {
			m.logger.Warn().Err(err).Msg("receipt-v2 outbox scan failed")
		}
		return
	}
	sem := make(chan struct{}, pipeDrainConcurrency)
	var wg sync.WaitGroup
	for _, event := range events {
		if ctx.Err() != nil {
			break
		}
		event := event
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			m.deliverReceiptEvent(ctx, ss, event)
		}()
	}
	wg.Wait()
	if len(events) == pipeDrainLimit {
		m.nudgeSync()
	}
}

func (m *Manager) deliverReceiptEvent(parent context.Context, ss *store.SQLiteStore, outbox *store.FederatedReceiptOutbox) {
	ctx, cancel := context.WithTimeout(parent, pipeDeliveryTimeout)
	defer cancel()
	if outbox == nil || m.postV26ForNextTx == nil || !m.postV26ForNextTx() {
		return
	}
	deliveryCtx, releaseDelivery, err := m.beginLinkedDelivery(ctx, outbox.SenderChainID)
	if err != nil {
		m.recordReceiptDeliveryError(ss, outbox, err, "")
		return
	}
	defer releaseDelivery()
	// Receipt evidence was already exact-recipient verified and atomically
	// admitted before it entered this outbox. It carries no payload. Do not
	// re-run mutable content authorization here: after revoke, an exact retry of
	// an already accepted receipt must remain deliverable/recognizable by the
	// sender. Mutual peer authentication still binds the delivery channel.
	_, err = m.ActiveAgreement(outbox.SenderChainID)
	if err != nil {
		m.recordReceiptDeliveryError(ss, outbox, err, "")
		return
	}
	event := &PipeReceiptEvent{
		Version: PipeReceiptVersion, EventID: outbox.EventID, MessageID: outbox.MessageID,
		RecipientPipeID: outbox.RecipientPipeID, SenderChainID: outbox.SenderChainID,
		RecipientChainID: outbox.RecipientChainID, SenderAgentID: outbox.SenderAgentID,
		RecipientAgentID: outbox.RecipientAgentID, ContentDigest: outbox.ContentDigest,
		Kind: outbox.Kind, EventAt: outbox.EventAt, PolicyEpoch: outbox.PolicyEpoch,
		AgreementID: outbox.AgreementID, ContactID: outbox.ContactID,
		ContactRevision: outbox.ContactRevision, AuthorizationMode: outbox.AuthorizationMode,
		RelationDigest: outbox.RelationDigest, Proof: outbox.Proof,
	}
	if _, err := m.PushPipeReceiptV2(deliveryCtx, outbox.SenderChainID, event); err != nil {
		terminalState := ""
		var httpErr *pipeEventHTTPError
		if errors.As(err, &httpErr) {
			switch httpErr.Status {
			case http.StatusNotFound, http.StatusNotImplemented:
				terminalState = "unsupported"
			case http.StatusBadRequest, http.StatusForbidden, http.StatusConflict,
				http.StatusGone, http.StatusUnprocessableEntity:
				terminalState = "failed"
			}
		}
		m.recordReceiptDeliveryError(ss, outbox, err, terminalState)
		return
	}
	if err := ss.MarkFederatedReceiptOutboxDelivered(context.Background(), outbox.EventID); err != nil {
		m.logger.Warn().Err(err).Str("event_id", outbox.EventID).Msg("receipt-v2 delivery was not recorded")
	}
}

func (m *Manager) recordReceiptDeliveryError(
	ss *store.SQLiteStore, event *store.FederatedReceiptOutbox, deliveryErr error, terminalState string,
) {
	if event == nil {
		return
	}
	delay := pipelineRetryDelay(event.Attempts)
	if terminalState == "" && time.Now().UTC().Add(delay).After(event.ExpiresAt) {
		terminalState = "failed"
	}
	if err := ss.RecordFederatedReceiptOutboxFailure(context.Background(), event.EventID,
		deliveryErr.Error(), time.Now().UTC().Add(delay), terminalState); err != nil {
		m.logger.Warn().Err(err).Str("event_id", event.EventID).Msg("receipt-v2 delivery failure was not recorded")
	}
}

func (m *Manager) deliverPipelineEvent(parent context.Context, ss *store.SQLiteStore, outbox *store.PipelineTransportOutbox) {
	ctx, cancel := context.WithTimeout(parent, pipeDeliveryTimeout)
	defer cancel()
	if outbox != nil &&
		outbox.AuthorizationMode == LinkedMessageAuthorizationMode {
		deliveryCtx, releaseDelivery, leaseErr := m.beginLinkedDelivery(
			ctx, outbox.RemoteChainID,
		)
		if leaseErr != nil {
			m.recordPipelineDeliveryError(
				ss, outbox, leaseErr, false, time.Duration(0),
			)
			return
		}
		defer releaseDelivery()
		ctx = deliveryCtx
	}
	// Revoke, pause and policy replacement take the write side. For outbound
	// sends the final contact resolution, network acknowledgement, and durable
	// delivery outcome are one read-leased operation: once revoke returns, no
	// payload from its retired generation can still be in flight.
	policyUnlock := func() {}
	ownerUnlock := func() {}
	contactUnlock := func() {}
	if outbox != nil && outbox.EventKind == "send" &&
		outbox.AuthorizationMode != LinkedMessageAuthorizationMode {
		policyUnlock = ss.LockSyncPolicyRead()
		if m.badger != nil {
			ownerUnlock = m.badger.LockDomainOwnershipRead()
		}
		contactUnlock = ss.LockAgentContactRead()
	}
	defer policyUnlock()
	defer ownerUnlock()
	defer contactUnlock()
	event, terminal, err := m.buildPipelineEvent(ctx, ss, outbox)
	if err != nil {
		retryFloor := time.Duration(0)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			terminal = false
		}
		if errors.Is(err, store.ErrPipeContentUnavailable) {
			terminal = false
			retryFloor = 2 * time.Minute
		}
		m.recordPipelineDeliveryError(ss, outbox, err, terminal, retryFloor)
		return
	}
	push := m.pipeEventPushFn
	if push == nil {
		push = m.PushPipeEvent
	}
	deliver := func() error {
		_, pushErr := push(ctx, outbox.RemoteChainID, event)
		return pushErr
	}
	deliveryRecorded := false
	if outbox.EventKind == "result" {
		msg, getErr := ss.GetPipeline(ctx, outbox.PipeID)
		if getErr != nil {
			err = getErr
		} else if outbox.AuthorizationMode == LinkedMessageAuthorizationMode {
			// Linked-v23 never performs peer status, relation callback, event
			// delivery, or acknowledgement while holding local policy/owner/
			// contact leases. The destination repeats the live relation check
			// at admission, so a revoke that wins after this staged local check
			// still rejects the result before it is applied.
			err = m.AuthorizeImportedPipe(ctx, msg)
			if err == nil && !m.sourceMayUseFederatedPipe(outbox.SourceAgentID) {
				err = errFederatedPipeSourceDenied
			}
			if err == nil {
				preflight := m.pipeResultPreflightFn
				if preflight == nil {
					preflight = m.preflightPipelineResultPeer
				}
				err = preflight(ctx, msg, outbox)
			}
			if err == nil {
				if hook := m.linkedResultBeforePushHook; hook != nil {
					hook(outbox.RemoteChainID)
				}
				if ctx.Err() != nil {
					err = ctx.Err()
				} else {
					// Repeat receiver-local consent, enrollment, capability and
					// relation checks after the no-payload peer handshake. The
					// cancellable per-peer delivery lease linearizes this final
					// gate with every participating control mutation.
					err = m.AuthorizeImportedPipe(ctx, msg)
				}
			}
			if err == nil {
				event.Result = msg.Result
				err = deliver()
				event.Result = ""
			}
			if err == nil {
				err = ss.MarkPipelineTransportDelivered(
					context.Background(), outbox.EventID,
				)
				deliveryRecorded = err == nil
			}
		} else {
			err = m.WithAuthorizedImportedPipe(ctx, msg, func() error {
				if !m.sourceMayUseFederatedPipe(outbox.SourceAgentID) {
					return errFederatedPipeSourceDenied
				}
				preflight := m.pipeResultPreflightFn
				if preflight == nil {
					preflight = m.preflightPipelineResultPeer
				}
				if preflightErr := preflight(ctx, msg, outbox); preflightErr != nil {
					return preflightErr
				}
				// Result bytes are attached only after the fresh authenticated status
				// preflight. Clear the transient envelope copy after this attempt.
				event.Result = msg.Result
				defer func() { event.Result = "" }()
				if pushErr := deliver(); pushErr != nil {
					return pushErr
				}
				// Keep peer acceptance and the durable local outcome inside the
				// same policy lease. Otherwise revoke could terminalize this still-
				// pending event after the peer accepted it but before this CAS,
				// producing a false "result was not received" notice.
				if markErr := ss.MarkPipelineTransportDelivered(context.Background(), outbox.EventID); markErr != nil {
					return fmt.Errorf("record accepted pipeline result delivery: %w", markErr)
				}
				deliveryRecorded = true
				return nil
			})
		}
	} else {
		err = deliver()
	}
	if err == nil {
		if deliveryRecorded {
			return
		}
		if markErr := ss.MarkPipelineTransportDelivered(context.Background(), outbox.EventID); markErr != nil {
			m.logger.Warn().Err(markErr).Str("event_id", outbox.EventID).Msg("pipeline transport delivery was not recorded")
			return
		}
		if outbox.EventKind == "send" && outbox.ReceiptProtocolVersion == PipeReceiptVersion {
			binding := store.FederatedReceiptBinding{
				MessageID: outbox.EventID, LocalPipeID: outbox.PipeID,
				SenderChainID: m.localChainID, RecipientChainID: outbox.RemoteChainID,
				SenderAgentID: outbox.SourceAgentID, RecipientAgentID: outbox.TargetAgentID,
				ContentDigest: event.ReceiptContentDigest, PolicyEpoch: outbox.PolicyEpoch,
				AgreementID: outbox.AgreementID, ContactID: outbox.ContactID,
				ContactRevision: outbox.ContactRevision, AuthorizationMode: outbox.AuthorizationMode,
				RelationDigest: linkedMessageRelationDigest(event.LinkedRelation),
			}
			if _, receiptErr := ss.RecordFederatedReceiptDelivery(context.Background(), binding, time.Now().UTC()); receiptErr != nil {
				m.logger.Warn().Err(receiptErr).Str("event_id", outbox.EventID).
					Msg("receipt-v2 peer admission was not recorded")
			}
		}
		return
	}
	terminal = false
	retryFloor := time.Duration(0)
	var httpErr *pipeEventHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Status {
		case http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound,
			http.StatusConflict, http.StatusGone, http.StatusRequestEntityTooLarge,
			http.StatusUnprocessableEntity:
			terminal = true
		case http.StatusNotImplemented:
			retryFloor = time.Hour
		}
	}
	m.recordPipelineDeliveryError(ss, outbox, err, terminal, retryFloor)
}

func (m *Manager) recordPipelineDeliveryError(ss *store.SQLiteStore, event *store.PipelineTransportOutbox, deliveryErr error, terminal bool, retryFloor time.Duration) {
	delay := pipelineRetryDelay(event.Attempts)
	if retryFloor > delay {
		delay = retryFloor
	}
	if time.Now().UTC().Add(delay).After(event.ExpiresAt) {
		terminal = true
	}
	if err := ss.RecordPipelineTransportFailure(context.Background(), event.EventID, deliveryErr.Error(), time.Now().UTC().Add(delay), terminal); err != nil {
		m.logger.Warn().Err(err).Str("event_id", event.EventID).Msg("pipeline transport failure was not recorded")
	}
	if terminal && event.EventKind == "send" && event.ReceiptProtocolVersion == PipeReceiptVersion {
		msg, err := ss.GetPipeline(context.Background(), event.PipeID)
		if err != nil {
			return
		}
		kind := "failed"
		if !time.Now().UTC().Before(event.ExpiresAt) {
			kind = "expired"
		} else if errors.Is(deliveryErr, ErrFederatedPipeInvalid) {
			kind = "revoked"
		}
		binding := store.FederatedReceiptBinding{
			MessageID: event.EventID, LocalPipeID: event.PipeID,
			SenderChainID: m.localChainID, RecipientChainID: event.RemoteChainID,
			SenderAgentID: event.SourceAgentID, RecipientAgentID: event.TargetAgentID,
			ContentDigest: pipeReceiptContentDigest(event.EventID, m.localChainID, event.RemoteChainID, msg),
			PolicyEpoch:   event.PolicyEpoch, AgreementID: event.AgreementID,
			ContactID: event.ContactID, ContactRevision: event.ContactRevision,
			AuthorizationMode: event.AuthorizationMode,
			RelationDigest:    pipelineStoredRelationDigest(event.LinkedRelation),
		}
		if _, err := ss.RecordFederatedReceiptTerminal(context.Background(), binding, kind, time.Now().UTC()); err != nil {
			m.logger.Warn().Err(err).Str("event_id", event.EventID).Msg("receipt-v2 terminal state was not recorded")
		}
	}
}

// preflightPipelineResultPeer proves the remote SAGE is live and still bound to
// the exact agreement/operator/CA/policy generation before result bytes are
// attached to an outbound event. Receiver-side admission remains defense in
// depth; this gate prevents stale payload disclosure at the sender.
func (m *Manager) preflightPipelineResultPeer(ctx context.Context, msg *store.PipelineMessage, outbox *store.PipelineTransportOutbox) error {
	if msg == nil || outbox == nil || msg.SourceChainID != outbox.RemoteChainID ||
		msg.FederationPolicyEpoch != outbox.PolicyEpoch {
		return ErrFederatedPipeInvalid
	}
	agreement, err := m.ActiveAgreement(outbox.RemoteChainID)
	if err != nil {
		return ErrFederatedPipeInvalid
	}
	ss := m.syncStore()
	if ss == nil {
		return ErrFederatedPipeInvalid
	}
	control, err := ss.GetSyncControl(ctx, outbox.RemoteChainID)
	if err != nil || control == nil || control.PolicyEpoch != outbox.PolicyEpoch ||
		!m.syncControlPeerBound(control, &peerIdentity{
			ChainID: outbox.RemoteChainID, AgentID: control.PeerAgentID, Agreement: agreement,
		}) {
		return ErrFederatedPipeInvalid
	}
	status, err := m.fetchPeerStatus(ctx, agreement)
	if err != nil {
		return errors.Join(ErrRemotePipeResolutionIncomplete, err)
	}
	if !hasFederatedPipelineCapability(status) {
		return ErrRemotePipePeerUnsupported
	}
	currentAgreement, err := m.ActiveAgreement(outbox.RemoteChainID)
	if err != nil || !sameAgreementGeneration(agreement, currentAgreement) {
		return ErrFederatedPipeInvalid
	}
	currentControl, err := ss.GetSyncControl(ctx, outbox.RemoteChainID)
	if err != nil || !sameRemotePipeContactBinding(control, currentControl) {
		return ErrFederatedPipeInvalid
	}
	return nil
}

func (m *Manager) buildPipelineEvent(ctx context.Context, ss *store.SQLiteStore, outbox *store.PipelineTransportOutbox) (*PipeEvent, bool, error) {
	if outbox == nil {
		return nil, true, fmt.Errorf("pipeline transport event is nil")
	}
	msg, err := ss.GetPipeline(ctx, outbox.PipeID)
	if err != nil {
		return nil, true, fmt.Errorf("load pipeline transport source: %w", err)
	}
	sourceAgent, err := ss.GetAgent(ctx, outbox.SourceAgentID)
	if err != nil || sourceAgent == nil || sourceAgent.Status != "active" || sourceAgent.RemovedAt != nil {
		return nil, true, fmt.Errorf("pipeline source agent is not active on this SAGE")
	}
	if !m.sourceMayUseFederatedPipe(outbox.SourceAgentID) {
		// Keep the event pending: the capability is an operator-controlled kill
		// switch and may be deliberately re-enabled before the event expires.
		// Most importantly, reject before intent/payload/result bytes are copied
		// into the outbound envelope or any peer resolution/network call occurs.
		return nil, false, errFederatedPipeSourceDenied
	}
	event := &PipeEvent{
		Version: PipeEventVersion, EventID: outbox.EventID, Kind: outbox.EventKind,
		SourceChainID: m.localChainID, DestinationChainID: outbox.RemoteChainID,
		SourceAgentID: outbox.SourceAgentID, TargetAgentID: outbox.TargetAgentID,
		CreatedAt: outbox.CreatedAt, ExpiresAt: outbox.ExpiresAt,
		PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
		ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
		AuthorizationMode:      outbox.AuthorizationMode,
		ReceiptProtocolVersion: outbox.ReceiptProtocolVersion,
		Proof:                  outbox.Proof,
	}
	if outbox.AuthorizationMode == LinkedMessageAuthorizationMode {
		event.LinkedRelation, err = decodeLinkedMessageRelation(outbox.LinkedRelation)
		if err != nil {
			return nil, true, ErrFederatedPipeInvalid
		}
	} else if len(outbox.LinkedRelation) != 0 {
		return nil, true, ErrFederatedPipeInvalid
	}
	if event.EventID != PipelineProofEventID(event.SourceChainID, event.Kind, event.Proof) {
		return nil, true, fmt.Errorf("pipeline outbox event id no longer matches its agent proof")
	}
	switch outbox.EventKind {
	case "send":
		if msg.SourceChainID != "" || msg.DestinationChainID != outbox.RemoteChainID || msg.Status != "pending" ||
			msg.FromAgent != outbox.SourceAgentID || msg.ToAgent != outbox.TargetAgentID {
			return nil, true, fmt.Errorf("outbound pipeline state no longer matches the send event")
		}
		var target *RemotePipeTarget
		var resolveErr error
		if outbox.AuthorizationMode == LinkedMessageAuthorizationMode {
			target, resolveErr = m.ResolveRemoteLinkedPipeTarget(
				ctx, outbox.SourceAgentID,
				msg.ToAgent+"@"+msg.DestinationChainID,
			)
		} else {
			resolve := m.pipeTargetResolveFn
			if resolve == nil {
				resolve = m.resolveRemotePipeTargetLive
			}
			target, resolveErr = resolve(ctx, msg.ToAgent+"@"+msg.DestinationChainID)
		}
		if resolveErr != nil {
			switch {
			case errors.Is(resolveErr, ErrRemotePipeTargetUnavailable), errors.Is(resolveErr, ErrRemotePipeTargetNotAccepting),
				errors.Is(resolveErr, ErrRemotePipeResolutionIncomplete), errors.Is(resolveErr, ErrRemotePipePeerUnsupported):
				return nil, false, resolveErr
			default:
				return nil, true, resolveErr
			}
		}
		if target.PolicyEpoch != outbox.PolicyEpoch || target.AgreementID != outbox.AgreementID ||
			target.ContactID != outbox.ContactID || target.ContactRevision != outbox.ContactRevision ||
			target.AgentID != outbox.TargetAgentID || target.ChainID != outbox.RemoteChainID ||
			target.AuthorizationMode != outbox.AuthorizationMode ||
			!linkedMessageRelationEqual(target.LinkedRelation, event.LinkedRelation) ||
			target.ReceiptProtocolVersion != outbox.ReceiptProtocolVersion {
			return nil, true, ErrFederatedPipeInvalid
		}
		event.Intent, event.Payload = msg.Intent, msg.Payload
		if outbox.ReceiptProtocolVersion == PipeReceiptVersion {
			event.ReceiptContentDigest = pipeReceiptContentDigest(
				event.EventID, event.SourceChainID, event.DestinationChainID, msg,
			)
		} else if outbox.ReceiptProtocolVersion != 0 {
			return nil, true, ErrFederatedPipeInvalid
		}
	case "result":
		if msg.SourceChainID != outbox.RemoteChainID || msg.DestinationChainID != "" || msg.Status != "completed" ||
			msg.ToAgent != outbox.SourceAgentID || msg.FromAgent != outbox.TargetAgentID || msg.SourcePipeID == "" {
			return nil, true, fmt.Errorf("foreign pipeline state no longer matches the result event")
		}
		if authErr := m.AuthorizeImportedPipe(ctx, msg); authErr != nil {
			return nil, errors.Is(authErr, ErrFederatedPipeInvalid), authErr
		}
		event.OriginEventID = msg.SourcePipeID
		event.SourcePipeID = msg.PipeID
	default:
		return nil, true, fmt.Errorf("unsupported pipeline outbox kind %q", outbox.EventKind)
	}
	return event, false, nil
}

func (m *Manager) sourceMayUseFederatedPipe(agentID string) bool {
	if m.postV22ForNextTx == nil || !m.postV22ForNextTx() {
		return true
	}
	if m.badger == nil || agentID == "" {
		return false
	}
	capabilities, registered, err := m.badger.GetRegisteredAgentCapabilities(agentID)
	return err == nil && registered &&
		!capabilities.Has(store.AgentCapabilityDenyFederatedPipe)
}
