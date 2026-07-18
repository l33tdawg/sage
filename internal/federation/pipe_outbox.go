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

func (m *Manager) deliverPipelineEvent(parent context.Context, ss *store.SQLiteStore, outbox *store.PipelineTransportOutbox) {
	ctx, cancel := context.WithTimeout(parent, pipeDeliveryTimeout)
	defer cancel()
	// Revoke, pause and policy replacement take the write side. For outbound
	// sends the final contact resolution, network acknowledgement, and durable
	// delivery outcome are one read-leased operation: once revoke returns, no
	// payload from its retired generation can still be in flight.
	policyUnlock := func() {}
	contactUnlock := func() {}
	if outbox != nil && outbox.EventKind == "send" {
		policyUnlock = ss.LockSyncPolicyRead()
		contactUnlock = ss.LockAgentContactRead()
	}
	defer policyUnlock()
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
		} else {
			err = m.WithAuthorizedImportedPipe(ctx, msg, func() error {
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
	event := &PipeEvent{
		Version: PipeEventVersion, EventID: outbox.EventID, Kind: outbox.EventKind,
		SourceChainID: m.localChainID, DestinationChainID: outbox.RemoteChainID,
		SourceAgentID: outbox.SourceAgentID, TargetAgentID: outbox.TargetAgentID,
		CreatedAt: outbox.CreatedAt, ExpiresAt: outbox.ExpiresAt,
		PolicyEpoch: outbox.PolicyEpoch, AgreementID: outbox.AgreementID,
		ContactID: outbox.ContactID, ContactRevision: outbox.ContactRevision,
		Proof: outbox.Proof,
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
		resolve := m.pipeTargetResolveFn
		if resolve == nil {
			resolve = m.resolveRemotePipeTargetLive
		}
		target, resolveErr := resolve(ctx, msg.ToAgent+"@"+msg.DestinationChainID)
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
			target.AgentID != outbox.TargetAgentID || target.ChainID != outbox.RemoteChainID {
			return nil, true, ErrFederatedPipeInvalid
		}
		event.Intent, event.Payload = msg.Intent, msg.Payload
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
