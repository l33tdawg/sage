package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	messageWakeVersion          = 1
	messageWakeHeartbeat        = 15 * time.Second
	messageWakeLeaseTTL         = 5 * time.Minute
	messageWakeWriteBudget      = 5 * time.Second
	maxMessageWakeConsumerBytes = 128
)

var errMessageWakeLeaseHeld = errors.New("message wake consumer lease is already held")

// messageWakePayload is deliberately the whole data contract. Do not add
// message IDs, senders, payloads, delivery/read state, or presence inference.
type messageWakePayload struct {
	Version int    `json:"version"`
	Seq     uint64 `json:"seq"`
	Pending bool   `json:"pending"`
}

type messageWakeLease struct {
	token      uint64
	consumerID string
	events     chan uint64
	cancel     context.CancelFunc
	done       <-chan struct{}
	timer      *time.Timer
	latest     uint64
}

type messageWakeSubscription struct {
	events  <-chan uint64
	done    <-chan struct{}
	release func()
}

// messageWakeBroker is process-local acceleration over the durable store
// sequence. One exact agent owns at most one active lease. A reconnect from the
// same consumer supersedes its old stream; a different consumer fails until
// release/TTL, preventing two runtimes from treating the same wake as owned.
type messageWakeBroker struct {
	mu       sync.Mutex
	leases   map[string]*messageWakeLease
	nextID   atomic.Uint64
	leaseTTL time.Duration
}

func newMessageWakeBroker(leaseTTL time.Duration) *messageWakeBroker {
	if leaseTTL <= 0 {
		leaseTTL = messageWakeLeaseTTL
	}
	return &messageWakeBroker{leases: make(map[string]*messageWakeLease), leaseTTL: leaseTTL}
}

func (b *messageWakeBroker) acquire(agentID, consumerID string) (*messageWakeSubscription, error) {
	if b == nil || agentID == "" || consumerID == "" {
		return nil, errors.New("message wake lease identity is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if prior := b.leases[agentID]; prior != nil {
		if prior.consumerID != consumerID {
			return nil, errMessageWakeLeaseHeld
		}
		prior.cancel()
		if prior.timer != nil {
			prior.timer.Stop()
		}
		delete(b.leases, agentID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lease := &messageWakeLease{
		token: b.nextID.Add(1), consumerID: consumerID,
		events: make(chan uint64, 1), cancel: cancel, done: ctx.Done(),
	}
	lease.timer = time.AfterFunc(b.leaseTTL, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		current := b.leases[agentID]
		if current == nil || current.token != lease.token {
			return
		}
		delete(b.leases, agentID)
		current.cancel()
	})
	b.leases[agentID] = lease
	return &messageWakeSubscription{
		events: lease.events,
		done:   lease.done,
		release: func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			current := b.leases[agentID]
			if current == nil || current.token != lease.token {
				return
			}
			delete(b.leases, agentID)
			current.cancel()
			if current.timer != nil {
				current.timer.Stop()
			}
		},
	}, nil
}

func (b *messageWakeBroker) publish(agentID string, seq uint64) {
	if b == nil || agentID == "" || seq == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	lease := b.leases[agentID]
	if lease == nil || seq <= lease.latest {
		return
	}
	lease.latest = seq
	select {
	case lease.events <- seq:
		return
	default:
	}
	// A slow consumer retains only the newest monotonic sequence. Durable state
	// supplies the exact pending predicate when that coalesced event is written.
	select {
	case <-lease.events:
	default:
	}
	select {
	case lease.events <- seq:
	default:
	}
}

func (s *Server) messageWakeBroker() *messageWakeBroker {
	s.messageWakeMu.Lock()
	defer s.messageWakeMu.Unlock()
	if s.messageWake == nil {
		s.messageWake = newMessageWakeBroker(messageWakeLeaseTTL)
	}
	return s.messageWake
}

func (s *Server) publishMessageWake(agentID string, seq uint64) {
	s.messageWakeBroker().publish(agentID, seq)
}

func parseMessageWakeCursor(r *http.Request) (uint64, error) {
	queryValue := strings.TrimSpace(r.URL.Query().Get("after_seq"))
	headerValue := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	parse := func(label, value string) (uint64, bool, error) {
		if value == "" {
			return 0, false, nil
		}
		seq, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("%s must be an unsigned decimal sequence", label)
		}
		return seq, true, nil
	}
	querySeq, querySet, err := parse("after_seq", queryValue)
	if err != nil {
		return 0, err
	}
	headerSeq, headerSet, err := parse("Last-Event-ID", headerValue)
	if err != nil {
		return 0, err
	}
	if querySet && headerSet && querySeq != headerSeq {
		return 0, errors.New("after_seq and Last-Event-ID must match when both are supplied")
	}
	if querySet {
		return querySeq, nil
	}
	return headerSeq, nil
}

func writeMessageWakeEvent(w http.ResponseWriter, state store.MessageWakeState) error {
	payload, err := json.Marshal(messageWakePayload{
		Version: messageWakeVersion, Seq: state.Seq, Pending: state.Pending,
	})
	if err != nil {
		return err
	}
	return writeMessageWakeFrame(w, func() error {
		_, err := fmt.Fprintf(w, "id: %d\nevent: wake\ndata: %s\n\n", state.Seq, payload)
		return err
	})
}

func writeMessageWakeHeartbeat(w http.ResponseWriter) error {
	return writeMessageWakeFrame(w, func() error {
		_, err := fmt.Fprint(w, ": heartbeat\n\n")
		return err
	})
}

func writeMessageWakeFrame(w http.ResponseWriter, write func() error) error {
	controller := http.NewResponseController(w)
	// In-process/test writers may not expose transport deadlines. Retain their
	// existing best-effort behavior, but fail closed on real transport errors.
	if err := controller.SetWriteDeadline(time.Now().Add(messageWakeWriteBudget)); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	return controller.Flush()
}

// handleMessageWake is an ordinary signed exact-agent SSE route. It is not the
// dashboard broadcaster and never observes, claims, reads, or acknowledges a
// message. Durable state closes commit-before-publish and process-crash gaps.
func (s *Server) handleMessageWake(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	agentID := middleware.ContextAgentID(r.Context())
	consumerID := strings.TrimSpace(r.URL.Query().Get("consumer_id"))
	if consumerID == "" || len(consumerID) > maxMessageWakeConsumerBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid wake consumer", "consumer_id must be between 1 and 128 bytes")
		return
	}
	afterSeq, err := parseMessageWakeCursor(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid wake cursor", err.Error())
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical message wakes.")
		return
	}
	subscription, err := s.messageWakeBroker().acquire(agentID, consumerID)
	if err != nil {
		if errors.Is(err, errMessageWakeLeaseHeld) {
			w.Header().Set("Retry-After", "2")
			writeProblem(w, http.StatusConflict, "Wake consumer already active", "Another consumer holds this exact agent's wake lease. Retry after it disconnects or the lease expires.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Wake unavailable", err.Error())
		return
	}
	defer subscription.release()

	state, err := messageStore.GetMessageWakeState(r.Context(), agentID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Wake state unavailable", err.Error())
		return
	}
	if afterSeq > state.Seq {
		writeProblem(w, http.StatusConflict, "Wake cursor is ahead", "after_seq cannot exceed this exact recipient's durable wake sequence")
		return
	}

	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	lastSent := afterSeq
	if state.Seq > afterSeq {
		if err := writeMessageWakeEvent(w, state); err != nil {
			return
		}
		lastSent = state.Seq
	}

	heartbeatInterval := s.messageWakeHeartbeat
	if heartbeatInterval <= 0 {
		heartbeatInterval = messageWakeHeartbeat
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-subscription.done:
			return
		case <-heartbeat.C:
			if err := writeMessageWakeHeartbeat(w); err != nil {
				return
			}
		case <-subscription.events:
			state, err := messageStore.GetMessageWakeState(r.Context(), agentID)
			if err != nil || state.Seq <= lastSent {
				continue
			}
			if err := writeMessageWakeEvent(w, state); err != nil {
				return
			}
			lastSent = state.Seq
		}
	}
}
