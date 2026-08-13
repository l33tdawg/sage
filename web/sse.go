package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// EventType represents the type of SSE event.
type EventType string

// maxClients is the maximum number of concurrent SSE connections allowed.
const maxClients = 50

const (
	EventRemember   EventType = "remember"
	EventRecall     EventType = "recall"
	EventForget     EventType = "forget"
	EventVote       EventType = "vote"
	EventConsensus  EventType = "consensus"
	EventAgent      EventType = "agent"
	EventImport     EventType = "import"
	EventUpdate     EventType = "update"
	EventGovernance EventType = "governance"
	EventTask       EventType = "task"
	// EventRecovery is a local, Root-authorized recovery decision. It is shown
	// in activity for operator visibility but must never be presented as a
	// consensus commit or a rewrite of historical chain state.
	EventRecovery EventType = "recovery"
	// EventAccess is emitted only after an RBAC transaction commits. It keeps
	// Chain Activity an auditable view of enforced permissions, not merely a
	// memory-operation feed.
	EventAccess EventType = "access"
	// EventConnectome announces that the local agent connectome may have
	// changed. It is a CACHE-INVALIDATION TICK, not data: it carries no
	// identifiers, no counts and no text at all.
	//
	// The reason it must stay empty is structural. This stream is a global
	// fan-out with no subscriber identity, while the connectome snapshot at
	// /v1/dashboard/network/synapses is RBAC-filtered per caller — an edge is
	// returned only when both endpoints are visible. Putting an edge on the
	// tick would publish, to every connected client, a relationship the
	// snapshot would have withheld from most of them. Sending nothing and
	// letting each client re-fetch through the authorized endpoint keeps that
	// filter as the single enforcement point, so the guarantee holds by
	// construction rather than by a second filter kept in sync by hand.
	EventConnectome EventType = "connectome"
	// EventReinstate is the counterpart of EventForget: a deprecated memory
	// returned to active service by consensus.
	EventReinstate EventType = "reinstate"
	// EventCoCommit is a multi-agent shared commit. It is a commit like
	// EventRemember, so the dashboard must be able to observe it.
	EventCoCommit EventType = "cocommit"
	// EventSearch is a text-search retrieval — the lexical sibling of
	// EventRecall.
	EventSearch EventType = "search"
	// EventHybrid is a combined vector+text retrieval — the hybrid sibling of
	// EventRecall.
	EventHybrid EventType = "hybrid"
	// EventPipelineSend reports a message handed to an agent pipeline.
	EventPipelineSend EventType = "pipeline_send"
	// EventPipelineComplete reports an agent pipeline run reaching its end.
	EventPipelineComplete EventType = "pipeline_complete"
	// EventRedeploy reports chain-reconfiguration progress for the network page.
	EventRedeploy EventType = "redeploy"
)

// AllEventTypes is the canonical registry of every SSE event the node emits.
//
// An SSE event only reaches a user when three separate places agree on the same
// string: the emit site in Go, this registry, and the listener list in
// static/js/sse.js. Nothing about the language links them — the REST layer emits
// raw strings that cmd/sage-gui converts straight into EventType, and the
// browser only receives event names it explicitly subscribed to. Miss one copy
// and the feature compiles, runs, emits, and is seen by nobody.
//
// This slice is the single Go-side source of truth that closes that gap:
// TestSSEEventWiring in sse_wiring_test.go proves the const block above, every
// emit site in the repository, and the JavaScript listener list all name exactly
// these events. Adding an event means adding it here and in static/js/sse.js;
// the test fails until both are done.
var AllEventTypes = []EventType{
	EventRemember,
	EventRecall,
	EventForget,
	EventVote,
	EventConsensus,
	EventAgent,
	EventImport,
	EventUpdate,
	EventGovernance,
	EventTask,
	EventRecovery,
	EventAccess,
	EventConnectome,
	EventReinstate,
	EventCoCommit,
	EventSearch,
	EventHybrid,
	EventPipelineSend,
	EventPipelineComplete,
	EventRedeploy,
}

// EventTypeFromREST adopts an event name produced by the REST layer, which
// passes plain strings to api/rest.Server.OnEvent because that package does not
// import this one.
//
// This is the ONLY place an unconstrained string is allowed to become an
// EventType. The name is still checked — TestSSEEventWiring reads the OnEvent
// call sites directly and requires every name they pass to be registered in
// AllEventTypes — so routing the bridge through a named function keeps that one
// unchecked-looking conversion greppable instead of letting any
// EventType(someExpression) anywhere in the tree slip an unregistered event onto
// the stream.
func EventTypeFromREST(name string) EventType {
	return EventType(name)
}

// SSEEvent is an event sent to connected dashboard clients.
type SSEEvent struct {
	Type     EventType `json:"type"`
	MemoryID string    `json:"memory_id"`
	Domain   string    `json:"domain,omitempty"`
	Content  string    `json:"content,omitempty"`
	Data     any       `json:"data,omitempty"`
}

// SSEBroadcaster manages SSE client connections and broadcasts events.
type SSEBroadcaster struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
	closed  bool
}

// NewSSEBroadcaster creates a new SSE broadcaster.
func NewSSEBroadcaster() *SSEBroadcaster {
	return &SSEBroadcaster{
		clients: make(map[chan []byte]struct{}),
	}
}

// Subscribe registers a new SSE client and returns its channel.
// Returns nil if the maximum number of concurrent connections has been
// reached or the broadcaster has been closed for shutdown.
func (b *SSEBroadcaster) Subscribe() chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || len(b.clients) >= maxClients {
		return nil
	}
	ch := make(chan []byte, 64)
	b.clients[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a client channel. Safe to call after CloseAll already
// disconnected the client.
func (b *SSEBroadcaster) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.clients[ch]; ok {
		delete(b.clients, ch)
		close(ch)
	}
}

// ClientCount reports how many dashboard event streams are currently alive.
// The macOS launcher uses this as a browser-independent presence signal: Firefox
// does not expose its tabs to AppleScript, but an open CEREBRUM tab keeps this
// stream connected for its lifetime.
func (b *SSEBroadcaster) ClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// CloseAll disconnects every connected client and rejects new subscriptions.
// The dashboard holds its event stream open for the whole tab lifetime, so a
// coordinated shutdown must drain these handlers explicitly — otherwise
// http.Server.Shutdown blocks its full budget on a stream that never ends.
func (b *SSEBroadcaster) CloseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for ch := range b.clients {
		delete(b.clients, ch)
		close(ch)
	}
}

// Broadcast sends an event to all connected clients.
func (b *SSEBroadcaster) Broadcast(event SSEEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	msg := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event.Type, data))

	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- msg:
		default:
			// Drop message if client is slow
		}
	}
}

// ServeHTTP handles the SSE endpoint.
func (b *SSEBroadcaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// SSE is a long-lived stream, but the http.Server has WriteTimeout set
	// (15s). That timeout is an ABSOLUTE per-connection write deadline — the
	// heartbeat below does NOT reset it — so without clearing it the server
	// guillotines every SSE connection at 15s mid-stream, which the browser
	// reports as ERR_INCOMPLETE_CHUNKED_ENCODING and then reconnects, producing
	// a connect/drop/reconnect storm. Clear the write deadline so the heartbeat
	// is what actually governs liveness.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{}) //nolint:errcheck // best-effort; unsupported writers keep prior behaviour

	ch := b.Subscribe()
	if ch == nil {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer b.Unsubscribe(ch)

	// Send initial keepalive
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	// Heartbeat ticker — periodic SSE comments so a dead/half-open client is
	// detected (the Write fails, ctx cancels) and proxies don't idle-close the
	// stream. (The WriteTimeout guillotine is handled above by clearing the
	// write deadline; the heartbeat alone never cured it.)
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			// SSE comment — keeps the connection alive without triggering client events
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case msg, ok := <-ch:
			if !ok {
				return
			}
			w.Write(msg) //nolint:errcheck
			flusher.Flush()
		}
	}
}
