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
	EventRemember  EventType = "remember"
	EventRecall    EventType = "recall"
	EventForget    EventType = "forget"
	EventVote      EventType = "vote"
	EventConsensus EventType = "consensus"
	EventAgent     EventType = "agent"
	EventImport     EventType = "import"
	EventUpdate     EventType = "update"
	EventGovernance EventType = "governance"
)

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
}

// NewSSEBroadcaster creates a new SSE broadcaster.
func NewSSEBroadcaster() *SSEBroadcaster {
	return &SSEBroadcaster{
		clients: make(map[chan []byte]struct{}),
	}
}

// Subscribe registers a new SSE client and returns its channel.
// Returns nil if the maximum number of concurrent connections has been reached.
func (b *SSEBroadcaster) Subscribe() chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.clients) >= maxClients {
		return nil
	}
	ch := make(chan []byte, 64)
	b.clients[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a client channel.
func (b *SSEBroadcaster) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
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

	ch := b.Subscribe()
	if ch == nil {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer b.Unsubscribe(ch)

	// Send initial keepalive
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	// Heartbeat ticker — keeps the connection alive past the HTTP server's WriteTimeout.
	// Without this, idle SSE connections get killed every WriteTimeout seconds,
	// causing a connect/disconnect cycle in the chain activity log.
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
