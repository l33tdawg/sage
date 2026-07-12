package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSEBroadcaster_SubscribeUnsubscribe(t *testing.T) {
	b := NewSSEBroadcaster()
	assert.Zero(t, b.ClientCount())

	ch := b.Subscribe()
	assert.Equal(t, 1, b.ClientCount())
	b.mu.RLock()
	assert.Len(t, b.clients, 1)
	b.mu.RUnlock()

	b.Unsubscribe(ch)
	assert.Zero(t, b.ClientCount())
	b.mu.RLock()
	assert.Len(t, b.clients, 0)
	b.mu.RUnlock()
}

func TestSSEBroadcaster_Broadcast(t *testing.T) {
	b := NewSSEBroadcaster()
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	event := SSEEvent{
		Type:     EventRemember,
		MemoryID: "m1",
		Domain:   "general",
		Content:  "test content",
	}
	b.Broadcast(event)

	select {
	case msg := <-ch:
		require.NotEmpty(t, msg)
		assert.Contains(t, string(msg), "event: remember")
		assert.Contains(t, string(msg), "m1")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast message")
	}
}

func TestSSEBroadcaster_MultipleClients(t *testing.T) {
	b := NewSSEBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()
	ch3 := b.Subscribe()
	defer b.Unsubscribe(ch1)
	defer b.Unsubscribe(ch2)
	defer b.Unsubscribe(ch3)

	event := SSEEvent{Type: EventForget, MemoryID: "m42"}
	b.Broadcast(event)

	for i, ch := range []chan []byte{ch1, ch2, ch3} {
		select {
		case msg := <-ch:
			assert.Contains(t, string(msg), "m42", "client %d should receive event", i)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for client %d", i)
		}
	}
}

func TestSSEBroadcaster_SlowClient(t *testing.T) {
	b := NewSSEBroadcaster()
	slowCh := b.Subscribe()
	fastCh := b.Subscribe()
	defer b.Unsubscribe(slowCh)
	defer b.Unsubscribe(fastCh)

	// Fill up the slow client's buffer (capacity 64)
	for i := 0; i < 70; i++ {
		b.Broadcast(SSEEvent{Type: EventRemember, MemoryID: "fill"})
	}

	// Fast client should still get messages (up to buffer size)
	received := 0
	for {
		select {
		case <-fastCh:
			received++
		default:
			goto done
		}
	}
done:
	// Fast client should have received up to the buffer size (64)
	assert.GreaterOrEqual(t, received, 1, "fast client should receive messages")
	assert.LessOrEqual(t, received, 64, "should not exceed buffer capacity")

	// Now send one more — slow client buffer is full, should not block
	done2 := make(chan struct{})
	go func() {
		b.Broadcast(SSEEvent{Type: EventForget, MemoryID: "final"})
		close(done2)
	}()

	select {
	case <-done2:
		// Broadcast completed without blocking
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on slow client")
	}
}

func TestSSEBroadcaster_MaxClientsLimit(t *testing.T) {
	b := NewSSEBroadcaster()

	// Subscribe up to the limit — all should succeed.
	channels := make([]chan []byte, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		ch := b.Subscribe()
		require.NotNil(t, ch, "Subscribe should succeed for connection %d", i)
		channels = append(channels, ch)
	}

	// One more should return nil.
	extra := b.Subscribe()
	assert.Nil(t, extra, "Subscribe should return nil when maxClients is reached")

	// After freeing a slot, subscribe should succeed again.
	b.Unsubscribe(channels[0])
	ch := b.Subscribe()
	assert.NotNil(t, ch, "Subscribe should succeed after freeing a slot")
	b.Unsubscribe(ch)

	// Clean up.
	for _, c := range channels[1:] {
		b.Unsubscribe(c)
	}
}

func TestServeHTTP_Returns503WhenFull(t *testing.T) {
	b := NewSSEBroadcaster()

	// Fill all slots.
	channels := make([]chan []byte, 0, maxClients)
	for i := 0; i < maxClients; i++ {
		ch := b.Subscribe()
		require.NotNil(t, ch)
		channels = append(channels, ch)
	}

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	w := httptest.NewRecorder()
	b.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	for _, c := range channels {
		b.Unsubscribe(c)
	}
}

func TestSSEBroadcaster_CloseAllDrainsClientsAndRejectsNew(t *testing.T) {
	b := NewSSEBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()
	require.NotNil(t, ch1)
	require.NotNil(t, ch2)

	b.CloseAll()

	// Every connected client's channel is closed so its handler returns.
	for _, ch := range []chan []byte{ch1, ch2} {
		select {
		case _, ok := <-ch:
			assert.False(t, ok, "CloseAll must close client channels")
		case <-time.After(time.Second):
			t.Fatal("client channel not closed by CloseAll")
		}
	}

	// New subscriptions are rejected during shutdown.
	assert.Nil(t, b.Subscribe(), "Subscribe after CloseAll must be rejected")

	// A handler's deferred Unsubscribe after CloseAll must not double-close.
	assert.NotPanics(t, func() { b.Unsubscribe(ch1) })

	// Broadcast after CloseAll must not send on closed channels.
	assert.NotPanics(t, func() { b.Broadcast(SSEEvent{Type: EventUpdate, MemoryID: "x"}) })
}
