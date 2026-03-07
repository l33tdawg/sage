package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSEBroadcaster_SubscribeUnsubscribe(t *testing.T) {
	b := NewSSEBroadcaster()

	ch := b.Subscribe()
	b.mu.RLock()
	assert.Len(t, b.clients, 1)
	b.mu.RUnlock()

	b.Unsubscribe(ch)
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
