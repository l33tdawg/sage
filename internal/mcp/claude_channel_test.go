package mcp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testWakeSubscription struct {
	events chan ClaudeWakeEvent
	once   sync.Once
}

func (s *testWakeSubscription) Events() <-chan ClaudeWakeEvent { return s.events }
func (s *testWakeSubscription) Close() error {
	s.once.Do(func() {})
	return nil
}

type subscribeResult struct {
	sub ClaudeWakeSubscription
	err error
}

type testWakeSource struct {
	mu      sync.Mutex
	results chan subscribeResult
	after   []uint64
}

func (s *testWakeSource) Subscribe(ctx context.Context, afterSeq uint64) (ClaudeWakeSubscription, error) {
	s.mu.Lock()
	s.after = append(s.after, afterSeq)
	s.mu.Unlock()
	select {
	case result := <-s.results:
		return result.sub, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *testWakeSource) afterSeqs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.after...)
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *lockedBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	trimmed := strings.TrimSpace(b.b.String())
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, condition(), "condition did not become true before deadline")
}

func testClaudeConfig(source ClaudeWakeSource) claudeChannelConfig {
	return claudeChannelConfig{
		source:         source,
		coalesceWindow: 5 * time.Millisecond,
		backoffMin:     5 * time.Millisecond,
		backoffMax:     20 * time.Millisecond,
	}
}

func TestClaudeChannelCapabilityIsExplicitOptIn(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	server := NewServer("http://127.0.0.1:1", privateKey)
	server.ensureAutoInception(context.Background(), true)

	initialize := func() map[string]any {
		response := server.handleInitialize(context.Background(), &jsonRPCRequest{JSONRPC: "2.0", ID: 1, Method: "initialize"})
		result := response.Result.(map[string]any)
		return result["capabilities"].(map[string]any)
	}

	capabilities := initialize()
	require.NotContains(t, capabilities, "experimental")
	require.Error(t, server.ConfigureClaudeChannel(nil))
	require.NotContains(t, initialize(), "experimental")

	source := &testWakeSource{results: make(chan subscribeResult, 1)}
	require.NoError(t, server.ConfigureClaudeChannel(source))
	capabilities = initialize()
	experimental := capabilities["experimental"].(map[string]any)
	require.Equal(t, map[string]any{}, experimental["claude/channel"])
	require.Len(t, experimental, 1)

	server.DisableClaudeChannel()
	require.NotContains(t, initialize(), "experimental")
}

func TestClaudeChannelCoalescesDuplicateReorderedAndBurstWakeEvents(t *testing.T) {
	subscription := &testWakeSubscription{events: make(chan ClaudeWakeEvent, 256)}
	source := &testWakeSource{results: make(chan subscribeResult, 1)}
	source.results <- subscribeResult{sub: subscription}

	ctx, cancel := context.WithCancel(context.Background())
	var sink lockedBuffer
	out := newStdioOutbound(ctx, &sink)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClaudeChannel(ctx, out, testClaudeConfig(source))
	}()
	subscription.events <- ClaudeWakeEvent{Version: 2, Seq: 999, Pending: true}
	for _, seq := range []uint64{2, 1, 2, 5, 4, 5} {
		subscription.events <- ClaudeWakeEvent{Version: 1, Seq: seq, Pending: true}
	}
	waitFor(t, func() bool { return len(sink.lines()) == 1 })

	lines := sink.lines()
	require.Len(t, lines, 1)
	var notification map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &notification))
	require.Equal(t, "notifications/claude/channel", notification["method"])
	params := notification["params"].(map[string]any)
	require.Equal(t, claudeChannelWakePrompt, params["content"])
	require.ElementsMatch(t, []string{"content", "meta"}, mapKeys(params))
	meta := params["meta"].(map[string]any)
	require.Equal(t, "5", meta["wake_seq"])
	require.ElementsMatch(t, []string{"wake_seq"}, mapKeys(meta))

	// A passive pointer saying nothing is pending cancels a not-yet-emitted
	// cursor without creating a synthetic host turn.
	subscription.events <- ClaudeWakeEvent{Version: 1, Seq: 6, Pending: true}
	subscription.events <- ClaudeWakeEvent{Version: 1, Seq: 6, Pending: false}
	time.Sleep(20 * time.Millisecond)
	require.Len(t, sink.lines(), 1)

	cancel()
	<-done
	out.Close()
}

func TestClaudeChannelKeepsOnlyOneBusyFollowUp(t *testing.T) {
	subscription := &testWakeSubscription{events: make(chan ClaudeWakeEvent, 512)}
	source := &testWakeSource{results: make(chan subscribeResult, 1)}
	source.results <- subscribeResult{sub: subscription}

	writer := &blockingFirstWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := newStdioOutbound(ctx, writer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClaudeChannel(ctx, out, testClaudeConfig(source))
	}()
	subscription.events <- ClaudeWakeEvent{Version: 1, Seq: 1, Pending: true}
	select {
	case <-writer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first host notification never entered the writer")
	}
	for seq := uint64(2); seq <= 100; seq++ {
		subscription.events <- ClaudeWakeEvent{Version: 1, Seq: seq, Pending: true}
	}
	close(writer.release)
	waitFor(t, func() bool { return len(writer.lines()) == 2 })
	time.Sleep(20 * time.Millisecond)
	require.Len(t, writer.lines(), 2, "burst while busy must create one follow-up")
	require.Equal(t, []string{"1", "100"}, wakeSeqs(t, writer.lines()))

	cancel()
	<-done
	out.Close()
}

func TestClaudeChannelReconnectsFromHighestObservedCursor(t *testing.T) {
	first := &testWakeSubscription{events: make(chan ClaudeWakeEvent, 8)}
	second := &testWakeSubscription{events: make(chan ClaudeWakeEvent, 8)}
	source := &testWakeSource{results: make(chan subscribeResult, 3)}
	source.results <- subscribeResult{err: errors.New("temporary failure")}
	source.results <- subscribeResult{sub: first}
	source.results <- subscribeResult{sub: second}

	ctx, cancel := context.WithCancel(context.Background())
	var sink lockedBuffer
	out := newStdioOutbound(ctx, &sink)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runClaudeChannel(ctx, out, testClaudeConfig(source))
	}()
	waitFor(t, func() bool { return len(source.afterSeqs()) >= 2 })
	first.events <- ClaudeWakeEvent{Version: 1, Seq: 10, Pending: true}
	waitFor(t, func() bool { return len(sink.lines()) == 1 })
	close(first.events)
	waitFor(t, func() bool { return len(source.afterSeqs()) >= 3 })
	require.Equal(t, []uint64{0, 0, 10}, source.afterSeqs()[:3])
	second.events <- ClaudeWakeEvent{Version: 1, Seq: 10, Pending: true}
	second.events <- ClaudeWakeEvent{Version: 1, Seq: 11, Pending: true}
	waitFor(t, func() bool { return len(sink.lines()) == 2 })
	require.Equal(t, []string{"10", "11"}, wakeSeqs(t, sink.lines()))

	cancel()
	<-done
	out.Close()
}

func TestStdioOutboundStressNeverInterleavesFrames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &yieldingWriter{}
	out := newStdioOutbound(ctx, writer)
	const frames = 200
	var wg sync.WaitGroup
	errs := make(chan error, frames)
	for i := 0; i < frames; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			errs <- out.WriteJSON(ctx, map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result":  strings.Repeat(strconv.Itoa(id%10), 128),
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	out.Close()
	lines := writer.lines()
	require.Len(t, lines, frames)
	seen := make(map[int]bool, frames)
	for _, line := range lines {
		var response struct {
			JSONRPC string `json:"jsonrpc"`
			ID      int    `json:"id"`
			Result  string `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &response), line)
		require.Equal(t, "2.0", response.JSONRPC)
		require.Len(t, response.Result, 128)
		seen[response.ID] = true
	}
	require.Len(t, seen, frames)
}

func TestStdioOutboundFailsClosedOnShortWrite(t *testing.T) {
	ctx := context.Background()
	out := newStdioOutbound(ctx, shortWriter{})
	err := out.WriteJSON(ctx, map[string]any{"jsonrpc": "2.0", "id": 1})
	require.ErrorIs(t, err, io.ErrShortWrite)
	out.Close()
}

type blockingFirstWriter struct {
	mu      sync.Mutex
	b       bytes.Buffer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingFirstWriter) Write(data []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(data)
}

func (w *blockingFirstWriter) lines() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	trimmed := strings.TrimSpace(w.b.String())
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

type yieldingWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) {
	return len(data) - 1, nil
}

func (w *yieldingWriter) Write(data []byte) (int, error) {
	for _, value := range data {
		w.mu.Lock()
		_ = w.b.WriteByte(value)
		w.mu.Unlock()
		runtime.Gosched()
	}
	return len(data), nil
}

func (w *yieldingWriter) lines() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Split(strings.TrimSpace(w.b.String()), "\n")
}

func wakeSeqs(t *testing.T, lines []string) []string {
	t.Helper()
	seqs := make([]string, 0, len(lines))
	for _, line := range lines {
		var notification struct {
			Method string `json:"method"`
			Params struct {
				Content string            `json:"content"`
				Meta    map[string]string `json:"meta"`
			} `json:"params"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &notification))
		require.Equal(t, "notifications/claude/channel", notification.Method)
		require.Equal(t, claudeChannelWakePrompt, notification.Params.Content)
		seqs = append(seqs, notification.Params.Meta["wake_seq"])
	}
	return seqs
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
