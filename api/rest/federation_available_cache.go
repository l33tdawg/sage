package rest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	federationAvailabilityCacheTTL = 5 * time.Second
	federationAvailabilityCacheMax = 256
	federationProbeBudgetWindow    = 10 * time.Second
	federationProbeBudgetPerCaller = 24
	federationProbeBudgetGlobal    = 96
	federationScanHorizonPeers     = 256
	federationCursorTTL            = 5 * time.Minute
)

type federationAvailabilityCacheBypassKey struct{}

type federationAvailabilityResponse struct {
	status int
	header http.Header
	body   []byte
}

type federationAvailabilityCacheEntry struct {
	response federationAvailabilityResponse
	expires  time.Time
}

type federationAvailabilityCache struct {
	mu      sync.Mutex
	entries map[string]federationAvailabilityCacheEntry
	order   []string
	cursors map[string]federationPeerCursor
	group   singleflight.Group
}

type federationPeerCursor struct {
	callerID   string
	agentName  string
	agentLimit int
	peers      []string
	offset     int
	expires    time.Time
}

type federationProbeBudgetEntry struct {
	windowStart time.Time
	used        int
}

// federationProbeRateBudget bounds total outbound peer calls, not merely
// concurrency. A targeted miss can otherwise perform status + legacy lookup +
// linked lookup against every peer, and distinct cache keys defeat
// singleflight. The caller and process windows make that amplification finite.
type federationProbeRateBudget struct {
	mu      sync.Mutex
	global  federationProbeBudgetEntry
	callers map[string]federationProbeBudgetEntry
}

func newFederationProbeRateBudget() *federationProbeRateBudget {
	return &federationProbeRateBudget{callers: make(map[string]federationProbeBudgetEntry)}
}

func refreshProbeBudgetEntry(entry federationProbeBudgetEntry, now time.Time) federationProbeBudgetEntry {
	if entry.windowStart.IsZero() || now.Sub(entry.windowStart) >= federationProbeBudgetWindow {
		return federationProbeBudgetEntry{windowStart: now}
	}
	return entry
}

func (b *federationProbeRateBudget) reserve(callerID string, cost int, now time.Time) bool {
	if b == nil || cost <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.global = refreshProbeBudgetEntry(b.global, now)
	caller := refreshProbeBudgetEntry(b.callers[callerID], now)
	if caller.used+cost > federationProbeBudgetPerCaller ||
		b.global.used+cost > federationProbeBudgetGlobal {
		return false
	}
	caller.used += cost
	b.global.used += cost
	b.callers[callerID] = caller
	// Bound inactive caller bookkeeping without weakening the live window.
	if len(b.callers) > 1024 {
		for id, entry := range b.callers {
			if now.Sub(entry.windowStart) >= federationProbeBudgetWindow {
				delete(b.callers, id)
			}
		}
	}
	return true
}

func newFederationAvailabilityCache() *federationAvailabilityCache {
	return &federationAvailabilityCache{
		entries: make(map[string]federationAvailabilityCacheEntry),
		cursors: make(map[string]federationPeerCursor),
	}
}

func (c *federationAvailabilityCache) putPeerCursor(cursor federationPeerCursor) string {
	if c == nil {
		return ""
	}
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return ""
	}
	token := base64.RawURLEncoding.EncodeToString(random)
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.cursors) >= federationAvailabilityCacheMax {
		for key, entry := range c.cursors {
			if !now.Before(entry.expires) {
				delete(c.cursors, key)
			}
		}
	}
	if len(c.cursors) >= federationAvailabilityCacheMax {
		for key := range c.cursors {
			delete(c.cursors, key)
			break
		}
	}
	cursor.peers = append([]string(nil), cursor.peers...)
	cursor.expires = now.Add(federationCursorTTL)
	c.cursors[token] = cursor
	return token
}

func (c *federationAvailabilityCache) getPeerCursor(token, callerID, agentName string, agentLimit int) (federationPeerCursor, bool) {
	if c == nil || token == "" {
		return federationPeerCursor{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.cursors[token]
	if !ok || !time.Now().Before(entry.expires) || entry.callerID != callerID ||
		entry.agentName != agentName || entry.agentLimit != agentLimit {
		return federationPeerCursor{}, false
	}
	entry.peers = append([]string(nil), entry.peers...)
	return entry, true
}

func cloneAvailabilityResponse(in federationAvailabilityResponse) federationAvailabilityResponse {
	return federationAvailabilityResponse{
		status: in.status, header: in.header.Clone(), body: append([]byte(nil), in.body...),
	}
}

func (c *federationAvailabilityCache) get(key string, now time.Time) (federationAvailabilityResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || !now.Before(entry.expires) {
		if ok {
			delete(c.entries, key)
		}
		return federationAvailabilityResponse{}, false
	}
	return cloneAvailabilityResponse(entry.response), true
}

func (c *federationAvailabilityCache) put(key string, response federationAvailabilityResponse, now time.Time) {
	if response.status != http.StatusOK {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.entries[key]; !exists {
		c.order = append(c.order, key)
	}
	c.entries[key] = federationAvailabilityCacheEntry{
		response: cloneAvailabilityResponse(response), expires: now.Add(federationAvailabilityCacheTTL),
	}
	for len(c.entries) > federationAvailabilityCacheMax && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
}

func (c *federationAvailabilityCache) load(
	ctx context.Context, key string,
	loader func(context.Context) federationAvailabilityResponse,
) (federationAvailabilityResponse, error) {
	if response, ok := c.get(key, time.Now()); ok {
		return response, nil
	}
	result := c.group.DoChan(key, func() (any, error) {
		if response, ok := c.get(key, time.Now()); ok {
			return response, nil
		}
		loaderCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fedRecallTimeout())
		defer cancel()
		response := loader(loaderCtx)
		c.put(key, response, time.Now())
		return response, nil
	})
	select {
	case <-ctx.Done():
		return federationAvailabilityResponse{}, ctx.Err()
	case loaded := <-result:
		if loaded.Err != nil {
			return federationAvailabilityResponse{}, loaded.Err
		}
		return cloneAvailabilityResponse(loaded.Val.(federationAvailabilityResponse)), nil
	}
}

type federationAvailabilityRecorder struct {
	header      http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func newFederationAvailabilityRecorder() *federationAvailabilityRecorder {
	return &federationAvailabilityRecorder{header: make(http.Header), status: http.StatusOK}
}

func (r *federationAvailabilityRecorder) Header() http.Header { return r.header }
func (r *federationAvailabilityRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
}
func (r *federationAvailabilityRecorder) Write(body []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(body)
}

func writeFederationAvailabilityResponse(w http.ResponseWriter, response federationAvailabilityResponse) {
	for key, values := range response.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}
