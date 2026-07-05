package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// DefaultMempoolMaxTxs is the fallback CometBFT mempool capacity (max tx
// count) used when the owning process does not wire the real value via
// SetMempoolCap. It mirrors CometBFT's DefaultMempoolConfig Size, which IS
// the runtime cap for sage-gui nodes: cmd/sage-gui/node.go builds its
// running config via config.DefaultConfig() and never overrides
// Mempool.Size (node.go still calls SetMempoolCap with the live value so a
// future code-side override propagates automatically).
//
// This constant MUST stay in sync with cometCfg.Mempool.Size in
// cmd/sage-gui/node.go's runServe. Do NOT source it from an on-disk
// config.toml — those files are reference-only, never read at runtime, and
// stale copies carry an obsolete size = 1000.
const DefaultMempoolMaxTxs = 5000

// mempoolSampleTTL is how long one /num_unconfirmed_txs probe stays fresh.
// ~1s (the personal-mode block interval) means a hot loop on the
// backpressure endpoint — or a burst of memory submits — costs at most one
// upstream RPC per second instead of storming CometBFT.
const mempoolSampleTTL = time.Second

// mempoolPressureThreshold is the mempool fill fraction at or above which
// the node advises writers to back off (accepting_writes=false and a
// non-zero retry_after_ms in the backpressure response).
const mempoolPressureThreshold = 0.9

// mempoolRetryAfterMs is the back-off hint handed to clients when the
// mempool is near capacity. One block interval (personal-mode
// TimeoutCommit = 1s) is the soonest a saturated mempool can drain.
const mempoolRetryAfterMs = 1000

// mempoolSample is one observation of CometBFT mempool depth.
type mempoolSample struct {
	Txs        int     // unconfirmed tx count (n_txs)
	TotalBytes int64   // unconfirmed bytes (total_bytes)
	MaxTxs     int     // runtime cap the percentage is computed against
	Pct        float64 // Txs / MaxTxs, 0..1
}

// mempoolSampler polls {cometRPC}/num_unconfirmed_txs behind a short TTL
// cache so backpressure consumers (GET /v1/chain/backpressure and the
// X-Sage-Mempool-Pct header on submits) never storm the CometBFT RPC.
// The mutex doubles as single-flight: concurrent callers arriving on a
// stale cache wait for one probe instead of each issuing their own.
type mempoolSampler struct {
	cometRPC string
	client   *http.Client

	mu         sync.Mutex
	ttl        time.Duration
	maxTxs     int
	txs        int
	totalBytes int64
	sampledAt  time.Time
	valid      bool
	probing    bool // a refresh is in flight; other callers must not block behind it
}

func newMempoolSampler(cometRPC string, maxTxs int) *mempoolSampler {
	return &mempoolSampler{
		cometRPC: cometRPC,
		// Short timeout: the probe is best-effort and must never stall a
		// submit response (the header hint is skipped on failure).
		client: &http.Client{Timeout: 500 * time.Millisecond},
		ttl:    mempoolSampleTTL,
		maxTxs: maxTxs,
	}
}

// setMaxTxs overrides the capacity the percentage is computed against.
// Non-positive values are ignored (the DefaultMempoolMaxTxs fallback stays).
func (m *mempoolSampler) setMaxTxs(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	m.maxTxs = n
	m.mu.Unlock()
}

// sample returns the current mempool depth, served from cache when the last
// probe is fresher than the TTL. ok is false when no fresh sample exists and
// the probe failed — callers degrade gracefully (omit the header, 503 the
// signal endpoint) rather than fail their own request.
func (m *mempoolSampler) sample() (mempoolSample, bool) {
	m.mu.Lock()
	// Fast path: a RECENT probe — success OR failure — serves everyone without
	// touching the RPC. Keying on probe recency (not just m.valid) negative-caches a
	// down RPC so it is re-probed at most once per TTL instead of on every call.
	if !m.sampledAt.IsZero() && time.Since(m.sampledAt) < m.ttl {
		if m.valid {
			s := m.snapshotLocked()
			m.mu.Unlock()
			return s, true
		}
		m.mu.Unlock()
		return mempoolSample{}, false
	}
	// Stale, but another goroutine is already refreshing. Do NOT block behind the
	// probe (this endpoint is safe to poll tightly, and a hung CometBFT RPC must not
	// serialize submit responses). Serve the last-known value if we have one, else
	// report unknown.
	if m.probing {
		if m.valid {
			s := m.snapshotLocked()
			m.mu.Unlock()
			return s, true
		}
		m.mu.Unlock()
		return mempoolSample{}, false
	}
	// We are the single refresher. Release the lock so concurrent callers are never
	// blocked by the (up to 500ms) probe.
	m.probing = true
	m.mu.Unlock()

	txs, totalBytes, err := m.fetch()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.probing = false
	// Stamp on success AND failure: this negative-caches a down RPC so it is
	// re-probed at most once per TTL instead of by every caller.
	m.sampledAt = time.Now()
	if err != nil {
		m.valid = false
		return mempoolSample{}, false
	}
	m.txs, m.totalBytes, m.valid = txs, totalBytes, true
	return m.snapshotLocked(), true
}

// snapshotLocked builds a mempoolSample from the cached fields. Caller holds m.mu.
func (m *mempoolSampler) snapshotLocked() mempoolSample {
	s := mempoolSample{Txs: m.txs, TotalBytes: m.totalBytes, MaxTxs: m.maxTxs}
	if m.maxTxs > 0 {
		s.Pct = float64(m.txs) / float64(m.maxTxs)
	}
	return s
}

// fetch probes CometBFT for the live mempool depth. n_txs and total_bytes
// are JSON STRINGS (CometBFT encodes int64 that way) — same decode shape as
// the dashboard health probe in web/handler.go.
func (m *mempoolSampler) fetch() (txs int, totalBytes int64, err error) {
	req, err := http.NewRequest(http.MethodGet, m.cometRPC+"/num_unconfirmed_txs", nil)
	if err != nil {
		return 0, 0, fmt.Errorf("create mempool probe: %w", err)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("mempool probe: %w", err)
	}
	defer resp.Body.Close()

	var out struct {
		Result struct {
			NTxs       string `json:"n_txs"`
			TotalBytes string `json:"total_bytes"`
		} `json:"result"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, 0, fmt.Errorf("decode mempool probe: %w", err)
	}
	if txs, err = strconv.Atoi(out.Result.NTxs); err != nil {
		return 0, 0, fmt.Errorf("parse n_txs %q: %w", out.Result.NTxs, err)
	}
	if totalBytes, err = strconv.ParseInt(out.Result.TotalBytes, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("parse total_bytes %q: %w", out.Result.TotalBytes, err)
	}
	return txs, totalBytes, nil
}

// formatMempoolPct renders a fill fraction for the X-Sage-Mempool-Pct
// response header, e.g. "0.42".
func formatMempoolPct(pct float64) string {
	return strconv.FormatFloat(pct, 'f', 2, 64)
}

// BackpressureResponse is the wire shape of GET /v1/chain/backpressure —
// the first-class mempool signal that lets clients pace their writes
// without polling raw CometBFT RPC.
type BackpressureResponse struct {
	MempoolTxs      int     `json:"mempool_txs"`
	MempoolBytes    int64   `json:"mempool_bytes"`
	MempoolMaxTxs   int     `json:"mempool_max_txs"`
	MempoolPct      float64 `json:"mempool_pct"`
	AcceptingWrites bool    `json:"accepting_writes"`
	RetryAfterMs    int     `json:"retry_after_ms"`
}

// handleChainBackpressure handles GET /v1/chain/backpressure.
// Served from the ~1s-TTL sampler cache, so clients may poll it tightly
// without amplifying load on the CometBFT RPC.
func (s *Server) handleChainBackpressure(w http.ResponseWriter, _ *http.Request) {
	smp, ok := s.mempool.sample()
	if !ok {
		writeProblem(w, http.StatusServiceUnavailable, "Backpressure unavailable",
			"Could not read mempool depth from the CometBFT RPC.")
		return
	}
	resp := BackpressureResponse{
		MempoolTxs:      smp.Txs,
		MempoolBytes:    smp.TotalBytes,
		MempoolMaxTxs:   smp.MaxTxs,
		MempoolPct:      smp.Pct,
		AcceptingWrites: smp.Pct < mempoolPressureThreshold,
	}
	if !resp.AcceptingWrites {
		resp.RetryAfterMs = mempoolRetryAfterMs
	}
	writeJSON(w, http.StatusOK, resp)
}
