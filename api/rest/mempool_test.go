package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMempoolMock returns a fake CometBFT RPC serving /num_unconfirmed_txs
// with a fixed depth. hits (optional) counts upstream probes so tests can
// prove the TTL cache absorbs repeat callers.
func newMempoolMock(t *testing.T, nTxs int, totalBytes int64, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/num_unconfirmed_txs" {
			http.NotFound(w, r)
			return
		}
		if hits != nil {
			hits.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		// CometBFT encodes int64 fields as JSON strings.
		fmt.Fprintf(w, `{"result":{"n_txs":"%d","total":"%d","total_bytes":"%d"}}`, nTxs, nTxs, totalBytes)
	}))
}

func TestMempoolSamplerDefaults(t *testing.T) {
	smp := newMempoolSampler("http://127.0.0.1:1", 0)
	assert.Equal(t, mempoolSampleTTL, smp.ttl)
	assert.Equal(t, time.Second, smp.ttl, "TTL must stay ~1s so a polling loop costs at most 1 RPC/sec")
}

func TestMempoolSamplerCachesWithinTTL(t *testing.T) {
	var hits atomic.Int64
	mock := newMempoolMock(t, 2100, 4096, &hits)
	defer mock.Close()

	smp := newMempoolSampler(mock.URL, DefaultMempoolMaxTxs)
	smp.ttl = time.Hour // deterministic: everything below is "within TTL"

	for i := 0; i < 5; i++ {
		s, ok := smp.sample()
		require.True(t, ok)
		assert.Equal(t, 2100, s.Txs)
		assert.Equal(t, int64(4096), s.TotalBytes)
		assert.Equal(t, 5000, s.MaxTxs, "cap must be the runtime CometBFT default, not the dead config.toml 1000")
		assert.InDelta(t, 0.42, s.Pct, 1e-9)
	}
	assert.Equal(t, int64(1), hits.Load(), "repeat samples within the TTL must cost exactly one upstream probe")
}

func TestMempoolSamplerRefreshesAfterTTL(t *testing.T) {
	var hits atomic.Int64
	mock := newMempoolMock(t, 10, 100, &hits)
	defer mock.Close()

	smp := newMempoolSampler(mock.URL, DefaultMempoolMaxTxs)
	smp.ttl = 10 * time.Millisecond

	_, ok := smp.sample()
	require.True(t, ok)
	time.Sleep(25 * time.Millisecond)
	_, ok = smp.sample()
	require.True(t, ok)
	assert.Equal(t, int64(2), hits.Load(), "a stale cache must trigger a fresh probe")
}

func TestMempoolSamplerPctMath(t *testing.T) {
	cases := []struct {
		nTxs   int
		maxTxs int
		want   float64
	}{
		{0, 5000, 0.0},
		{2500, 5000, 0.5},
		{5000, 5000, 1.0},
		{500, 1000, 0.5}, // SetMempoolCap-style override
	}
	for _, tc := range cases {
		mock := newMempoolMock(t, tc.nTxs, 0, nil)
		smp := newMempoolSampler(mock.URL, tc.maxTxs)
		s, ok := smp.sample()
		require.True(t, ok)
		assert.InDelta(t, tc.want, s.Pct, 1e-9, "n_txs=%d cap=%d", tc.nTxs, tc.maxTxs)
		mock.Close()
	}
}

// A down/erroring RPC must be probed at most once per TTL (negative caching), so a
// tight poll on /v1/chain/backpressure or a burst of submits doesn't storm a sick RPC.
func TestMempoolSampler_NegativeCacheThrottlesFailures(t *testing.T) {
	var hits atomic.Int64
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError) // empty body -> fetch decode fails
	}))
	defer mock.Close()
	smp := newMempoolSampler(mock.URL, DefaultMempoolMaxTxs)
	for i := 0; i < 5; i++ {
		if _, ok := smp.sample(); ok {
			t.Fatalf("call %d: expected ok=false on a failing RPC", i)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("a failing RPC must be probed once per TTL (negative cache), got %d probes", got)
	}
}

func TestMempoolSamplerUnreachableRPC(t *testing.T) {
	smp := newMempoolSampler("http://127.0.0.1:1", DefaultMempoolMaxTxs) // nothing listens
	_, ok := smp.sample()
	assert.False(t, ok, "unreachable RPC must report ok=false, never fabricated depth")
}

func TestBackpressureEndpointShape(t *testing.T) {
	mock := newMempoolMock(t, 2100, 4096, nil)
	defer mock.Close()

	srv, _, _ := newTestServer(t, mock.URL)
	req, _ := signedRequest(t, http.MethodGet, "/v1/chain/backpressure", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	// Decode into a map to pin the exact wire field names.
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, float64(2100), resp["mempool_txs"])
	assert.Equal(t, float64(4096), resp["mempool_bytes"])
	assert.Equal(t, float64(5000), resp["mempool_max_txs"])
	assert.InDelta(t, 0.42, resp["mempool_pct"].(float64), 1e-9)
	assert.Equal(t, true, resp["accepting_writes"])
	assert.Equal(t, float64(0), resp["retry_after_ms"])
}

func TestBackpressureEndpointNearCap(t *testing.T) {
	mock := newMempoolMock(t, 4600, 1<<20, nil) // 4600/5000 = 0.92 >= 0.9
	defer mock.Close()

	srv, _, _ := newTestServer(t, mock.URL)
	req, _ := signedRequest(t, http.MethodGet, "/v1/chain/backpressure", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BackpressureResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.False(t, resp.AcceptingWrites)
	assert.Greater(t, resp.RetryAfterMs, 0, "near cap the response must carry a positive back-off hint")
}

func TestBackpressureEndpointHonorsSetMempoolCap(t *testing.T) {
	mock := newMempoolMock(t, 950, 0, nil)
	defer mock.Close()

	srv, _, _ := newTestServer(t, mock.URL)
	srv.SetMempoolCap(1000) // node.go passes cometCfg.Mempool.Size the same way

	req, _ := signedRequest(t, http.MethodGet, "/v1/chain/backpressure", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp BackpressureResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1000, resp.MempoolMaxTxs)
	assert.InDelta(t, 0.95, resp.MempoolPct, 1e-9)
	assert.False(t, resp.AcceptingWrites)
}

func TestBackpressureEndpointRPCDown(t *testing.T) {
	srv, _, _ := newTestServer(t, "http://127.0.0.1:1")
	req, _ := signedRequest(t, http.MethodGet, "/v1/chain/backpressure", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")
}

func TestSubmitMemorySetsMempoolPctHeader(t *testing.T) {
	// One mock serving both the broadcast (success) and the depth probe.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/num_unconfirmed_txs" {
			fmt.Fprint(w, `{"result":{"n_txs":"2100","total":"2100","total_bytes":"4096"}}`)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"check_tx":  map[string]interface{}{"code": 0, "log": ""},
				"tx_result": map[string]interface{}{"code": 0, "data": "", "log": ""},
				"hash":      "MEMPOOLPCT",
				"height":    "1",
			},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{
		"content": "backpressure header test",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.9
	}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.Equal(t, "0.42", rr.Header().Get("X-Sage-Mempool-Pct"),
		"2100 of the 5000-tx runtime cap must surface as 0.42")
}

func TestSubmitMemoryMempoolFullReturns429(t *testing.T) {
	// CometBFT rejects a broadcast into a full mempool with a JSON-RPC error
	// whose message is just "Internal error" — the real cause lives in
	// error.data. This previously fell through to an opaque 500.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"error":{"code":-32603,"message":"Internal error","data":"mempool is full: number of txs 5000 (max: 5000)"}}`)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{
		"content": "mempool full test",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.9
	}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTooManyRequests, rr.Code)
	assert.Equal(t, "1", rr.Header().Get("Retry-After"), "mempool-full must carry a Retry-After hint")

	var problem map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &problem))
	assert.Equal(t, mempoolFullProblemType, problem["type"],
		"problem type must be distinct from the rate limiter's status-derived type")
	assert.NotEqual(t, "https://sage.dev/errors/429", problem["type"])
	assert.Equal(t, "Mempool full", problem["title"])
	assert.Equal(t, "mempool full, retry later", problem["detail"])
}

func TestBroadcastErrorPublicMempoolFull(t *testing.T) {
	status, msg := broadcastErrorPublic(
		fmt.Errorf("broadcast error: Internal error: mempool is full: number of txs 5000 (max: 5000)"))
	assert.Equal(t, http.StatusTooManyRequests, status)
	assert.Equal(t, "mempool full, retry later", msg)

	// Unrelated broadcast errors keep the opaque default.
	status, msg = broadcastErrorPublic(fmt.Errorf("broadcast error: Internal error"))
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "internal error", msg)
}
