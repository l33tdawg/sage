package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func clearAMIDEmbeddingEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SAGE_EMBEDDING_PROVIDER", "SAGE_EMBEDDING_BASE_URL",
		"SAGE_EMBEDDING_MODEL", "SAGE_EMBEDDING_API_KEY",
		"SAGE_EMBEDDING_DIMENSION", "OLLAMA_URL", "OLLAMA_MODEL",
	} {
		t.Setenv(key, "")
	}
}

func TestAMIDEmbeddingProviderPreservesLegacyDefaultSpace(t *testing.T) {
	clearAMIDEmbeddingEnv(t)

	provider, err := newAMIDEmbeddingProviderFromEnv()

	require.NoError(t, err)
	assert.Equal(t, "ollama", embedding.SpaceID(provider))
	assert.Equal(t, embedding.Dimension, provider.Dimension())
}

func TestAMIDEmbeddingProviderPropagatesCanonicalOllamaModelAndDimension(t *testing.T) {
	clearAMIDEmbeddingEnv(t)
	t.Setenv("OLLAMA_URL", "http://legacy.invalid")
	t.Setenv("OLLAMA_MODEL", "legacy-model")
	t.Setenv("SAGE_EMBEDDING_BASE_URL", "http://canonical.invalid")
	t.Setenv("SAGE_EMBEDDING_MODEL", "snowflake-arctic-embed:xs")
	t.Setenv("SAGE_EMBEDDING_DIMENSION", "384")

	provider, err := newAMIDEmbeddingProviderFromEnv()

	require.NoError(t, err)
	assert.Equal(t, 384, provider.Dimension())
	assert.Equal(t, "ollama:snowflake-arctic-embed:xs:384", embedding.SpaceID(provider))
}

func TestAMIDEmbeddingProviderRejectsSilentFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "invalid dimension",
			env:  map[string]string{"SAGE_EMBEDDING_DIMENSION": "three-eighty-four"},
			want: "positive integer",
		},
		{
			name: "unknown provider",
			env:  map[string]string{"SAGE_EMBEDDING_PROVIDER": "mystery"},
			want: "unsupported",
		},
		{
			name: "openai-compatible missing endpoint",
			env: map[string]string{
				"SAGE_EMBEDDING_PROVIDER": "openai-compatible",
				"SAGE_EMBEDDING_MODEL":    "small",
			},
			want: "BASE_URL is required",
		},
		{
			name: "openai-compatible missing model",
			env: map[string]string{
				"SAGE_EMBEDDING_PROVIDER": "openai-compatible",
				"SAGE_EMBEDDING_BASE_URL": "http://embedding.invalid",
			},
			want: "MODEL is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearAMIDEmbeddingEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			_, err := newAMIDEmbeddingProviderFromEnv()

			require.ErrorContains(t, err, tc.want)
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func rpcTestResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type recordingGovernanceDomainBinder struct {
	mu      sync.Mutex
	chainID string
	calls   int
}

type recordingRESTForkAccessors struct {
	postV8  func() bool
	postV17 func() bool
	postV20 func() bool
	postV22 func() bool
	postV23 func() bool
	postV27 func() bool
}

func (r *recordingRESTForkAccessors) SetPostV8ForkAccessor(fn func() bool) {
	r.postV8 = fn
}

func (r *recordingRESTForkAccessors) SetPostV17ForNextTxAccessor(fn func() bool) {
	r.postV17 = fn
}

func (r *recordingRESTForkAccessors) SetPostV20ForNextTxAccessor(fn func() bool) {
	r.postV20 = fn
}

func (r *recordingRESTForkAccessors) SetPostV22ForNextTxAccessor(fn func() bool) {
	r.postV22 = fn
}

func (r *recordingRESTForkAccessors) SetPostV23ForNextTxAccessor(fn func() bool) {
	r.postV23 = fn
}

func (r *recordingRESTForkAccessors) SetPostV27ForNextTxAccessor(fn func() bool) {
	r.postV27 = fn
}

type mutableAppForkAccessors struct {
	postV8  bool
	postV17 bool
	postV20 bool
	postV22 bool
	postV23 bool
	postV27 bool
}

func (a *mutableAppForkAccessors) IsPostV8Fork() bool            { return a.postV8 }
func (a *mutableAppForkAccessors) IsAppV17ActiveForNextTx() bool { return a.postV17 }
func (a *mutableAppForkAccessors) IsAppV20ActiveForNextTx() bool { return a.postV20 }
func (a *mutableAppForkAccessors) IsAppV22ActiveForNextTx() bool { return a.postV22 }
func (a *mutableAppForkAccessors) IsAppV23ActiveForNextTx() bool { return a.postV23 }
func (a *mutableAppForkAccessors) IsAppV27ActiveForNextTx() bool { return a.postV27 }

func TestWireRESTForkAccessorsIncludesAppV23AndStaysDynamic(t *testing.T) {
	server := &recordingRESTForkAccessors{}
	app := &mutableAppForkAccessors{}

	wireRESTForkAccessors(server, app)

	if server.postV8 == nil || server.postV17 == nil || server.postV20 == nil ||
		server.postV22 == nil || server.postV23 == nil || server.postV27 == nil {
		t.Fatal("amid did not wire every REST fork accessor")
	}
	if server.postV23() {
		t.Fatal("app-v23 accessor unexpectedly active before the app reports activation")
	}
	app.postV23 = true
	if !server.postV23() {
		t.Fatal("app-v23 REST accessor did not track the live app predicate")
	}
	app.postV27 = true
	if !server.postV27() {
		t.Fatal("app-v27 REST accessor did not track the live app predicate")
	}
}

func (b *recordingGovernanceDomainBinder) SetExpectedGovernanceDelegationDomain(chainID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.chainID != "" && b.chainID != chainID {
		return errors.New("governance domain already bound")
	}
	b.chainID = chainID
	return nil
}

func (b *recordingGovernanceDomainBinder) bindCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *recordingGovernanceDomainBinder) boundChainID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.chainID
}

func TestGovernanceDomainBindingRetriesUntilCometRPCIsReady(t *testing.T) {
	var requests atomic.Int32
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if requests.Add(1) < 3 {
			return rpcTestResponse(request, http.StatusServiceUnavailable, "starting"), nil
		}
		return rpcTestResponse(request, http.StatusOK, `{"result":{"node_info":{"network":"sage-v11-9-chaos"}}}`), nil
	})

	binder := &recordingGovernanceDomainBinder{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := bindExpectedGovernanceDomainFromRPCUntilReady(
		ctx,
		client,
		binder,
		"http://comet.test",
		time.Millisecond,
		5*time.Millisecond,
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("bind governance domain: %v", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("RPC requests = %d, want 3", got)
	}
	if got := binder.boundChainID(); got != "sage-v11-9-chaos" {
		t.Fatalf("bound chain ID = %q, want %q", got, "sage-v11-9-chaos")
	}
}

func TestGovernanceDomainBindingStopsOnContextCancellation(t *testing.T) {
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return rpcTestResponse(request, http.StatusServiceUnavailable, "starting"), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := bindExpectedGovernanceDomainFromRPCUntilReady(
		ctx,
		client,
		&recordingGovernanceDomainBinder{},
		"http://comet.test",
		time.Millisecond,
		5*time.Millisecond,
		zerolog.Nop(),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("binding error = %v, want context deadline exceeded", err)
	}
}

func TestConfigureGovernanceDomainRejectsUnusableRPCResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "non-200", status: http.StatusServiceUnavailable, body: "starting"},
		{name: "malformed JSON", status: http.StatusOK, body: `{"result":`},
		{name: "missing network", status: http.StatusOK, body: `{"result":{"node_info":{}}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newGovernanceDomainHTTPClient()
			client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return rpcTestResponse(request, tc.status, tc.body), nil
			})
			binder := &recordingGovernanceDomainBinder{}
			_, err := configureExpectedGovernanceDomainFromRPC(
				context.Background(),
				client,
				binder,
				"http://comet.test",
			)
			if err == nil {
				t.Fatal("unusable CometBFT response unexpectedly bound a governance domain")
			}
			if calls := binder.bindCalls(); calls != 0 {
				t.Fatalf("binder calls = %d, want 0", calls)
			}
		})
	}
}

func TestConfigureGovernanceDomainRefusesHTTPRedirects(t *testing.T) {
	var redirectedRequests atomic.Int32
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "comet.test" {
			response := rpcTestResponse(request, http.StatusTemporaryRedirect, "")
			response.Header.Set("Location", "https://wrong-authority.test/status")
			return response, nil
		}
		redirectedRequests.Add(1)
		return rpcTestResponse(request, http.StatusOK, `{"result":{"node_info":{"network":"wrong-authority"}}}`), nil
	})

	binder := &recordingGovernanceDomainBinder{}
	_, err := configureExpectedGovernanceDomainFromRPC(
		context.Background(),
		client,
		binder,
		"http://comet.test",
	)
	if err == nil {
		t.Fatal("redirected CometBFT authority unexpectedly bound a governance domain")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("redirect target requests = %d, want 0", got)
	}
	if calls := binder.bindCalls(); calls != 0 {
		t.Fatalf("binder calls = %d, want 0", calls)
	}
}

// amidOperatorTestKey derives a deterministic Ed25519 key for the amid
// operator-surface fixtures.
func amidOperatorTestKey(label string) ed25519.PrivateKey {
	seed := sha256.Sum256([]byte(label))
	return ed25519.NewKeyFromSeed(seed[:])
}

type amidOperatorFixture struct {
	badger   *store.BadgerStore
	agents   store.OffchainStore
	rootKey  ed25519.PrivateKey
	rootID   string
	agentKey ed25519.PrivateKey
	agentID  string
}

// newAmidOperatorFixture stands up the smallest post-app-v23 chain the two
// operator routes accept: a committed Root, one active companion agent with a
// home domain, and the off-chain projection row the state read joins.
func newAmidOperatorFixture(t *testing.T) amidOperatorFixture {
	t.Helper()
	ctx := context.Background()
	rootKey := amidOperatorTestKey("amid-operator-root")
	agentKey := amidOperatorTestKey("amid-operator-agent")
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	agents, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "agents.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, agents.Close()) })

	rootID := hex.EncodeToString(rootKey.Public().(ed25519.PublicKey))
	agentID := hex.EncodeToString(agentKey.Public().(ed25519.PublicKey))
	require.NoError(t, badgerStore.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "amid-operator-test", AgentID: agentID,
		Profile: store.AppV23ProfileCompanion, HomeDomain: "amid-operator-home",
		Clearance: 1, Capabilities: 15, Height: 1, BootstrapDigest: "fixture",
	}))
	require.NoError(t, agents.CreateAgent(ctx, &store.AgentEntry{
		AgentID: agentID, Name: "audit-writer", Role: "member", Status: "active", Clearance: 1,
	}))
	return amidOperatorFixture{
		badger: badgerStore, agents: agents,
		rootKey: rootKey, rootID: rootID, agentKey: agentKey, agentID: agentID,
	}
}

// amidOperatorRPCStub answers the commit broadcast with a hash-bound success
// envelope (the shape a real node returns) and captures the parsed transaction.
func amidOperatorRPCStub(t *testing.T, captured **tx.ParsedTx) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, err := hex.DecodeString(raw)
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(encoded)
		require.NoError(t, err)
		*captured = parsed
		sum := tx.CometTxHash(encoded)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(
			w,
			`{"result":{"check_tx":{"code":0,"log":""},"tx_result":{"code":0,"log":""},"hash":%q,"height":"42"}}`,
			strings.ToUpper(hex.EncodeToString(sum[:])),
		)
	}))
}

func amidOperatorRequest(t *testing.T, method, path string, body any) (*http.Request, []byte) {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Content-Type", "application/json")
	return req, encoded
}

// signAmidOperatorRequest signs the exact request the way an operator script
// does on the node host: the same canonical request signature the REST
// middleware verifies, carried in the dashboard headers.
func signAmidOperatorRequest(t *testing.T, req *http.Request, key ed25519.PrivateKey, body []byte) {
	t.Helper()
	ts := time.Now().Unix()
	nonce := make([]byte, 8)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	req.Header.Set("X-Agent-ID", hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(
		auth.SignRequestWithNonce(key, req.Method, req.URL.RequestURI(), body, ts, nonce),
	))
}

func amidOperatorPolicyBody(clearance uint8) map[string]any {
	return map[string]any{
		"role": "member", "profile": "companion",
		"clearance": clearance, "capabilities": 15,
	}
}

func TestAmidOperatorRoutesServeTheAppV23AccessPair(t *testing.T) {
	fixture := newAmidOperatorFixture(t)
	var captured *tx.ParsedTx
	rpc := amidOperatorRPCStub(t, &captured)
	defer rpc.Close()

	router := chi.NewRouter()
	wireAmidOperatorRoutes(
		router, fixture.agents, fixture.badger, rpc.URL, "amid-test",
		func() bool { return true },
		func(id string) (ed25519.PrivateKey, bool) {
			return fixture.rootKey, id == fixture.rootID
		},
	)

	// The read surface: an operator script discovers the exact state the write
	// has to echo back.
	readReq, _ := amidOperatorRequest(t, http.MethodGet, "/v1/dashboard/network/access", nil)
	signAmidOperatorRequest(t, readReq, fixture.rootKey, nil)
	readRec := httptest.NewRecorder()
	router.ServeHTTP(readRec, readReq)
	require.Equal(t, http.StatusOK, readRec.Code, readRec.Body.String())
	require.Contains(t, readRec.Body.String(), fixture.agentID)

	// The write surface: a Root-signed policy update reaches consensus as the
	// same TxTypeAgentRoleChange CEREBRUM emits, with the reserved clearance.
	body := amidOperatorPolicyBody(2)
	writeReq, encoded := amidOperatorRequest(
		t, http.MethodPut,
		"/v1/dashboard/network/access/agents/"+fixture.agentID+"/policy", body,
	)
	signAmidOperatorRequest(t, writeReq, fixture.rootKey, encoded)
	writeRec := httptest.NewRecorder()
	router.ServeHTTP(writeRec, writeReq)
	require.Equal(t, http.StatusOK, writeRec.Code, writeRec.Body.String())

	require.NotNil(t, captured)
	assert.Equal(t, tx.TxTypeAgentRoleChange, captured.Type)
	require.NotNil(t, captured.AgentRoleChange)
	assert.Equal(t, fixture.agentID, captured.AgentRoleChange.AgentID)
	assert.Equal(t, "member", captured.AgentRoleChange.Role)
	assert.Equal(t, uint8(2), captured.AgentRoleChange.Clearance)

	// An unsigned loopback request never reaches the handlers: the mounted pair
	// keeps the CEREBRUM operator gate.
	unsignedReq, _ := amidOperatorRequest(
		t, http.MethodPut,
		"/v1/dashboard/network/access/agents/"+fixture.agentID+"/policy", body,
	)
	unsignedRec := httptest.NewRecorder()
	router.ServeHTTP(unsignedRec, unsignedReq)
	require.Equal(t, http.StatusForbidden, unsignedRec.Code, unsignedRec.Body.String())
}
