package web

// Guest side of the LAN node-join ceremony driven from the JOINING node's own
// CEREBRUM ("Join a network"). Unlike the CLI, this node is already running its
// own SAGE, so it cannot wipe+adopt in-process. Instead the driver runs the
// ceremony (prove S → wait for host approval → fetch + decrypt the bundle),
// STAGES the decrypted bundle via WritePendingJoinFn, and the frontend then
// restarts — the staged join is applied at startup before any store opens.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/pairing"
)

const (
	guestJoinStateConnecting = "connecting"
	guestJoinStateAwaiting   = "awaiting_approval"
	guestJoinStateReady      = "ready" // bundle staged; restart to finish
	guestJoinStateError      = "error"
	guestJoinStateCancelled  = "cancelled"
	guestJoinPollTimeout     = 5 * time.Minute
)

// guestJoinSession is the JOINING node's single in-flight ceremony.
type guestJoinSession struct {
	state   string
	sas     string
	chainID string
	errMsg  string
	cancel  context.CancelFunc
}

// GuestJoinStore holds the one active guest-side join session.
type GuestJoinStore struct {
	mu     sync.Mutex
	active *guestJoinSession
}

// NewGuestJoinStore creates an empty store.
func NewGuestJoinStore() *GuestJoinStore { return &GuestJoinStore{} }

// RegisterNetworkJoinGuestRoutes wires the joining-node ceremony routes. Must be
// called inside the authenticated (and same-origin gated) dashboard group.
func (h *DashboardHandler) RegisterNetworkJoinGuestRoutes(r chi.Router) {
	r.Post("/v1/dashboard/network/join/guest/start", h.handleGuestJoinStart)
	r.Get("/v1/dashboard/network/join/guest/status", h.handleGuestJoinStatus)
	r.Post("/v1/dashboard/network/join/guest/cancel", h.handleGuestJoinCancel)
	r.Post("/v1/dashboard/network/join/guest/restart", h.handleGuestJoinRestart)
}

// setState updates sess's state under lock, but ONLY if it is still the active
// session — a poller from a cancelled or superseded attempt must not mutate the
// replacement (which would corrupt state or stage the wrong bundle).
func (s *GuestJoinStore) setState(sess *guestJoinSession, state, sas, chainID, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != sess {
		return
	}
	sess.state = state
	if sas != "" {
		sess.sas = sas
	}
	if chainID != "" {
		sess.chainID = chainID
	}
	sess.errMsg = errMsg
}

// handleGuestJoinStart decodes the pairing token, performs the hello handshake
// to obtain the SAS, then spawns a background poller that waits for host
// approval and stages the decrypted bundle.
func (h *DashboardHandler) handleGuestJoinStart(w http.ResponseWriter, r *http.Request) {
	if h.GuestJoin == nil || h.GuestNodeIDFn == nil || h.WritePendingJoinFn == nil {
		writeError(w, http.StatusServiceUnavailable, "network join not available on this node")
		return
	}
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	tok, secret, err := pairing.DecodeToken(strings.TrimSpace(req.Token))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid pairing code: "+err.Error())
		return
	}
	nodeID, err := h.GuestNodeIDFn()
	if err != nil || nodeID == "" {
		writeError(w, http.StatusInternalServerError, "cannot read this node's identity")
		return
	}

	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "nonce")
		return
	}
	nonce := hex.EncodeToString(nonceBytes)
	guestName, _ := os.Hostname()

	// hello (synchronous) — obtain the SAS to show the operator immediately.
	base := "http://" + tok.Addr
	helloBody, _ := json.Marshal(map[string]string{
		"session_id":    tok.SessionID,
		"guest_node_id": nodeID,
		"guest_name":    guestName,
		"guest_nonce":   nonce,
		"proof":         pairing.ProofHello(secret, tok.SessionID, nonce, nodeID),
	})
	var hello struct {
		SAS   string `json:"sas"`
		Error string `json:"error"`
	}
	code, herr := guestPost(base+"/pair/hello", helloBody, &hello)
	if herr != nil {
		writeError(w, http.StatusBadGateway, "could not reach the host: "+herr.Error())
		return
	}
	if code != http.StatusOK {
		writeError(w, http.StatusBadGateway, "host rejected pairing: "+firstNonEmptyStr(hello.Error, http.StatusText(code)))
		return
	}
	localSAS := pairing.SAS(secret, tok.SessionID, nonce)
	if hello.SAS != localSAS {
		writeError(w, http.StatusBadGateway, "security check failed — the codes do not match; aborting")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	sess := &guestJoinSession{state: guestJoinStateAwaiting, sas: localSAS, cancel: cancel}
	h.GuestJoin.mu.Lock()
	if h.GuestJoin.active != nil && h.GuestJoin.active.cancel != nil {
		h.GuestJoin.active.cancel() // cancel any prior attempt
	}
	h.GuestJoin.active = sess
	h.GuestJoin.mu.Unlock()

	go h.pollForBundle(ctx, sess, base, tok.SessionID, secret)

	writeJSONResp(w, http.StatusOK, map[string]any{"sas": localSAS})
}

// pollForBundle waits for the host operator to approve, then fetches + decrypts
// + stages the bundle. Runs in the background; only mutates its OWN session.
func (h *DashboardHandler) pollForBundle(ctx context.Context, sess *guestJoinSession, base, sessionID string, secret []byte) {
	proof := pairing.ProofBundle(secret, sessionID)
	body, _ := json.Marshal(map[string]string{"session_id": sessionID, "proof": proof})
	deadline := time.Now().Add(guestJoinPollTimeout)

	sleep := func() bool { // returns false if cancelled
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
			return true
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			h.GuestJoin.setState(sess, guestJoinStateError, "", "", "timed out waiting for the host to approve")
			return
		}
		var enc struct {
			Nonce      string `json:"nonce"`
			Ciphertext string `json:"ciphertext"`
			State      string `json:"state"`
			Error      string `json:"error"`
		}
		code, err := guestPost(base+"/pair/bundle", body, &enc)
		if err != nil {
			if !sleep() { // transient network error — keep trying until the deadline
				return
			}
			continue
		}
		if code == http.StatusOK {
			bundleJSON, derr := pairing.DecryptBundle(secret, enc.Nonce, enc.Ciphertext)
			if derr != nil {
				h.GuestJoin.setState(sess, guestJoinStateError, "", "", "could not decrypt the bundle: "+derr.Error())
				return
			}
			// Don't stage a bundle for a cancelled/superseded attempt.
			if ctx.Err() != nil {
				return
			}
			if werr := h.WritePendingJoinFn(bundleJSON); werr != nil {
				h.GuestJoin.setState(sess, guestJoinStateError, "", "", "could not stage the join: "+werr.Error())
				return
			}
			var meta struct {
				ChainID string `json:"chain_id"`
			}
			_ = json.Unmarshal(bundleJSON, &meta)
			h.GuestJoin.setState(sess, guestJoinStateReady, "", meta.ChainID, "")
			return
		}
		if code == http.StatusAccepted || code == http.StatusTooManyRequests {
			if !sleep() {
				return
			}
			continue
		}
		h.GuestJoin.setState(sess, guestJoinStateError, "", "", "host declined: "+firstNonEmptyStr(enc.Error, http.StatusText(code)))
		return
	}
}

// handleGuestJoinStatus reports the joining node's ceremony state for the poll.
func (h *DashboardHandler) handleGuestJoinStatus(w http.ResponseWriter, r *http.Request) {
	if h.GuestJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "network join not available")
		return
	}
	h.GuestJoin.mu.Lock()
	defer h.GuestJoin.mu.Unlock()
	if h.GuestJoin.active == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"state": "none"})
		return
	}
	a := h.GuestJoin.active
	writeJSONResp(w, http.StatusOK, map[string]any{
		"state":    a.state,
		"sas":      a.sas,
		"chain_id": a.chainID,
		"error":    a.errMsg,
	})
}

// handleGuestJoinCancel aborts the in-flight ceremony.
func (h *DashboardHandler) handleGuestJoinCancel(w http.ResponseWriter, r *http.Request) {
	if h.GuestJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "network join not available")
		return
	}
	h.GuestJoin.mu.Lock()
	if h.GuestJoin.active != nil {
		if h.GuestJoin.active.cancel != nil {
			h.GuestJoin.active.cancel()
		}
		h.GuestJoin.active = nil
	}
	h.GuestJoin.mu.Unlock()
	// Cancel is a genuine backout: if the ceremony already staged the (armed,
	// destructive) join, un-stage it so a later restart doesn't silently apply
	// a join the user backed out of.
	if h.RemovePendingJoinFn != nil {
		_ = h.RemovePendingJoinFn()
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"state": guestJoinStateCancelled})
}

// handleGuestJoinRestart re-execs so applyPendingJoinAtStartup finishes the join.
// Valid only once the bundle is staged (state == ready).
func (h *DashboardHandler) handleGuestJoinRestart(w http.ResponseWriter, r *http.Request) {
	if h.GuestJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "network join not available")
		return
	}
	h.GuestJoin.mu.Lock()
	ready := h.GuestJoin.active != nil && h.GuestJoin.active.state == guestJoinStateReady
	h.GuestJoin.mu.Unlock()
	if !ready {
		writeError(w, http.StatusConflict, "no staged join to apply")
		return
	}
	execPath, err := os.Executable()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine binary path")
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "message": "Joining and restarting..."})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		syscall.Exec(execPath, os.Args, os.Environ()) //nolint:errcheck,gosec // execPath is the verified current binary
	}()
}

// guestPost POSTs JSON to the host pairing listener and decodes the response.
func guestPost(url string, body []byte, out any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode, nil
}

func firstNonEmptyStr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
