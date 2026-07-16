package federation

import (
	"context"
	"errors"
	"net/http"
)

const (
	// CapabilityWrite reserves the mixed-version wire label, but v11.9 never
	// advertises it. A reusable ordinary AccessGrant is agent-wide and therefore
	// cannot honestly implement a permission scoped to one trusted peer link.
	CapabilityWrite = "write-v1"
)

// ErrRemoteWriteCapabilityUnavailable keeps preview callers fail-closed and
// explicit until consensus has a one-shot ingress capability bound to the
// active ceremony generation, frozen peer, domain, and exact submission.
var ErrRemoteWriteCapabilityUnavailable = errors.New("federation write requires a consensus-bound ingress capability and is unavailable in v11.9")

// RemoteWriteHeaders preserves the reserved write-v1 envelope for
// mixed-version compatibility. v11.9 never dispatches these credentials to
// /v1/memory/submit.
type RemoteWriteHeaders struct {
	AgentID   string `json:"x_agent_id"`
	Signature string `json:"x_signature"`
	Timestamp int64  `json:"x_timestamp"`
	Nonce     string `json:"x_nonce"`
}

type RemoteWriteRequest struct {
	Headers RemoteWriteHeaders `json:"headers"`
	Body    []byte             `json:"body"`
}

type RemoteWriteResult struct {
	StatusCode int
	Body       []byte
}

// WritePeer never sends the reserved envelope. Returning the typed error before
// agreement lookup or dialing prevents callers from mistaking this for a
// transient network failure.
func (m *Manager) WritePeer(context.Context, string, *RemoteWriteRequest) (*RemoteWriteResult, error) {
	return nil, ErrRemoteWriteCapabilityUnavailable
}

// handleRemoteWrite remains mounted behind peerAuth so preview clients get an
// authenticated explicit 501. It intentionally does not parse the body: even a
// valid peer credential plus ordinary AccessGrant cannot reach local submit.
func (m *Manager) handleRemoteWrite(w http.ResponseWriter, _ *http.Request) {
	httpError(w, http.StatusNotImplemented, ErrRemoteWriteCapabilityUnavailable.Error())
}
