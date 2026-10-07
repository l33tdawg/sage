package federation

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

var errNodeContactCursor = errors.New("invalid or expired agent cursor")

// The scan can cross a filtered Root/ineligible row. Encrypt the position so
// continuation metadata cannot disclose an identity omitted from the directory.
// Agreement-associated data prevents replay against another peer or pairing.
func (m *Manager) nodeCursorCipher() (cipher.AEAD, error) {
	if len(m.agentKey) != ed25519.PrivateKeySize {
		return nil, errNodeContactCursor
	}
	key := sha256.Sum256(append([]byte("sage-node-contact-cursor-v1\x00"), m.agentKey.Seed()...))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func validNodeCursorShape(value string) bool {
	if len(value) > 192 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(raw) == 12+64+8+16
}

func (m *Manager) sealNodeCursor(peer *peerIdentity, policy *store.PeerRBACPolicy, agentID string) (string, error) {
	box, err := m.nodeCursorCipher()
	if err != nil {
		return "", err
	}
	plain := make([]byte, 72)
	copy(plain, agentID)
	binary.BigEndian.PutUint64(plain[64:], uint64(time.Now().Add(30*time.Minute).Unix()))
	nonce := make([]byte, box.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := box.Seal(nonce, nonce, plain, []byte(m.pipeContactAgreementID(peer, policy)))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (m *Manager) openNodeCursor(peer *peerIdentity, policy *store.PeerRBACPolicy, value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !validNodeCursorShape(value) {
		return "", errNodeContactCursor
	}
	raw, _ := base64.RawURLEncoding.DecodeString(value)
	box, err := m.nodeCursorCipher()
	if err != nil {
		return "", err
	}
	plain, err := box.Open(nil, raw[:box.NonceSize()], raw[box.NonceSize():], []byte(m.pipeContactAgreementID(peer, policy)))
	if err != nil || len(plain) != 72 {
		return "", errNodeContactCursor
	}
	if binary.BigEndian.Uint64(plain[64:]) <= uint64(time.Now().Unix()) || !isCanonicalAgentID(string(plain[:64])) {
		return "", errNodeContactCursor
	}
	return string(plain[:64]), nil
}

func (m *Manager) buildNodeContactPage(ctx context.Context, peer *peerIdentity, policy *store.PeerRBACPolicy, after string, limit int) (*PipeContactGrant, error) {
	position, err := m.openNodeCursor(peer, policy, after)
	if err != nil {
		return nil, err
	}
	if limit < 1 || limit > maxPipeContactStatusCandidates {
		return nil, fmt.Errorf("invalid agent page limit")
	}
	agents, err := m.syncStore().ListNodeContactCandidates(ctx, position, limit+1)
	if err != nil {
		return nil, err
	}
	next := ""
	if len(agents) > limit {
		next, err = m.sealNodeCursor(peer, policy, agents[limit-1].AgentID)
		if err != nil {
			return nil, err
		}
		agents = agents[:limit]
	}
	grant, err := m.buildPipeContactGrantForCandidates(ctx, peer, policy, agents, nil, false, nil, true, NodeMessageAuthorizationMode)
	if err != nil {
		return nil, err
	}
	grant.NextCursor = next
	return grant, nil
}

// ListRemoteNodeContacts uses one negotiated, authenticated bounded request.
// A cursor is a roster position, never an authorization token.
func (m *Manager) ListRemoteNodeContacts(ctx context.Context, chain string, status *StatusResponse, after string) (*PipeContactLookupResponse, error) {
	if nodeMessageMode(status) == "" {
		return nil, fmt.Errorf("peer requires an update for automatic agent discovery")
	}
	agreement, err := m.ActiveAgreement(chain)
	if err != nil {
		return nil, err
	}
	return m.lookupRemotePipeContacts(ctx, agreement, &PipeContactLookupRequest{
		AuthorizationMode: NodeMessageAuthorizationMode, List: true, After: after, Limit: maxPipeContactLookupResults,
	})
}

// LocalNodeContacts is the operator's view of automatic messaging membership.
// Memory export controls continue to use LocalPipeContacts independently.
func (m *Manager) LocalNodeContacts(ctx context.Context, chain, after string) (*PipeContactGrant, error) {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		return nil, nil
	}
	return m.localPipeContacts(ctx, chain, "", NodeMessageAuthorizationMode, after)
}
