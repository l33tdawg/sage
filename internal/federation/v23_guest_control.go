package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	FederatedGuestOperationAttach = "attach"
	FederatedGuestOperationPause  = "pause"
	FederatedGuestOperationRevoke = "revoke"
	FederatedGuestOperationRebind = "rebind"

	federatedGuestElevationScope = "sage-federated-guest-admin-v23"
	federatedGuestElevationTTL   = 60 * time.Second
)

type FederatedGuestMutationInput struct {
	Operation         string `json:"operation"`
	GroupID           string `json:"group_id"`
	RemoteChainID     string `json:"remote_chain_id"`
	RemoteAgentID     string `json:"remote_agent_id"`
	MaxClassification uint8  `json:"max_classification,omitempty"`
	ExpectedRevision  int64  `json:"expected_revision"`
}

type PreparedFederatedGuestMutation struct {
	Operation        string                    `json:"operation"`
	ExpectedRevision int64                     `json:"expected_revision"`
	Guest            store.FederatedGroupGuest `json:"guest"`
	SigningBytes     []byte                    `json:"signing_bytes"`
	ActionDigest     string                    `json:"action_digest"`
}

type FederatedGuestElevation struct {
	Scope            string `json:"scope"`
	RootCredentialID string `json:"root_credential_id"`
	RootGeneration   uint64 `json:"root_generation"`
	AdminID          string `json:"admin_id"`
	Operation        string `json:"operation"`
	ActionDigest     string `json:"action_digest"`
	Nonce            string `json:"nonce"`
	IssuedAt         int64  `json:"issued_at"`
	ExpiresAt        int64  `json:"expires_at"`
	Signature        []byte `json:"signature"`
}

type FederatedGuestMutation struct {
	Operation        string                    `json:"operation"`
	ExpectedRevision int64                     `json:"expected_revision"`
	Guest            store.FederatedGroupGuest `json:"guest"`
	Elevation        *FederatedGuestElevation  `json:"elevation,omitempty"`
}

type FederatedGuestLinkView struct {
	Guest          store.FederatedGroupGuest `json:"guest"`
	EffectiveState string                    `json:"effective_state"`
	BindingCurrent bool                      `json:"binding_current"`
}

type federatedGuestElevationSigned struct {
	Scope            string `json:"scope"`
	RootCredentialID string `json:"root_credential_id"`
	RootGeneration   uint64 `json:"root_generation"`
	AdminID          string `json:"admin_id"`
	Operation        string `json:"operation"`
	ActionDigest     string `json:"action_digest"`
	Nonce            string `json:"nonce"`
	IssuedAt         int64  `json:"issued_at"`
	ExpiresAt        int64  `json:"expires_at"`
}

func (e *FederatedGuestElevation) signingBytes() ([]byte, error) {
	if e == nil {
		return nil, errors.New("federated guest elevation is required")
	}
	return json.Marshal(federatedGuestElevationSigned{
		Scope: e.Scope, RootCredentialID: e.RootCredentialID,
		RootGeneration: e.RootGeneration, AdminID: e.AdminID,
		Operation: e.Operation, ActionDigest: e.ActionDigest, Nonce: e.Nonce,
		IssuedAt: e.IssuedAt, ExpiresAt: e.ExpiresAt,
	})
}

func federatedGuestActionDigest(operation string, expectedRevision int64, guest store.FederatedGroupGuest) (string, error) {
	guestBytes, err := guest.SigningBytes()
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		Purpose          string `json:"purpose"`
		Operation        string `json:"operation"`
		ExpectedRevision int64  `json:"expected_revision"`
		Guest            []byte `json:"guest"`
	}{
		Purpose: "sage-federated-guest-action-v23", Operation: operation,
		ExpectedRevision: expectedRevision, Guest: guestBytes,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func validFederatedGuestOperation(operation string) bool {
	switch operation {
	case FederatedGuestOperationAttach, FederatedGuestOperationPause,
		FederatedGuestOperationRevoke, FederatedGuestOperationRebind:
		return true
	default:
		return false
	}
}

func (m *Manager) isCurrentRootCredential(agentID string) bool {
	if m.badger == nil {
		return false
	}
	root, err := m.badger.GetAppV23Root()
	return err == nil && root != nil && root.CredentialID == agentID &&
		m.localAdminSignerActive(agentID)
}

func (m *Manager) IsCurrentFederationRoot(_ context.Context, agentID string) bool {
	return m.isCurrentRootCredential(agentID)
}

// SetRootKeyResolver installs the runtime credential-vault lookup used after
// app-v23 root rotation. It is safe to replace during node operation.
func (m *Manager) SetRootKeyResolver(resolver func(string) (ed25519.PrivateKey, bool)) {
	m.rootKeyResolverMu.Lock()
	m.rootKeyResolver = resolver
	m.rootKeyResolverMu.Unlock()
}

func (m *Manager) currentRootSigningKey(credentialID string) (ed25519.PrivateKey, bool) {
	m.rootKeyResolverMu.RLock()
	resolver := m.rootKeyResolver
	m.rootKeyResolverMu.RUnlock()
	if resolver != nil {
		if key, ok := resolver(credentialID); ok {
			pub, pubOK := key.Public().(ed25519.PublicKey)
			if pubOK && hex.EncodeToString(pub) == credentialID {
				return key, true
			}
		}
		return nil, false
	}
	if hex.EncodeToString(m.agentPub) == credentialID {
		return m.agentKey, true
	}
	return nil, false
}

// localConsensusSigningKey keeps the JOIN-frozen federation transport
// credential out of locally-originated consensus mutations after app-v23.
//
// Before app-v23 the historical node operator key remains the signer so old
// chains and embedded callers retain byte-compatible authority. Once app-v23
// is active, the exact currently committed CEREBRUM Root credential must be
// present in the local key vault. A retired Root/transport key is never a
// fallback: handover changes consensus control authority without changing the
// peer-pinned transport identity.
func (m *Manager) localConsensusSigningKey() (ed25519.PrivateKey, ed25519.PublicKey, error) {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		if len(m.agentKey) != ed25519.PrivateKeySize ||
			len(m.agentPub) != ed25519.PublicKeySize {
			return nil, nil, errors.New("legacy federation operator signing credential is unavailable")
		}
		return m.agentKey, m.agentPub, nil
	}
	if m.badger == nil {
		return nil, nil, errors.New("app-v23 federation control requires consensus Root state")
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil {
		return nil, nil, fmt.Errorf("read current CEREBRUM Root for federation control: %w", err)
	}
	if root == nil || strings.TrimSpace(root.CredentialID) == "" {
		return nil, nil, errors.New("app-v23 federation control requires a committed CEREBRUM Root")
	}
	if !m.localAdminSignerActive(root.CredentialID) {
		return nil, nil, errors.New("current CEREBRUM Root policy projection is stale or invalid")
	}
	// Consensus-control signing is stricter than the compatibility fallback
	// retained by currentRootSigningKey for older embedded elevation callers:
	// post-v23 must obtain Root from the explicit local credential vault, never
	// repurpose cfg.AgentKey merely because the two IDs happened to match at
	// genesis.
	m.rootKeyResolverMu.RLock()
	resolver := m.rootKeyResolver
	m.rootKeyResolverMu.RUnlock()
	if resolver == nil {
		return nil, nil, errors.New("current CEREBRUM Root key resolver is unavailable")
	}
	key, ok := resolver(root.CredentialID)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, nil, fmt.Errorf(
			"current CEREBRUM Root signing credential %s is unavailable",
			root.CredentialID,
		)
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize ||
		hex.EncodeToString(pub) != root.CredentialID {
		return nil, nil, errors.New("resolved federation control key does not match the committed CEREBRUM Root")
	}
	return key, pub, nil
}

func (m *Manager) exactResolvedLocalKey(
	credentialID string,
) (ed25519.PrivateKey, ed25519.PublicKey, error) {
	m.rootKeyResolverMu.RLock()
	resolver := m.rootKeyResolver
	m.rootKeyResolverMu.RUnlock()
	if resolver == nil {
		return nil, nil, errors.New("local credential resolver is unavailable")
	}
	key, ok := resolver(credentialID)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, nil, fmt.Errorf("local signing credential %s is unavailable", credentialID)
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize ||
		hex.EncodeToString(pub) != credentialID {
		return nil, nil, errors.New("resolved local credential does not match consensus identity")
	}
	return key, pub, nil
}

// localConsensusControlSigner preserves the exact local CEREBRUM actor for
// post-v23 federation consensus mutations. Empty actor means current Root for
// internal/root-only callers. A promoted Admin must remain active in the
// current Root generation and have its exact local key available; the current
// Root key is returned separately only to countersign the action.
func (m *Manager) localConsensusControlSigner(
	requestedActorID string,
) (
	actorKey ed25519.PrivateKey,
	actorPub ed25519.PublicKey,
	effectiveActorID string,
	root *store.AppV23RootState,
	rootKey ed25519.PrivateKey,
	err error,
) {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		key, pub, signErr := m.localConsensusSigningKey()
		return key, pub, hex.EncodeToString(pub), nil, nil, signErr
	}
	if m.badger == nil {
		return nil, nil, "", nil, nil,
			errors.New("app-v23 federation control requires consensus Root state")
	}
	root, err = m.badger.GetAppV23Root()
	if err != nil || root == nil || root.CredentialID == "" {
		return nil, nil, "", nil, nil,
			errors.New("app-v23 federation control requires a committed CEREBRUM Root")
	}
	effectiveActorID = strings.TrimSpace(requestedActorID)
	if effectiveActorID == "" {
		effectiveActorID = root.CredentialID
	}
	if effectiveActorID == root.CredentialID {
		actorKey, actorPub, err = m.localConsensusSigningKey()
		return actorKey, actorPub, effectiveActorID, root, nil, err
	}
	if wasRoot, historyErr := m.badger.IsAppV23RootCredential(effectiveActorID); historyErr != nil {
		return nil, nil, "", nil, nil, historyErr
	} else if wasRoot {
		return nil, nil, "", nil, nil, errors.New("retired CEREBRUM Root cannot authorize federation control")
	}
	enrollment, err := m.badger.GetAppV23Enrollment(effectiveActorID)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}
	role, err := m.badger.GetAppV23Role(effectiveActorID)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}
	if enrollment == nil || role == nil || !enrollment.Active ||
		role.Role != store.AppV23RoleAdmin ||
		enrollment.RootGeneration != root.Generation ||
		!m.localAdminSignerActive(effectiveActorID) ||
		m.badger.ValidateCurrentAgentPolicy(
			role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
		) != nil {
		return nil, nil, "", nil, nil,
			errors.New("current-generation local Admin is required for federation control")
	}
	actorKey, actorPub, err = m.exactResolvedLocalKey(effectiveActorID)
	if err != nil {
		return nil, nil, "", nil, nil, err
	}
	rootKey, _, err = m.exactResolvedLocalKey(root.CredentialID)
	if err != nil {
		return nil, nil, "", nil, nil,
			fmt.Errorf("current CEREBRUM Root countersignature is unavailable: %w", err)
	}
	return actorKey, actorPub, effectiveActorID, root, rootKey, nil
}

func (m *Manager) attachConsensusControlElevation(
	parsed *tx.ParsedTx,
	actorID string,
	root *store.AppV23RootState,
	rootKey ed25519.PrivateKey,
) error {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() ||
		root == nil || actorID == root.CredentialID {
		return nil
	}
	if parsed == nil || parsed.LocalElevation != nil {
		return errors.New("invalid app-v23 federation control action")
	}
	if len(rootKey) != ed25519.PrivateKeySize {
		return errors.New("current CEREBRUM Root countersignature key is unavailable")
	}
	heightBytes, err := m.badger.GetState("height")
	if err != nil || len(heightBytes) != 8 {
		return errors.New("committed consensus height is unavailable")
	}
	committedHeight := int64(binary.BigEndian.Uint64(heightBytes)) // #nosec G115 -- consensus height is a bounded positive int64.
	if committedHeight < 0 {
		return errors.New("committed consensus height is invalid")
	}
	nonceBytes := make([]byte, 16)
	if _, randomErr := rand.Read(nonceBytes); randomErr != nil {
		return fmt.Errorf("generate federation elevation nonce: %w", randomErr)
	}
	proof := &tx.LocalElevationProof{
		RootGeneration:   root.Generation,
		ValidFromHeight:  committedHeight + 1,
		ValidUntilHeight: committedHeight + 1 + store.AppV23MaxElevationWindow,
		Nonce:            hex.EncodeToString(nonceBytes),
	}
	actionBytes, err := tx.PayloadBytes(parsed)
	if err != nil {
		return err
	}
	proof.Signature = ed25519.Sign(
		rootKey,
		tx.AppV23ElevationSignBytes(
			root.Scope, actorID, parsed.Type, actionBytes, proof,
		),
	)
	parsed.LocalElevation = proof
	return nil
}

// PrepareFederatedGuestMutation returns the exact row bytes the authenticated
// local Admin must sign. It is intentionally read-only; mutation repeats every
// check under the write lease and fails on any generation/revision change.
func (m *Manager) PrepareFederatedGuestMutation(
	ctx context.Context,
	callerID string,
	input FederatedGuestMutationInput,
) (*PreparedFederatedGuestMutation, error) {
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	eligibility, err := m.preflightFederatedGuestEligibility(ctx, input)
	if err != nil {
		return nil, err
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockPolicy := ss.LockSyncPolicyRead()
	defer unlockPolicy()
	if m.badger == nil {
		return nil, errors.New("app-v23 authorization state is unavailable")
	}
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	return m.prepareFederatedGuestMutationLocked(
		ctx, callerID, input, eligibility,
	)
}

type federatedGuestEligibilityPreflight struct {
	agreement     *store.CrossFedRecord
	remoteAgentID string
}

func federatedGuestOperationNeedsEligibility(operation string) bool {
	return operation == FederatedGuestOperationAttach ||
		operation == FederatedGuestOperationRebind
}

// preflightFederatedGuestEligibility performs the bounded network exchange
// before any local agreement/policy/Badger lease is held. Otherwise two peers
// preparing reciprocal links can each hold its local agreement writer while
// peerAuth on the opposite node waits for that same writer. The locked phase
// below compares this exact agreement snapshot before using the result.
func (m *Manager) preflightFederatedGuestEligibility(
	ctx context.Context,
	input FederatedGuestMutationInput,
) (*federatedGuestEligibilityPreflight, error) {
	if !federatedGuestOperationNeedsEligibility(input.Operation) {
		return nil, nil
	}
	if _, err := auth.AgentIDToPublicKey(input.RemoteAgentID); err != nil ||
		input.RemoteAgentID != strings.ToLower(input.RemoteAgentID) {
		return nil, errors.New("remote agent id is invalid")
	}
	agreement, err := m.ActiveAgreement(input.RemoteChainID)
	if err != nil {
		return nil, err
	}
	if err := m.checkRemoteFederatedGuestAgentEligibility(
		ctx, agreement, input.RemoteAgentID,
	); err != nil {
		return nil, err
	}
	return &federatedGuestEligibilityPreflight{
		agreement:     agreement,
		remoteAgentID: input.RemoteAgentID,
	}, nil
}

func (m *Manager) prepareFederatedGuestMutationLocked(
	ctx context.Context,
	callerID string,
	input FederatedGuestMutationInput,
	eligibility *federatedGuestEligibilityPreflight,
) (*PreparedFederatedGuestMutation, error) {
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("local Admin authorization changed during preparation")
	}
	if !validFederatedGuestOperation(input.Operation) || input.ExpectedRevision < 0 {
		return nil, errors.New("valid federated guest operation and expected revision are required")
	}
	if _, err := auth.AgentIDToPublicKey(input.RemoteAgentID); err != nil ||
		input.RemoteAgentID != strings.ToLower(input.RemoteAgentID) {
		return nil, errors.New("remote agent id is invalid")
	}
	group, err := m.badger.GetAppV23AccessGroup(input.GroupID)
	if err != nil || group == nil {
		return nil, errors.New("local app-v23 access group does not exist")
	}
	agreement, err := m.ActiveAgreement(input.RemoteChainID)
	if err != nil {
		return nil, err
	}
	if federatedGuestOperationNeedsEligibility(input.Operation) &&
		(eligibility == nil ||
			eligibility.remoteAgentID != input.RemoteAgentID ||
			!sameAgreementGeneration(eligibility.agreement, agreement)) {
		return nil, errors.New(
			"federation agreement changed during remote agent eligibility check",
		)
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, input.RemoteChainID)
	if err != nil {
		return nil, err
	}
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil || policy == nil {
		return nil, fmt.Errorf("federation v23 binding is not ready: %w", err)
	}
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	existing, err := m.syncStore().GetFederatedGroupGuest(
		ctx, input.GroupID, input.RemoteChainID, input.RemoteAgentID,
	)
	if err != nil {
		return nil, err
	}
	currentRevision := int64(0)
	if existing != nil {
		currentRevision = existing.Revision
	}
	if currentRevision != input.ExpectedRevision {
		return nil, store.ErrAppV23RevisionConflict
	}
	var next store.FederatedGroupGuest
	switch input.Operation {
	case FederatedGuestOperationAttach:
		if existing != nil {
			return nil, errors.New("federated guest link already exists; use rebind")
		}
		if input.MaxClassification > agreement.MaxClearance {
			return nil, errors.New("guest classification exceeds the active agreement ceiling")
		}
		next = store.FederatedGroupGuest{
			GroupID: input.GroupID, RemoteChainID: input.RemoteChainID,
			RemoteAgentID: input.RemoteAgentID, MaxClassification: input.MaxClassification,
			State: store.FederatedGuestStateActive,
		}
	case FederatedGuestOperationPause:
		if existing == nil || existing.State != store.FederatedGuestStateActive {
			return nil, errors.New("only an active federated guest link can be paused")
		}
		next = *existing
		next.State = store.FederatedGuestStatePaused
	case FederatedGuestOperationRevoke:
		if existing == nil || existing.State == store.FederatedGuestStateRevoked {
			return nil, errors.New("federated guest link is absent or already revoked")
		}
		next = *existing
		next.State = store.FederatedGuestStateRevoked
	case FederatedGuestOperationRebind:
		if existing == nil || existing.State == store.FederatedGuestStateRevoked {
			return nil, errors.New("a non-revoked federated guest link is required for rebind")
		}
		if existing.AgreementBindingDigest == digest &&
			existing.State == store.FederatedGuestStateActive &&
			m.federatedGuestAuthorityActive(*existing) {
			return nil, errors.New("federated guest link already matches the current binding")
		}
		next = *existing
		next.State = store.FederatedGuestStateActive
	}
	authorityKind, rootGeneration, roleRevision, enrollmentRevision, err :=
		m.federatedGuestAuthoritySnapshot(callerID)
	if err != nil {
		return nil, err
	}
	next.AgreementBindingDigest = digest
	next.Revision = currentRevision + 1
	next.AuthorizedBy = callerID
	next.AuthorityKind = authorityKind
	next.AuthorityRootGeneration = rootGeneration
	next.AuthorityRoleRevision = roleRevision
	next.AuthorityEnrollmentRevision = enrollmentRevision
	next.Signature = nil
	signingBytes, err := next.SigningBytes()
	if err != nil {
		return nil, err
	}
	actionDigest, err := federatedGuestActionDigest(input.Operation, input.ExpectedRevision, next)
	if err != nil {
		return nil, err
	}
	return &PreparedFederatedGuestMutation{
		Operation: input.Operation, ExpectedRevision: input.ExpectedRevision,
		Guest: next, SigningBytes: signingBytes, ActionDigest: actionDigest,
	}, nil
}

func (m *Manager) federatedGuestAuthoritySnapshot(
	callerID string,
) (
	kind string,
	rootGeneration uint64,
	roleRevision uint64,
	enrollmentRevision uint64,
	err error,
) {
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil {
		return "", 0, 0, 0, errors.New("current Root state is unavailable")
	}
	if callerID == root.CredentialID {
		return store.FederatedGuestAuthorityRoot, root.Generation, 0, 0, nil
	}
	enrollment, err := m.badger.GetAppV23Enrollment(callerID)
	if err != nil {
		return "", 0, 0, 0, err
	}
	role, err := m.badger.GetAppV23Role(callerID)
	if err != nil {
		return "", 0, 0, 0, err
	}
	if enrollment == nil || role == nil ||
		role.Role != store.AppV23RoleAdmin ||
		enrollment.RootGeneration != root.Generation ||
		enrollment.Revision == 0 || role.Revision == 0 ||
		!m.localAdminSignerActive(callerID) {
		return "", 0, 0, 0,
			errors.New("current-generation local Admin is required")
	}
	return store.FederatedGuestAuthorityAdmin, root.Generation,
		role.Revision, enrollment.Revision, nil
}

// MintFederatedGuestElevation creates a stateless, one-action sudo token. Only
// a node whose operator key is the CURRENT root credential can mint it.
func (m *Manager) MintFederatedGuestElevation(
	ctx context.Context,
	adminCallerID string,
	mutation FederatedGuestMutation,
) (*FederatedGuestElevation, error) {
	if m.badger == nil {
		return nil, errors.New("app-v23 authorization state is unavailable")
	}
	if mutation.Guest.AuthorizedBy != adminCallerID ||
		!m.localAdminSignerActive(adminCallerID) ||
		m.isCurrentRootCredential(adminCallerID) {
		return nil, errors.New("authenticated promoted Admin must match the signed guest mutation")
	}
	eligibility, err := m.preflightFederatedGuestEligibility(
		ctx,
		FederatedGuestMutationInput{
			Operation: mutation.Operation, GroupID: mutation.Guest.GroupID,
			RemoteChainID:     mutation.Guest.RemoteChainID,
			RemoteAgentID:     mutation.Guest.RemoteAgentID,
			MaxClassification: mutation.Guest.MaxClassification,
			ExpectedRevision:  mutation.ExpectedRevision,
		},
	)
	if err != nil {
		return nil, err
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockPolicy := ss.LockSyncPolicyRead()
	defer unlockPolicy()
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	if !m.localAdminSignerActive(adminCallerID) {
		return nil, errors.New("local Admin authorization changed during elevation")
	}
	prepared, err := m.prepareFederatedGuestMutationLocked(
		ctx,
		adminCallerID,
		FederatedGuestMutationInput{
			Operation: mutation.Operation, GroupID: mutation.Guest.GroupID,
			RemoteChainID:     mutation.Guest.RemoteChainID,
			RemoteAgentID:     mutation.Guest.RemoteAgentID,
			MaxClassification: mutation.Guest.MaxClassification,
			ExpectedRevision:  mutation.ExpectedRevision,
		},
		eligibility,
	)
	if err != nil {
		return nil, err
	}
	expectedBytes, _ := prepared.Guest.SigningBytes()
	suppliedBytes, _ := mutation.Guest.SigningBytes()
	if !bytes.Equal(expectedBytes, suppliedBytes) ||
		store.VerifyFederatedGroupGuest(mutation.Guest) != nil {
		return nil, errors.New("admin signature does not match the current prepared guest mutation")
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil {
		return nil, errors.New("current root state is unavailable")
	}
	rootKey, ok := m.currentRootSigningKey(root.CredentialID)
	if !ok {
		return nil, errors.New("current root signing credential is unavailable")
	}
	digest, err := federatedGuestActionDigest(
		mutation.Operation, mutation.ExpectedRevision, mutation.Guest,
	)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, 16)
	if _, randomErr := rand.Read(nonce); randomErr != nil {
		return nil, randomErr
	}
	now := time.Now()
	token := &FederatedGuestElevation{
		Scope: federatedGuestElevationScope, RootCredentialID: root.CredentialID,
		RootGeneration: root.Generation, AdminID: mutation.Guest.AuthorizedBy,
		Operation: mutation.Operation, ActionDigest: digest,
		Nonce: hex.EncodeToString(nonce), IssuedAt: now.Unix(),
		ExpiresAt: now.Add(federatedGuestElevationTTL).Unix(),
	}
	body, err := token.signingBytes()
	if err != nil {
		return nil, err
	}
	token.Signature = ed25519.Sign(rootKey, body)
	return token, nil
}

func (m *Manager) verifyFederatedGuestElevation(
	now time.Time,
	callerID string,
	mutation FederatedGuestMutation,
) (*store.FederatedAdminElevationUse, error) {
	token := mutation.Elevation
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil || token == nil ||
		token.Scope != federatedGuestElevationScope ||
		token.RootCredentialID != root.CredentialID ||
		token.RootGeneration != root.Generation ||
		token.AdminID != callerID || token.Operation != mutation.Operation ||
		token.IssuedAt > now.Unix() || token.ExpiresAt < now.Unix() ||
		token.ExpiresAt-token.IssuedAt > int64(federatedGuestElevationTTL/time.Second) {
		return nil, errors.New("federated Admin elevation is missing, stale, or mismatched")
	}
	digest, err := federatedGuestActionDigest(
		mutation.Operation, mutation.ExpectedRevision, mutation.Guest,
	)
	if err != nil || token.ActionDigest != digest {
		return nil, errors.New("federated Admin elevation action digest mismatch")
	}
	rootPub, err := auth.AgentIDToPublicKey(root.CredentialID)
	if err != nil {
		return nil, err
	}
	body, err := token.signingBytes()
	if err != nil || len(token.Signature) != ed25519.SignatureSize ||
		!ed25519.Verify(rootPub, body, token.Signature) {
		return nil, errors.New("federated Admin elevation root signature is invalid")
	}
	return &store.FederatedAdminElevationUse{
		Scope: token.Scope, RootGeneration: token.RootGeneration,
		AdminID: token.AdminID, Nonce: token.Nonce, ExpiresAt: token.ExpiresAt,
	}, nil
}

// CommitFederatedGuestMutation repeats preparation under the write lease,
// verifies both signatures, then CASes the guest and consumes any sudo token
// atomically in SQLite.
func (m *Manager) CommitFederatedGuestMutation(
	ctx context.Context,
	callerID string,
	mutation FederatedGuestMutation,
) (*store.FederatedGroupGuest, error) {
	if !m.localAdminSignerActive(callerID) || mutation.Guest.AuthorizedBy != callerID {
		return nil, errors.New("current local Admin or root credential is required")
	}
	eligibility, err := m.preflightFederatedGuestEligibility(
		ctx,
		FederatedGuestMutationInput{
			Operation: mutation.Operation, GroupID: mutation.Guest.GroupID,
			RemoteChainID:     mutation.Guest.RemoteChainID,
			RemoteAgentID:     mutation.Guest.RemoteAgentID,
			MaxClassification: mutation.Guest.MaxClassification,
			ExpectedRevision:  mutation.ExpectedRevision,
		},
	)
	if err != nil {
		return nil, err
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	releaseDelivery := m.beginPeerLinkedAuthorizationMutation(
		mutation.Guest.RemoteChainID,
	)
	defer releaseDelivery()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockPolicy := ss.LockSyncPolicyWrite()
	defer unlockPolicy()
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("local Admin authorization changed during mutation")
	}

	prepared, err := m.prepareFederatedGuestMutationLocked(
		ctx,
		callerID,
		FederatedGuestMutationInput{
			Operation: mutation.Operation, GroupID: mutation.Guest.GroupID,
			RemoteChainID:     mutation.Guest.RemoteChainID,
			RemoteAgentID:     mutation.Guest.RemoteAgentID,
			MaxClassification: mutation.Guest.MaxClassification,
			ExpectedRevision:  mutation.ExpectedRevision,
		},
		eligibility,
	)
	if err != nil {
		return nil, err
	}
	expectedBytes, _ := prepared.Guest.SigningBytes()
	suppliedBytes, _ := mutation.Guest.SigningBytes()
	if !bytes.Equal(expectedBytes, suppliedBytes) ||
		store.VerifyFederatedGroupGuest(mutation.Guest) != nil {
		return nil, errors.New("signed federated guest mutation does not match current prepared state")
	}
	var elevationUse *store.FederatedAdminElevationUse
	if !m.isCurrentRootCredential(callerID) {
		elevationUse, err = m.verifyFederatedGuestElevation(time.Now(), callerID, mutation)
		if err != nil {
			return nil, err
		}
	}
	if err := ss.CommitFederatedGroupGuestLocked(
		ctx, mutation.Guest, mutation.ExpectedRevision, elevationUse, time.Now().Unix(),
	); err != nil {
		return nil, err
	}
	out := mutation.Guest
	return &out, nil
}

func (m *Manager) ListFederatedGuestLinks(
	ctx context.Context,
	callerID, remoteChainID, remoteAgentID string,
) ([]FederatedGuestLinkView, error) {
	if m.federatedGuestStore == nil || m.badger == nil {
		return nil, errors.New("federated guest store is unavailable")
	}
	if _, err := auth.AgentIDToPublicKey(remoteAgentID); err != nil {
		return nil, errors.New("remote agent id is invalid")
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockPolicy := ss.LockSyncPolicyRead()
	defer unlockPolicy()
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peerAgentID)
	if err != nil {
		return nil, err
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(ctx, remoteChainID, remoteAgentID)
	if err != nil {
		return nil, err
	}
	out := make([]FederatedGuestLinkView, 0, len(guests))
	for _, guest := range guests {
		current := guest.AgreementBindingDigest == digest
		state := guest.State
		if !current && state != store.FederatedGuestStateRevoked {
			state = store.FederatedGuestStateRebindRequired
		}
		out = append(out, FederatedGuestLinkView{
			Guest: guest, EffectiveState: state, BindingCurrent: current,
		})
	}
	return out, nil
}

// ListFederatedGroupMembers returns the federated guest links attached to one
// local Access Group, so an operator sees a federated agent where its group is
// shown instead of only inside a separate per-identity inventory.
//
// The authority these links carry is READ ONLY, and that is a statement about
// the tree, not a limitation of this projection: federated Write is deliberately
// disabled (store.ErrPeerRBACWriteCapabilityUnavailable — a remote write needs a
// consensus-bound ingress capability). Reporting any higher authority here would
// advertise something no code path honours.
func (m *Manager) ListFederatedGroupMembers(
	ctx context.Context,
	callerID, groupID string,
) ([]FederatedGuestLinkView, error) {
	if m.federatedGuestStore == nil || m.badger == nil {
		return nil, errors.New("federated guest store is unavailable")
	}
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("an exact group id is required")
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockPolicy := ss.LockSyncPolicyRead()
	defer unlockPolicy()
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuestsForGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	// One binding digest per remote chain, not per link: a group can hold links
	// from several peers and re-deriving the digest for each would repeat the
	// same work under the agreement lock.
	digests := make(map[string]string, 1)
	out := make([]FederatedGuestLinkView, 0, len(guests))
	for _, guest := range guests {
		digest, known := digests[guest.RemoteChainID]
		if !known {
			digest = m.groupMemberBindingDigest(ctx, guest.RemoteChainID)
			digests[guest.RemoteChainID] = digest
		}
		// An unprovable digest (peer removed, agreement rotated) must read as
		// rebind-required rather than current. Failing open here would present a
		// stale link as live authority.
		current := digest != "" && guest.AgreementBindingDigest == digest
		state := guest.State
		if !current && state != store.FederatedGuestStateRevoked {
			state = store.FederatedGuestStateRebindRequired
		}
		out = append(out, FederatedGuestLinkView{
			Guest: guest, EffectiveState: state, BindingCurrent: current,
		})
	}
	return out, nil
}

// groupMemberBindingDigest returns the current agreement binding digest for one
// remote chain, or "" when it cannot be proven.
func (m *Manager) groupMemberBindingDigest(ctx context.Context, remoteChainID string) string {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return ""
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return ""
	}
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peerAgentID)
	if err != nil {
		return ""
	}
	return digest
}

// ListFederatedGuestIdentities returns existing exact Linked-reader principals
// from local durable rows. New rows can enter this inventory only through the
// live target verification above; the JOIN peer operator key is never used.
func (m *Manager) ListFederatedGuestIdentities(
	ctx context.Context,
	callerID string,
) ([]store.FederatedGuestIdentity, error) {
	if m.badger == nil {
		return nil, errors.New("federated guest store is unavailable")
	}
	unlockAgreement := m.LockAgreementMutation()
	defer unlockAgreement()
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated guest control requires SQLite")
	}
	unlockBadger := m.badger.LockDomainOwnershipRead()
	defer unlockBadger()
	if !m.localAdminSignerActive(callerID) {
		return nil, errors.New("current local Admin or root credential is required")
	}
	identities, err := ss.ListFederatedGuestIdentities(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]store.FederatedGuestIdentity, 0, len(identities))
	for _, identity := range identities {
		if _, err := m.ActiveAgreement(identity.RemoteChainID); err != nil {
			continue
		}
		out = append(out, identity)
	}
	return out, nil
}
