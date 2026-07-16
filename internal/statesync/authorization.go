package statesync

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	// RequiredAppVersion is the only application protocol version accepted by
	// the first internet state-sync format. It is deliberately not negotiated.
	RequiredAppVersion        uint64 = 20
	cometNodeIDBytes                 = 20
	maxJoinAuthorizationBytes        = 64 << 10
)

var stateSyncChainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// JoinAuthorizationConfig is a trusted, locally installed approval for one
// validator bootstrapping onto one existing chain. Installation/configuration
// is the trust root: this type intentionally does not invent a ticket signature
// or issuer quorum that the accepted v11.9 design has not specified.
type JoinAuthorizationConfig struct {
	ChainID             string    `json:"chain_id"`
	JoiningNodeID       string    `json:"joining_node_id"`
	ValidatorPublicKey  []byte    `json:"validator_public_key"`
	AppVersion          uint64    `json:"app_version"`
	ExpiresAt           time.Time `json:"expires_at"`
	SnapshotHeightFloor uint64    `json:"snapshot_height_floor"`
	ValidatorNodeIDs    []string  `json:"validator_node_ids"`
	ProviderNodeIDs     []string  `json:"provider_node_ids"`
}

// ValidatorP2PProfile is the already-applied CometBFT connection policy for an
// armed state-sync process. The pre-handshake address callback is only a
// syntax/liveness gate; Comet's post-handshake ABCI Query callback enforces the
// exact authenticated node-ID set before adding a peer to the switch. Peer
// fields contain canonical node IDs, not nodeID@address strings.
type ValidatorP2PProfile struct {
	ChainID                 string
	LocalNodeID             string
	LocalValidatorPublicKey []byte
	FilterPeers             bool
	PEX                     bool
	SeedMode                bool
	Seeds                   []string
	MaxInboundPeers         int
	MaxOutboundPeers        int
	UnconditionalPeerIDs    []string
	PrivatePeerIDs          []string
	PersistentPeerIDs       []string
}

type joinAuthorization struct {
	chainID             string
	joiningNodeID       string
	validatorPublicKey  []byte
	appVersion          uint64
	expiresAt           time.Time
	deadline            time.Time
	snapshotHeightFloor uint64
	validatorNodeIDs    []string
	providerNodeIDs     []string
	providers           map[string]struct{}
}

// ServingAuthorization proves that a serving validator's installed join
// approval and live P2P policy agree. ListSnapshots and LoadSnapshotChunk must
// not be armed without one.
type ServingAuthorization struct {
	join *joinAuthorization
}

// ReceivingAuthorization proves that the joining process is the approved node
// and can fetch only from the approved validator providers.
type ReceivingAuthorization struct {
	join *joinAuthorization
}

// NewServingAuthorization validates all join bindings and the exact
// validator-only P2P profile for a snapshot provider.
func NewServingAuthorization(join JoinAuthorizationConfig, profile ValidatorP2PProfile, now time.Time) (*ServingAuthorization, error) {
	validated, err := validateJoinAuthorization(join, now)
	if err != nil {
		return nil, err
	}
	if _, ok := validated.providers[profile.LocalNodeID]; !ok {
		return nil, errors.New("state sync serving node is not an approved provider")
	}
	if err := validateValidatorP2PProfile(profile, validated, false); err != nil {
		return nil, err
	}
	return &ServingAuthorization{join: validated}, nil
}

// NewReceivingAuthorization validates all join bindings and the exact
// validator-only P2P profile for the approved joining node.
func NewReceivingAuthorization(join JoinAuthorizationConfig, profile ValidatorP2PProfile, now time.Time) (*ReceivingAuthorization, error) {
	validated, err := validateJoinAuthorization(join, now)
	if err != nil {
		return nil, err
	}
	if profile.LocalNodeID != validated.joiningNodeID {
		return nil, errors.New("state sync receiving node does not match join authorization")
	}
	if !bytes.Equal(profile.LocalValidatorPublicKey, validated.validatorPublicKey) {
		return nil, errors.New("state sync receiving validator public key does not match join authorization")
	}
	if err := validateValidatorP2PProfile(profile, validated, true); err != nil {
		return nil, err
	}
	return &ReceivingAuthorization{join: validated}, nil
}

// LoadJoinAuthorization reads one strict locally-installed JSON authorization.
// The file is a trust root, so symlinks, writable-by-others permissions,
// unknown fields, oversized input, and trailing JSON values are rejected.
func LoadJoinAuthorization(path string) (JoinAuthorizationConfig, error) {
	if path == "" {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization path is empty")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return JoinAuthorizationConfig{}, fmt.Errorf("inspect state sync join authorization: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization must be a regular file")
	}
	if before.Mode().Perm()&0o022 != 0 {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization must not be group/world writable")
	}
	if before.Size() <= 0 || before.Size() > maxJoinAuthorizationBytes {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization has an invalid size")
	}
	file, err := os.Open(path) //nolint:gosec // explicit locally-installed trust root
	if err != nil {
		return JoinAuthorizationConfig{}, fmt.Errorf("open state sync join authorization: %w", err)
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxJoinAuthorizationBytes+1))
	if err != nil {
		return JoinAuthorizationConfig{}, fmt.Errorf("read state sync join authorization: %w", err)
	}
	if len(encoded) > maxJoinAuthorizationBytes {
		return JoinAuthorizationConfig{}, errors.New("state sync join authorization exceeds the maximum size")
	}
	if err := validateJoinAuthorizationJSONFields(encoded); err != nil {
		return JoinAuthorizationConfig{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var config JoinAuthorizationConfig
	if err := decoder.Decode(&config); err != nil {
		return JoinAuthorizationConfig{}, fmt.Errorf("decode state sync join authorization: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return JoinAuthorizationConfig{}, errors.New("state sync join authorization contains trailing JSON")
		}
		return JoinAuthorizationConfig{}, fmt.Errorf("decode state sync join authorization trailing data: %w", err)
	}
	return config, nil
}

var joinAuthorizationJSONFields = []string{
	"chain_id",
	"joining_node_id",
	"validator_public_key",
	"app_version",
	"expires_at",
	"snapshot_height_floor",
	"validator_node_ids",
	"provider_node_ids",
}

// validateJoinAuthorizationJSONFields closes two encoding/json ambiguities that
// are unsafe for a locally-installed trust root: duplicate object names use the
// last value, and tagged struct fields otherwise accept case-folded spellings.
func validateJoinAuthorizationJSONFields(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	opening, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode state sync join authorization: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return errors.New("state sync join authorization must be one JSON object")
	}
	seen := make([]string, 0, len(joinAuthorizationJSONFields))
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return fmt.Errorf("decode state sync join authorization field: %w", tokenErr)
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("state sync join authorization field name is invalid")
		}
		for _, previous := range seen {
			if strings.EqualFold(previous, name) {
				if previous == name {
					return fmt.Errorf("state sync join authorization contains duplicate top-level JSON field %q", name)
				}
				return fmt.Errorf("state sync join authorization contains case-folded duplicate top-level JSON fields %q and %q", previous, name)
			}
		}
		for _, canonical := range joinAuthorizationJSONFields {
			if strings.EqualFold(canonical, name) && canonical != name {
				return fmt.Errorf("state sync join authorization field %q must use canonical spelling %q", name, canonical)
			}
		}
		seen = append(seen, name)
		var value json.RawMessage
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return fmt.Errorf("decode state sync join authorization field %q: %w", name, decodeErr)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode state sync join authorization: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return errors.New("state sync join authorization object is not closed")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("state sync join authorization contains trailing JSON")
	}
	return nil
}

func (authorization *ServingAuthorization) ValidateAt(now time.Time) error {
	if authorization == nil || authorization.join == nil {
		return errors.New("state sync serving authorization is missing")
	}
	return authorization.join.validateAt(now)
}

func (authorization *ReceivingAuthorization) ValidateAt(now time.Time) error {
	if authorization == nil || authorization.join == nil {
		return errors.New("state sync receiving authorization is missing")
	}
	return authorization.join.validateAt(now)
}

func (authorization *ServingAuthorization) SnapshotHeightFloor() uint64 {
	if authorization == nil || authorization.join == nil {
		return 0
	}
	return authorization.join.snapshotHeightFloor
}

func (authorization *ReceivingAuthorization) SnapshotHeightFloor() uint64 {
	if authorization == nil || authorization.join == nil {
		return 0
	}
	return authorization.join.snapshotHeightFloor
}

func (authorization *ReceivingAuthorization) AllowsProvider(nodeID string) bool {
	if authorization == nil || authorization.join == nil {
		return false
	}
	_, ok := authorization.join.providers[nodeID]
	return ok
}

func (authorization *ReceivingAuthorization) ChainID() string {
	if authorization == nil || authorization.join == nil {
		return ""
	}
	return authorization.join.chainID
}

func (authorization *ReceivingAuthorization) JoiningNodeID() string {
	if authorization == nil || authorization.join == nil {
		return ""
	}
	return authorization.join.joiningNodeID
}

func (authorization *ReceivingAuthorization) ValidatorPublicKey() []byte {
	if authorization == nil || authorization.join == nil {
		return nil
	}
	return append([]byte(nil), authorization.join.validatorPublicKey...)
}

func (authorization *ReceivingAuthorization) AppVersion() uint64 {
	if authorization == nil || authorization.join == nil {
		return 0
	}
	return authorization.join.appVersion
}

// ExpiresAt returns the immutable one-shot ceremony deadline.
func (authorization *ReceivingAuthorization) ExpiresAt() time.Time {
	if authorization == nil || authorization.join == nil {
		return time.Time{}
	}
	return authorization.join.expiresAt
}

// Deadline is the process-local expiry boundary. When construction receives a
// time.Now value it retains Go's monotonic reading, so a later wall-clock
// rollback cannot extend the authorization.
func (authorization *ReceivingAuthorization) Deadline() time.Time {
	if authorization == nil || authorization.join == nil {
		return time.Time{}
	}
	return authorization.join.deadline
}

// ExpiresAt returns the immutable one-shot ceremony deadline.
func (authorization *ServingAuthorization) ExpiresAt() time.Time {
	if authorization == nil || authorization.join == nil {
		return time.Time{}
	}
	return authorization.join.expiresAt
}

// Deadline is the process-local monotonic expiry boundary.
func (authorization *ServingAuthorization) Deadline() time.Time {
	if authorization == nil || authorization.join == nil {
		return time.Time{}
	}
	return authorization.join.deadline
}

// ApprovedPeerNodeIDs returns a private, canonical copy of the exact Comet P2P
// allowlist: all existing validator nodes plus the distinct joining node.
func (authorization *ServingAuthorization) ApprovedPeerNodeIDs() []string {
	if authorization == nil || authorization.join == nil {
		return nil
	}
	return authorization.join.approvedPeerNodeIDs()
}

// ApprovedPeerNodeIDs returns a private, canonical copy of the exact Comet P2P
// allowlist: all existing validator nodes plus the distinct joining node.
func (authorization *ReceivingAuthorization) ApprovedPeerNodeIDs() []string {
	if authorization == nil || authorization.join == nil {
		return nil
	}
	return authorization.join.approvedPeerNodeIDs()
}

// ValidatorNodeIDs returns the immutable pre-join validator peer set. Unlike
// ApprovedPeerNodeIDs it deliberately omits the one-shot joining node. The P2P
// filter uses this stable set after a serving authorization expires, and after
// a receiver has sealed, so ordinary validator reconnects cannot be coupled to
// the lifetime of the snapshot-transfer session.
func (authorization *ServingAuthorization) ValidatorNodeIDs() []string {
	if authorization == nil || authorization.join == nil {
		return nil
	}
	return append([]string(nil), authorization.join.validatorNodeIDs...)
}

// ValidatorNodeIDs returns the immutable pre-join validator peer set. A sealed
// receiver may keep using these peers after its one-shot transfer authorization
// expires; snapshot admission itself remains closed.
func (authorization *ReceivingAuthorization) ValidatorNodeIDs() []string {
	if authorization == nil || authorization.join == nil {
		return nil
	}
	return append([]string(nil), authorization.join.validatorNodeIDs...)
}

// ValidCometNodeID reports whether nodeID is CometBFT's canonical lowercase
// hex encoding of a 20-byte authenticated node address.
func ValidCometNodeID(nodeID string) bool {
	decoded, err := hex.DecodeString(nodeID)
	return err == nil && len(decoded) == cometNodeIDBytes && hex.EncodeToString(decoded) == nodeID
}

func validateJoinAuthorization(config JoinAuthorizationConfig, now time.Time) (*joinAuthorization, error) {
	if !stateSyncChainIDPattern.MatchString(config.ChainID) || config.ChainID == "." || config.ChainID == ".." {
		return nil, errors.New("state sync join authorization has an invalid chain ID")
	}
	if !ValidCometNodeID(config.JoiningNodeID) {
		return nil, errors.New("state sync join authorization has an invalid joining node ID")
	}
	if len(config.ValidatorPublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("state sync join authorization requires an Ed25519 validator public key")
	}
	if config.AppVersion != RequiredAppVersion {
		return nil, fmt.Errorf("state sync join authorization requires app version %d", RequiredAppVersion)
	}
	if config.ExpiresAt.IsZero() || !now.Before(config.ExpiresAt) {
		return nil, errors.New("state sync join authorization is expired")
	}
	if config.SnapshotHeightFloor == 0 {
		return nil, errors.New("state sync join authorization requires a positive snapshot height floor")
	}
	validators, validatorSet, err := canonicalNodeIDSet(config.ValidatorNodeIDs, "validator")
	if err != nil {
		return nil, err
	}
	providers, providerSet, err := canonicalNodeIDSet(config.ProviderNodeIDs, "provider")
	if err != nil {
		return nil, err
	}
	if len(validators) == 0 || len(providers) == 0 {
		return nil, errors.New("state sync join authorization requires validators and providers")
	}
	if _, exists := validatorSet[config.JoiningNodeID]; exists {
		return nil, errors.New("state sync joining node must be distinct from existing validators")
	}
	if len(providers) != len(validators) {
		return nil, errors.New("state sync provider node IDs must exactly equal validator node IDs")
	}
	for index := range validators {
		if providers[index] != validators[index] {
			return nil, errors.New("state sync provider node IDs must exactly equal validator node IDs")
		}
	}
	return &joinAuthorization{
		chainID: config.ChainID, joiningNodeID: config.JoiningNodeID,
		validatorPublicKey: append([]byte(nil), config.ValidatorPublicKey...), appVersion: config.AppVersion,
		expiresAt: config.ExpiresAt.UTC(), deadline: now.Add(config.ExpiresAt.Sub(now)), snapshotHeightFloor: config.SnapshotHeightFloor,
		validatorNodeIDs: validators, providerNodeIDs: providers, providers: providerSet,
	}, nil
}

func (authorization *joinAuthorization) validateAt(now time.Time) error {
	if authorization == nil || !now.Before(authorization.deadline) {
		return errors.New("state sync join authorization is expired")
	}
	return nil
}

func (authorization *joinAuthorization) approvedPeerNodeIDs() []string {
	peers := append([]string(nil), authorization.validatorNodeIDs...)
	peers = append(peers, authorization.joiningNodeID)
	sort.Strings(peers)
	return peers
}

func validateValidatorP2PProfile(profile ValidatorP2PProfile, authorization *joinAuthorization, receiving bool) error {
	if profile.ChainID != authorization.chainID {
		return errors.New("state sync P2P chain ID does not match join authorization")
	}
	if !ValidCometNodeID(profile.LocalNodeID) {
		return errors.New("state sync P2P profile has an invalid local node ID")
	}
	if !profile.FilterPeers {
		return errors.New("state sync P2P profile must enable authenticated peer filtering")
	}
	if profile.PEX || profile.SeedMode || len(profile.Seeds) != 0 ||
		profile.MaxInboundPeers != 0 || profile.MaxOutboundPeers != 0 {
		return errors.New("state sync P2P profile must disable PEX, seeds, and seed mode and set ordinary peer capacity to zero")
	}
	expected := make(map[string]struct{}, len(authorization.validatorNodeIDs)+1)
	for _, nodeID := range authorization.validatorNodeIDs {
		expected[nodeID] = struct{}{}
	}
	expected[authorization.joiningNodeID] = struct{}{}
	if err := requireExactNodeIDSet(profile.UnconditionalPeerIDs, expected, "unconditional"); err != nil {
		return err
	}
	if err := requireExactNodeIDSet(profile.PrivatePeerIDs, expected, "private"); err != nil {
		return err
	}
	persistent, _, err := canonicalNodeIDSet(profile.PersistentPeerIDs, "persistent")
	if err != nil {
		return err
	}
	for _, nodeID := range persistent {
		if nodeID == profile.LocalNodeID {
			return errors.New("state sync P2P profile lists the local node as a persistent peer")
		}
		if _, ok := expected[nodeID]; !ok {
			return errors.New("state sync P2P profile contains a non-validator persistent peer")
		}
	}
	if receiving {
		persistentSet := make(map[string]struct{}, len(persistent))
		for _, nodeID := range persistent {
			persistentSet[nodeID] = struct{}{}
		}
		for _, provider := range authorization.providerNodeIDs {
			if provider == profile.LocalNodeID {
				continue
			}
			if _, ok := persistentSet[provider]; !ok {
				return errors.New("state sync receiving profile is missing an approved persistent provider")
			}
		}
	}
	return nil
}

func requireExactNodeIDSet(values []string, expected map[string]struct{}, label string) error {
	_, actual, err := canonicalNodeIDSet(values, label)
	if err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("state sync P2P %s peer set does not match approved validators and joiner", label)
	}
	for nodeID := range expected {
		if _, ok := actual[nodeID]; !ok {
			return fmt.Errorf("state sync P2P %s peer set does not match approved validators and joiner", label)
		}
	}
	return nil
}

func canonicalNodeIDSet(values []string, label string) ([]string, map[string]struct{}, error) {
	canonical := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, nodeID := range values {
		if !ValidCometNodeID(nodeID) {
			return nil, nil, fmt.Errorf("state sync %s node ID is invalid", label)
		}
		if _, ok := seen[nodeID]; ok {
			return nil, nil, fmt.Errorf("state sync %s node ID is duplicated", label)
		}
		seen[nodeID] = struct{}{}
		canonical = append(canonical, nodeID)
	}
	sort.Strings(canonical)
	return canonical, seen, nil
}
