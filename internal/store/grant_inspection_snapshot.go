package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/consensuskeys"
)

// This reader is intentionally private: it is not an authorization surface.
// A future inspection endpoint must supply its authorization in the same held
// transaction. No capability, policy, or permission is created by this helper.
type grantInspectionSnapshot struct {
	Height           uint64
	AppHash          string
	ChainDomain      string
	EvaluatedAt      int64
	Root             *AppV23RootState
	TargetEnrollment *AppV23LocalEnrollment
	TargetRole       *AppV23RoleState
	Grants           []AgentAccessGrant
}

type grantInspectionLimits struct{ scanned, rows, bytes int }

type grantInspectionMetadataCheck func(...[]byte) error

var errGrantInspectionLimit = errors.New("grant inspection exceeds bounded read budget")

// readGrantInspectionSnapshot only opens a committed DB.View. In particular it
// never reuses a FinalizeBlock transaction or labels its speculative state as a
// committed observation. Authorization receives the exact same read-only view
// as the rows and persisted handshake tuple, not a second current-state read.
func (s *BadgerStore) readGrantInspectionSnapshot(ctx context.Context, target string, now int64,
	limits grantInspectionLimits, authorize func(*BadgerStore, grantInspectionMetadataCheck) error,
) (*grantInspectionSnapshot, error) {
	if s == nil || s.db == nil || s.txn != nil || authorize == nil || ctx == nil {
		return nil, errors.New("committed authorized grant reader required")
	}
	if !isCanonicalAgentID(target) || now <= 0 || limits.scanned <= 0 || limits.scanned > 16384 ||
		limits.rows <= 0 || limits.rows > 1024 || limits.bytes <= 0 || limits.bytes > 1048576 {
		return nil, errors.New("invalid bounded grant inspection request")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var result *grantInspectionSnapshot
	err := s.db.View(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader := &BadgerStore{db: s.db, txn: txn, taintServingReadOnly: true, taintServingCommittedRead: true}
		out := &grantInspectionSnapshot{EvaluatedAt: now, Grants: []AgentAccessGrant{}}
		used := 0
		accounted := make(map[string]bool)
		check := func(keys ...[]byte) error {
			for _, key := range keys {
				if err := ctx.Err(); err != nil {
					return err
				}
				if accounted[string(key)] {
					continue
				}
				item, err := txn.Get(key)
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				size := int64(len(key)) + item.ValueSize()
				if size > int64(limits.bytes-used) {
					return errGrantInspectionLimit
				}
				used += int(size)
				accounted[string(key)] = true
			}
			return nil
		}
		take := func(key []byte, required bool) ([]byte, error) {
			if err := check(key); err != nil {
				return nil, err
			}
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) && !required {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			return item.ValueCopy(nil)
		}
		if err := authorize(reader, check); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		height, err := take([]byte("state:height"), true)
		if err != nil {
			return err
		}
		hash, err := take([]byte("state:app_hash"), true)
		if err != nil {
			return err
		}
		domain, err := take([]byte("state:"+consensuskeys.GovernanceDelegationDomainV20), true)
		if err != nil {
			return err
		}
		if len(height) != 8 || binary.BigEndian.Uint64(height) == 0 || binary.BigEndian.Uint64(height) > math.MaxInt64 || len(hash) != 32 || len(domain) != 32 {
			return errors.New("committed grant inspection metadata unavailable")
		}
		out.Height = binary.BigEndian.Uint64(height)
		out.AppHash = hex.EncodeToString(hash)
		out.ChainDomain = hex.EncodeToString(domain)
		// Preflight canonical metadata size before the typed readers decode it.
		for _, key := range [][]byte{appV23RootKey(), appV23EnrollmentKey(target), appV23RoleKey(target)} {
			if _, err := take(key, false); err != nil {
				return err
			}
		}
		out.Root, err = reader.GetAppV23Root()
		if err != nil {
			return err
		}
		out.TargetEnrollment, err = reader.GetAppV23Enrollment(target)
		if err != nil {
			return err
		}
		out.TargetRole, err = reader.GetAppV23Role(target)
		if err != nil {
			return err
		}
		if out.Root == nil || out.TargetEnrollment == nil || out.TargetRole == nil || out.TargetEnrollment.AgentID != target || out.TargetRole.AgentID != target {
			return errors.New("grant inspection target metadata unavailable")
		}
		prefix := []byte("grant:")
		suffix := []byte(":" + target)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		scanned, matched := 0, 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			scanned++
			if scanned > limits.scanned {
				return errGrantInspectionLimit
			}
			item := it.Item()
			key := item.Key()
			if !bytes.HasSuffix(key, suffix) {
				continue
			}
			matched++
			if matched > limits.rows {
				return errGrantInspectionLimit
			}
			domain, agent, ok := parseCanonicalGrantKey(key)
			if !ok || agent != target {
				return errors.New("invalid canonical grant key")
			}
			value, err := take(key, true)
			if err != nil {
				return err
			}
			if len(value) < 13 || value[0] < 1 || value[0] > 3 {
				return errors.New("invalid canonical grant value")
			}
			expiry := binary.BigEndian.Uint64(value[1:9])
			if expiry > math.MaxInt64 {
				return errors.New("invalid canonical grant expiry")
			}
			granter, off, err := decodeString(value, 9)
			if err != nil || off != len(value) || !isCanonicalAgentID(granter) {
				return errors.New("invalid canonical grant granter")
			}
			if expiry != 0 && int64(expiry) <= now {
				continue
			}
			out.Grants = append(out.Grants, AgentAccessGrant{Domain: domain, AgentID: target, Level: value[0], ExpiresAt: int64(expiry), GranterID: granter})
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		result = out
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read grant inspection snapshot: %w", err)
	}
	return result, nil
}

// ErrGrantInspectionDenied never means a checked empty grant set.
var ErrGrantInspectionDenied = errors.New("direct-grant inspection is not authorized")

// DirectGrantsSnapshot is a complete direct-grant observation for one target at
// one committed height. It is not effective access, a transaction receipt, or
// coverage of every chain principal. Different responses are separate snapshots.
type DirectGrantsSnapshot struct {
	Schema          string                   `json:"schema"`
	CallerID        string                   `json:"caller_id"`
	TargetAgentID   string                   `json:"target_agent_id"`
	ChainDomain     string                   `json:"chain_domain"`
	Height          uint64                   `json:"height"`
	AppHash         string                   `json:"app_hash"`
	RootPrincipalID string                   `json:"root_principal_id"`
	RootGeneration  uint64                   `json:"root_generation"`
	EvaluatedAt     int64                    `json:"evaluated_at"`
	Coverage        string                   `json:"coverage"`
	Complete        bool                     `json:"complete"`
	Grants          []DirectGrantObservation `json:"grants"`
}

type DirectGrantObservation struct {
	Domain      string `json:"domain"`
	GranteeID   string `json:"grantee_id"`
	GranterID   string `json:"granter_id"`
	AccessLevel uint8  `json:"access_level"`
	ExpiresAt   int64  `json:"expires_at"`
}

// InspectDirectGrants authorizes the exact current capability holder within the
// same immutable transaction as the target and rows. Admin/read-all status is
// never an implicit inspection capability, and Root is never an agent caller.
func (s *BadgerStore) InspectDirectGrants(ctx context.Context, caller, target string, now int64) (*DirectGrantsSnapshot, error) {
	if !isCanonicalAgentID(caller) || !isCanonicalAgentID(target) {
		return nil, ErrGrantInspectionDenied
	}
	snapshot, err := s.readGrantInspectionSnapshot(ctx, target, now, grantInspectionLimits{16384, 1024, 1048576}, func(reader *BadgerStore, check grantInspectionMetadataCheck) error {
		// Bound every raw and promoted-stage authorization record before any
		// typed decoder runs. Repeated reads share the same unique-byte budget.
		keys := [][]byte{appV23RootKey(), freshGenesisActivationKey(), upgradeAppliedKey("app-v31"), upgradeAppliedKey("app-v30"), []byte("state:height")}
		for _, id := range []string{caller, target} {
			for _, key := range [][]byte{appV23EnrollmentKey(id), appV23RoleKey(id), appV23ProjectedAgentKey(id), agentOnChainKey(id), appV23RootCredentialKey(id)} {
				keys = append(keys, key, consensuskeys.AppV23MigrationStageKey(key))
			}
		}
		if err := check(keys...); err != nil {
			return err
		}
		active, err := reader.IsAppV31Active()
		if err != nil {
			return err
		}
		if !active {
			return ErrGrantInspectionDenied
		}
		activation, err := reader.GetAppliedUpgrade("app-v31")
		if err != nil {
			return err
		}
		height, err := reader.GetState("height")
		if err != nil || len(height) != 8 {
			return errors.New("committed height unavailable")
		}
		if activation != nil && (activation.AppliedHeight <= 0 || uint64(activation.AppliedHeight) > binary.BigEndian.Uint64(height)) {
			return errors.New("inspection activation exceeds committed height")
		}
		root, err := reader.GetAppV23Root()
		if err != nil {
			return err
		}
		if root == nil || root.Generation == 0 || !isCanonicalAgentID(root.PrincipalID) || !isCanonicalAgentID(root.CredentialID) {
			return errors.New("current Root metadata unavailable")
		}
		for _, id := range []string{caller, target} {
			historical, err := reader.IsAppV23RootCredential(id)
			if err != nil {
				return err
			}
			if historical || id == root.PrincipalID || id == root.CredentialID {
				return ErrGrantInspectionDenied
			}
		}
		enrollment, err := reader.GetAppV23Enrollment(caller)
		if err != nil {
			return err
		}
		role, err := reader.GetAppV23Role(caller)
		if err != nil {
			return err
		}
		if enrollment == nil || role == nil || !enrollment.Active || enrollment.AgentID != caller || role.AgentID != caller ||
			enrollment.RootGeneration != root.Generation || !enrollment.Capabilities.Has(AgentCapabilityInspectDirectGrants) ||
			reader.ValidateCurrentAgentPolicy(role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance) != nil {
			return ErrGrantInspectionDenied
		}
		registered, err := reader.GetRegisteredAgent(caller)
		if err != nil {
			return err
		}
		if registered == nil || registered.AgentID != caller || registered.Role != role.Role || registered.Clearance != enrollment.Clearance || registered.Capabilities != enrollment.Capabilities {
			return ErrGrantInspectionDenied
		}
		targetEnrollment, err := reader.GetAppV23Enrollment(target)
		if err != nil {
			return err
		}
		targetRole, err := reader.GetAppV23Role(target)
		if err != nil {
			return err
		}
		if targetEnrollment == nil || targetRole == nil || targetEnrollment.AgentID != target || targetRole.AgentID != target || targetEnrollment.Profile == AppV23ProfileRoot ||
			reader.ValidateCurrentAgentPolicy(targetRole.Role, targetEnrollment.Profile, targetEnrollment.Capabilities, targetEnrollment.Clearance) != nil {
			return ErrGrantInspectionDenied
		}
		targetRegistered, err := reader.GetRegisteredAgent(target)
		if err != nil {
			return err
		}
		if targetRegistered == nil || targetRegistered.AgentID != target || targetRegistered.Role != targetRole.Role || targetRegistered.Clearance != targetEnrollment.Clearance || targetRegistered.Capabilities != targetEnrollment.Capabilities {
			return ErrGrantInspectionDenied
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := &DirectGrantsSnapshot{Schema: "sage.direct-grants-snapshot.v1", CallerID: caller, TargetAgentID: target,
		ChainDomain: snapshot.ChainDomain, Height: snapshot.Height, AppHash: snapshot.AppHash,
		RootPrincipalID: snapshot.Root.PrincipalID, RootGeneration: snapshot.Root.Generation,
		EvaluatedAt: snapshot.EvaluatedAt, Coverage: "direct-grants-for-target", Complete: true, Grants: []DirectGrantObservation{}}
	for _, row := range snapshot.Grants {
		out.Grants = append(out.Grants, DirectGrantObservation{row.Domain, row.AgentID, row.GranterID, row.Level, row.ExpiresAt})
	}
	return out, nil
}
