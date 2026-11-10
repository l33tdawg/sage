package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/consensuskeys"
)

const (
	directGrantsScanLimit = 16_384
	directGrantsRowLimit  = 1_024
	directGrantsByteLimit = 1 << 20
	directGrantsTimeout   = 2 * time.Second
)

// ErrDirectGrantsInspectionDenied deliberately covers missing, inactive,
// Root, and unauthorized callers/targets without disclosing which condition
// failed. It is distinct from a successful observation with no grants.
var ErrDirectGrantsInspectionDenied = errors.New("direct-grant inspection is not authorized")

// ErrDirectGrantsSnapshotUnavailable never means an empty grant set. Callers
// must fail closed instead of presenting a partial or unchecked observation.
var ErrDirectGrantsSnapshotUnavailable = errors.New("direct-grant snapshot is unavailable")

// DirectGrantTargetIdentity is the exact active ordinary-agent policy observed
// in the same committed Badger snapshot as the grants.
type DirectGrantTargetIdentity struct {
	Active             bool              `json:"active"`
	Profile            string            `json:"profile"`
	Role               string            `json:"role"`
	Clearance          uint8             `json:"clearance"`
	Capabilities       AgentCapabilities `json:"capabilities"`
	RootGeneration     uint64            `json:"root_generation"`
	EnrollmentRevision uint64            `json:"enrollment_revision"`
	RoleRevision       uint64            `json:"role_revision"`
}

// DirectGrantObservation is one active exact-domain grant. It is a raw direct
// grant row, not effective access through ownership, groups, or federation.
type DirectGrantObservation struct {
	Domain      string `json:"domain"`
	GranteeID   string `json:"grantee_id"`
	GranterID   string `json:"granter_id"`
	AccessLevel uint8  `json:"access_level"`
	ExpiresAt   int64  `json:"expires_at"`
}

// DirectGrantsSnapshot is a complete active direct-grant observation for one
// target at one committed height. Different responses are separate snapshots.
type DirectGrantsSnapshot struct {
	Schema          string                    `json:"schema"`
	CallerID        string                    `json:"caller_id"`
	TargetAgentID   string                    `json:"target_agent_id"`
	Height          uint64                    `json:"height"`
	AppHash         string                    `json:"app_hash"`
	RootPrincipalID string                    `json:"root_principal_id"`
	RootGeneration  uint64                    `json:"root_generation"`
	EvaluatedAt     int64                     `json:"evaluated_at"`
	Coverage        string                    `json:"coverage"`
	Complete        bool                      `json:"complete"`
	Target          DirectGrantTargetIdentity `json:"target"`
	Grants          []DirectGrantObservation  `json:"grants"`
}

type directGrantReadBudget struct {
	ctx       context.Context
	txn       *badger.Txn
	limit     int
	usedBytes int
	seen      map[string]struct{}
}

func directGrantItemSize(key []byte, item *badger.Item, limit int) (int, error) {
	valueSize := item.ValueSize()
	if valueSize < 0 || valueSize > int64(limit) {
		return 0, fmt.Errorf("%w: item byte budget", ErrDirectGrantsSnapshotUnavailable)
	}
	size := len(key) + int(valueSize)
	if size < len(key) || size > limit {
		return 0, fmt.Errorf("%w: item byte budget", ErrDirectGrantsSnapshotUnavailable)
	}
	return size, nil
}

func (b *directGrantReadBudget) reserve(keys ...[]byte) error {
	for _, key := range keys {
		if err := b.ctx.Err(); err != nil {
			return err
		}
		if _, seen := b.seen[string(key)]; seen {
			continue
		}
		item, err := b.txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		size, err := directGrantItemSize(key, item, b.limit)
		if err != nil {
			return err
		}
		if size > b.limit-b.usedBytes {
			return fmt.Errorf("%w: metadata byte budget", ErrDirectGrantsSnapshotUnavailable)
		}
		b.usedBytes += size
		b.seen[string(key)] = struct{}{}
	}
	return nil
}

type directGrantsReadLimits struct {
	scanned int
	rows    int
	bytes   int
}

func directGrantReadKeys(ids ...string) [][]byte {
	keys := [][]byte{
		appV23RootKey(),
		stateKey("height"),
		stateKey("app_hash"),
	}
	for _, id := range ids {
		for _, key := range [][]byte{
			appV23EnrollmentKey(id),
			appV23RoleKey(id),
			appV23ProjectedAgentKey(id),
			agentOnChainKey(id),
			appV23RootCredentialKey(id),
		} {
			keys = append(keys, key, consensuskeys.AppV23MigrationStageKey(key))
		}
	}
	return keys
}

func activeOrdinaryAgentInSnapshot(reader *BadgerStore, root *AppV23RootState, id string) (*AppV23LocalEnrollment, *AppV23RoleState, error) {
	wasRoot, err := reader.IsAppV23RootCredential(id)
	if err != nil {
		return nil, nil, err
	}
	if wasRoot || id == root.PrincipalID || id == root.CredentialID {
		return nil, nil, ErrDirectGrantsInspectionDenied
	}
	enrollment, err := reader.GetAppV23Enrollment(id)
	if err != nil {
		return nil, nil, err
	}
	role, err := reader.GetAppV23Role(id)
	if err != nil {
		return nil, nil, err
	}
	if enrollment == nil || role == nil || !enrollment.Active ||
		enrollment.AgentID != id || role.AgentID != id ||
		enrollment.Profile == AppV23ProfileRoot ||
		ValidateAppV23Policy(role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance) != nil {
		return nil, nil, ErrDirectGrantsInspectionDenied
	}
	if role.Role == AppV23RoleAdmin && enrollment.RootGeneration != root.Generation {
		return nil, nil, ErrDirectGrantsInspectionDenied
	}
	registered, err := reader.GetRegisteredAgent(id)
	if err != nil {
		return nil, nil, err
	}
	if registered == nil || registered.AgentID != id ||
		registered.Role != role.Role ||
		registered.Clearance != enrollment.Clearance ||
		registered.Capabilities != enrollment.Capabilities {
		return nil, nil, ErrDirectGrantsInspectionDenied
	}
	return enrollment, role, nil
}

// InspectDirectGrants reads the caller/target authorization records, current
// Root generation, committed height/AppHash, and exact active grant rows from
// one Badger read transaction. Only the calling agent may inspect itself; a
// current Admin may inspect another active ordinary agent. Existing REST
// locality and signed-request boundaries remain responsible for restricting
// Admin use to the local node.
func (s *BadgerStore) InspectDirectGrants(
	ctx context.Context, caller, target string, now int64,
) (*DirectGrantsSnapshot, error) {
	return s.inspectDirectGrants(ctx, caller, target, now, directGrantsReadLimits{
		scanned: directGrantsScanLimit,
		rows:    directGrantsRowLimit,
		bytes:   directGrantsByteLimit,
	})
}

func (s *BadgerStore) inspectDirectGrants(
	ctx context.Context, caller, target string, now int64, limits directGrantsReadLimits,
) (*DirectGrantsSnapshot, error) {
	if s == nil || s.db == nil || s.txn != nil || ctx == nil ||
		!isCanonicalAgentID(caller) || !isCanonicalAgentID(target) || now <= 0 ||
		limits.scanned <= 0 || limits.scanned > directGrantsScanLimit ||
		limits.rows <= 0 || limits.rows > directGrantsRowLimit ||
		limits.bytes <= 0 || limits.bytes > directGrantsByteLimit {
		return nil, fmt.Errorf("%w: invalid committed reader request", ErrDirectGrantsSnapshotUnavailable)
	}
	ctx, cancel := context.WithTimeout(ctx, directGrantsTimeout)
	defer cancel()

	var result *DirectGrantsSnapshot
	err := s.db.View(func(txn *badger.Txn) error {
		reader := &BadgerStore{
			db:                        s.db,
			txn:                       txn,
			taintServingReadOnly:      true,
			taintServingCommittedRead: true,
		}
		budget := &directGrantReadBudget{
			ctx:   ctx,
			txn:   txn,
			limit: limits.bytes,
			seen:  make(map[string]struct{}),
		}
		if err := budget.reserve(directGrantReadKeys(caller, target)...); err != nil {
			return err
		}

		root, err := reader.GetAppV23Root()
		if err != nil {
			return err
		}
		if root == nil || root.Generation == 0 ||
			!isCanonicalAgentID(root.PrincipalID) || !isCanonicalAgentID(root.CredentialID) {
			return errors.New("current Root state unavailable")
		}

		callerEnrollment, callerRole, err := activeOrdinaryAgentInSnapshot(reader, root, caller)
		if err != nil {
			return err
		}
		if caller != target {
			if callerRole.Role != AppV23RoleAdmin || callerEnrollment.RootGeneration != root.Generation {
				return ErrDirectGrantsInspectionDenied
			}
		}
		targetEnrollment, targetRole, err := activeOrdinaryAgentInSnapshot(reader, root, target)
		if err != nil {
			return err
		}

		heightValue, err := txn.Get(stateKey("height"))
		if err != nil {
			return fmt.Errorf("committed height unavailable: %w", err)
		}
		var height uint64
		if err := heightValue.Value(func(value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid committed height")
			}
			height = binary.BigEndian.Uint64(value)
			return nil
		}); err != nil {
			return err
		}
		if height == 0 || height > math.MaxInt64 {
			return errors.New("committed height unavailable")
		}
		hashValue, err := txn.Get(stateKey("app_hash"))
		if err != nil {
			return fmt.Errorf("committed AppHash unavailable: %w", err)
		}
		var appHash []byte
		if err := hashValue.Value(func(value []byte) error {
			if len(value) != 32 {
				return errors.New("invalid committed AppHash")
			}
			appHash = append([]byte(nil), value...)
			return nil
		}); err != nil {
			return err
		}

		prefix := []byte("grant:")
		suffix := []byte(":" + target)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		grants := make([]DirectGrantObservation, 0)
		scanned := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			scanned++
			if scanned > limits.scanned {
				return fmt.Errorf("%w: grant scan budget", ErrDirectGrantsSnapshotUnavailable)
			}
			item := it.Item()
			key := item.KeyCopy(nil)
			if !bytes.HasSuffix(key, suffix) {
				continue
			}
			if len(grants) >= limits.rows {
				return fmt.Errorf("%w: grant row budget", ErrDirectGrantsSnapshotUnavailable)
			}
			domain, grantee, ok := parseCanonicalGrantKey(key)
			if !ok || grantee != target {
				return errors.New("invalid canonical grant key")
			}
			size, err := directGrantItemSize(key, item, limits.bytes)
			if err != nil {
				return err
			}
			if size > limits.bytes-budget.usedBytes {
				return fmt.Errorf("%w: grant byte budget", ErrDirectGrantsSnapshotUnavailable)
			}
			budget.usedBytes += size
			var grant DirectGrantObservation
			if err := item.Value(func(value []byte) error {
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
					return nil
				}
				grant = DirectGrantObservation{
					Domain:      domain,
					GranteeID:   grantee,
					GranterID:   granter,
					AccessLevel: value[0],
					ExpiresAt:   int64(expiry), // #nosec G115 -- checked against MaxInt64 above
				}
				return nil
			}); err != nil {
				return fmt.Errorf("decode grant %q/%s: %w", domain, target, err)
			}
			if grant.Domain != "" {
				grants = append(grants, grant)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		sort.Slice(grants, func(i, j int) bool { return grants[i].Domain < grants[j].Domain })
		result = &DirectGrantsSnapshot{
			Schema:          "sage.direct-grants-snapshot.v1",
			CallerID:        caller,
			TargetAgentID:   target,
			Height:          height,
			AppHash:         hex.EncodeToString(appHash),
			RootPrincipalID: root.PrincipalID,
			RootGeneration:  root.Generation,
			EvaluatedAt:     now,
			Coverage:        "active-direct-grants-for-target",
			Complete:        true,
			Target: DirectGrantTargetIdentity{
				Active:             true,
				Profile:            targetEnrollment.Profile,
				Role:               targetRole.Role,
				Clearance:          targetEnrollment.Clearance,
				Capabilities:       targetEnrollment.Capabilities,
				RootGeneration:     targetEnrollment.RootGeneration,
				EnrollmentRevision: targetEnrollment.Revision,
				RoleRevision:       targetRole.Revision,
			},
			Grants: grants,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inspect direct grants: %w", err)
	}
	return result, nil
}
