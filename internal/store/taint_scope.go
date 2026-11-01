package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const taintScopeStateVersion uint8 = 1

// TaintDecisionScope is the immutable matching rule carried by one accepted
// apply decision. The full apply payload remains bound to the decision through
// Header.PayloadHash.
type TaintDecisionScope struct {
	Kind           uint8
	PrincipalID    string
	GenerationFrom uint64
	GenerationTo   uint64
	Credentials    []string
}

// AuthorProvenance is the canonical per-signer description of who created a
// memory. Exact is true only for a memprov:v30 marker. CoverageKnown is false
// for records whose creation provenance cannot be established.
type AuthorProvenance struct {
	Signers       []AppV30ProvenanceSigner
	Exact         bool
	CoverageKnown bool
}

type activeTaintDecisionScope struct {
	DecisionID string
	Header     taint.Header
	Scope      TaintDecisionScope
}

type taintUnitMeter struct {
	units int
	limit int
	err   error
}

func newTaintUnitMeter(limit int, err error) *taintUnitMeter {
	return &taintUnitMeter{limit: limit, err: err}
}

func (m *taintUnitMeter) charge(n int) error {
	if n < 0 {
		return fmt.Errorf("%w: negative taint unit charge", ErrTaintStateMalformed)
	}
	if m.limit > 0 && m.units+n > m.limit {
		if m.err != nil {
			return m.err
		}
		return ErrTaintServingWalkBudget
	}
	m.units += n
	return nil
}

func TaintDecisionScopeKey(decisionID string) []byte {
	return []byte("taintscope:" + decisionID)
}

func TaintDecisionScopeFromPayload(payload tx.TaintApplyPayload) TaintDecisionScope {
	return TaintDecisionScope{
		Kind:           payload.ScopeKind,
		PrincipalID:    payload.PrincipalID,
		GenerationFrom: payload.GenerationFrom,
		GenerationTo:   payload.GenerationTo,
		Credentials:    append([]string(nil), payload.Credentials...),
	}
}

func (scope TaintDecisionScope) Matches(provenance AuthorProvenance) bool {
	if !provenance.CoverageKnown {
		return false
	}
	switch scope.Kind {
	case tx.TaintScopeDirect:
		return false
	case tx.TaintScopeRootGeneration:
		for _, signer := range provenance.Signers {
			if !signer.Exact ||
				signer.PrincipalID != scope.PrincipalID ||
				signer.RootGeneration < scope.GenerationFrom ||
				signer.RootGeneration > scope.GenerationTo {
				continue
			}
			return true
		}
	case tx.TaintScopeCredentialSet:
		for _, credential := range scope.Credentials {
			for _, signer := range provenance.Signers {
				if signer.CredentialID == credential {
					return true
				}
			}
		}
	}
	return false
}

func encodeTaintDecisionScope(
	payloadHash [sha256.Size]byte,
	decisionScope TaintDecisionScope,
) ([]byte, error) {
	if err := validateTaintDecisionScope(decisionScope); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 64+len(decisionScope.PrincipalID)+len(decisionScope.Credentials)*66)
	encoded = append(encoded, taintScopeStateVersion)
	encoded = append(encoded, payloadHash[:]...)
	encoded = append(encoded, decisionScope.Kind)
	switch decisionScope.Kind {
	case tx.TaintScopeDirect:
	case tx.TaintScopeRootGeneration:
		encoded = appendTaintScopeBytes(encoded, []byte(decisionScope.PrincipalID))
		encoded = binary.BigEndian.AppendUint64(encoded, decisionScope.GenerationFrom)
		encoded = binary.BigEndian.AppendUint64(encoded, decisionScope.GenerationTo)
	case tx.TaintScopeCredentialSet:
		encoded = append(encoded, byte(len(decisionScope.Credentials)))
		for _, credential := range decisionScope.Credentials {
			encoded = appendTaintScopeBytes(encoded, []byte(credential))
		}
	}
	return encoded, nil
}

func decodeTaintDecisionScope(encoded []byte) ([sha256.Size]byte, TaintDecisionScope, error) {
	var payloadHash [sha256.Size]byte
	var decisionScope TaintDecisionScope
	if len(encoded) < 1+sha256.Size+1 || encoded[0] != taintScopeStateVersion {
		return payloadHash, decisionScope, fmt.Errorf(
			"%w: invalid taint decision scope encoding",
			ErrTaintStateMalformed,
		)
	}
	offset := 1
	copy(payloadHash[:], encoded[offset:offset+sha256.Size])
	offset += sha256.Size
	decisionScope.Kind = encoded[offset]
	offset++
	switch decisionScope.Kind {
	case tx.TaintScopeDirect:
	case tx.TaintScopeRootGeneration:
		principal, next, err := readTaintScopeBytes(encoded, offset)
		if err != nil {
			return payloadHash, decisionScope, err
		}
		offset = next
		if len(encoded)-offset < 16 {
			return payloadHash, decisionScope, fmt.Errorf(
				"%w: truncated Root generation range",
				ErrTaintStateMalformed,
			)
		}
		decisionScope.PrincipalID = string(principal)
		decisionScope.GenerationFrom = binary.BigEndian.Uint64(encoded[offset:])
		offset += 8
		decisionScope.GenerationTo = binary.BigEndian.Uint64(encoded[offset:])
		offset += 8
	case tx.TaintScopeCredentialSet:
		if offset >= len(encoded) {
			return payloadHash, decisionScope, fmt.Errorf(
				"%w: truncated credential count",
				ErrTaintStateMalformed,
			)
		}
		count := int(encoded[offset])
		offset++
		if count == 0 || count > tx.MaxTaintCredentialSetEntries {
			return payloadHash, decisionScope, fmt.Errorf(
				"%w: invalid credential count %d",
				ErrTaintStateMalformed,
				count,
			)
		}
		decisionScope.Credentials = make([]string, 0, count)
		for i := 0; i < count; i++ {
			credential, next, err := readTaintScopeBytes(encoded, offset)
			if err != nil {
				return payloadHash, decisionScope, err
			}
			offset = next
			decisionScope.Credentials = append(decisionScope.Credentials, string(credential))
		}
	default:
		return payloadHash, decisionScope, fmt.Errorf(
			"%w: unsupported taint scope kind %d",
			ErrTaintStateMalformed,
			decisionScope.Kind,
		)
	}
	if offset != len(encoded) {
		return payloadHash, decisionScope, fmt.Errorf(
			"%w: taint decision scope has %d trailing bytes",
			ErrTaintStateMalformed,
			len(encoded)-offset,
		)
	}
	if err := validateTaintDecisionScope(decisionScope); err != nil {
		return payloadHash, decisionScope, err
	}
	return payloadHash, decisionScope, nil
}

func validateTaintDecisionScope(decisionScope TaintDecisionScope) error {
	switch decisionScope.Kind {
	case tx.TaintScopeDirect:
		if decisionScope.PrincipalID != "" ||
			decisionScope.GenerationFrom != 0 ||
			decisionScope.GenerationTo != 0 ||
			len(decisionScope.Credentials) != 0 {
			return fmt.Errorf("%w: direct taint scope contains matching data", ErrTaintStateMalformed)
		}
	case tx.TaintScopeRootGeneration:
		if len(decisionScope.PrincipalID) == 0 ||
			len(decisionScope.PrincipalID) > tx.MaxTaintPrincipalIDBytes ||
			decisionScope.GenerationFrom == 0 ||
			decisionScope.GenerationTo < decisionScope.GenerationFrom ||
			len(decisionScope.Credentials) != 0 {
			return fmt.Errorf("%w: invalid Root generation taint scope", ErrTaintStateMalformed)
		}
	case tx.TaintScopeCredentialSet:
		if decisionScope.PrincipalID != "" ||
			decisionScope.GenerationFrom != 0 ||
			decisionScope.GenerationTo != 0 ||
			len(decisionScope.Credentials) == 0 ||
			len(decisionScope.Credentials) > tx.MaxTaintCredentialSetEntries {
			return fmt.Errorf("%w: invalid credential-set taint scope", ErrTaintStateMalformed)
		}
		for i, credential := range decisionScope.Credentials {
			if !isCanonicalAgentID(credential) {
				return fmt.Errorf("%w: non-canonical taint credential", ErrTaintStateMalformed)
			}
			if i > 0 && bytes.Compare(
				[]byte(decisionScope.Credentials[i-1]),
				[]byte(credential),
			) >= 0 {
				return fmt.Errorf("%w: taint credentials are not sorted", ErrTaintStateMalformed)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported taint scope kind %d", ErrTaintStateMalformed, decisionScope.Kind)
	}
	return nil
}

func equalTaintDecisionScope(a, b TaintDecisionScope) bool {
	if a.Kind != b.Kind ||
		a.PrincipalID != b.PrincipalID ||
		a.GenerationFrom != b.GenerationFrom ||
		a.GenerationTo != b.GenerationTo ||
		len(a.Credentials) != len(b.Credentials) {
		return false
	}
	for i := range a.Credentials {
		if a.Credentials[i] != b.Credentials[i] {
			return false
		}
	}
	return true
}

func appendTaintScopeBytes(dst, value []byte) []byte {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value))) // #nosec G115 -- validated caller bounds
	dst = append(dst, length[:]...)
	return append(dst, value...)
}

func readTaintScopeBytes(data []byte, offset int) ([]byte, int, error) {
	if len(data)-offset < 2 {
		return nil, offset, fmt.Errorf("%w: truncated taint scope string", ErrTaintStateMalformed)
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length == 0 || len(data)-offset < length {
		return nil, offset, fmt.Errorf("%w: invalid taint scope string length", ErrTaintStateMalformed)
	}
	return append([]byte(nil), data[offset:offset+length]...), offset + length, nil
}

func readMeteredTaintValueTxn(
	txn *badger.Txn,
	key []byte,
	meter *taintUnitMeter,
) ([]byte, bool, error) {
	if meter != nil {
		if err := meter.charge(1); err != nil {
			return nil, false, err
		}
	}
	return readTaintValueTxn(txn, key)
}

func readTaintDecisionScopeTxn(
	txn *badger.Txn,
	decisionID string,
	payloadHash [sha256.Size]byte,
	meter *taintUnitMeter,
) (TaintDecisionScope, bool, error) {
	raw, found, err := readMeteredTaintValueTxn(txn, TaintDecisionScopeKey(decisionID), meter)
	if err != nil || !found {
		return TaintDecisionScope{}, found, err
	}
	storedHash, decisionScope, err := decodeTaintDecisionScope(raw)
	if err != nil {
		return TaintDecisionScope{}, false, err
	}
	if storedHash != payloadHash {
		return TaintDecisionScope{}, false, fmt.Errorf(
			"%w: taint decision scope payload hash does not match the header",
			ErrTaintStateMalformed,
		)
	}
	return decisionScope, true, nil
}

func readActiveTaintDecisionScopesTxn(
	txn *badger.Txn,
	meter *taintUnitMeter,
) ([]activeTaintDecisionScope, error) {
	prefix := taint.ActiveDecisionPrefix()
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := txn.NewIterator(options)
	defer it.Close()
	if meter != nil {
		if err := meter.charge(1); err != nil {
			return nil, err
		}
	}
	scopes := make([]activeTaintDecisionScope, 0, taint.MaxActiveDecisions)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if meter != nil {
			if err := meter.charge(1); err != nil {
				return nil, err
			}
		}
		decisionID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
		if err := taint.ValidateDecisionID(decisionID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
		if meter != nil {
			if err := meter.charge(1); err != nil {
				return nil, err
			}
		}
		header, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: missing taint decision header", ErrTaintStateMalformed)
		}
		if !header.Status.Active() {
			return nil, fmt.Errorf("%w: active index references non-active decision", ErrTaintStateMalformed)
		}
		decisionScope, found, err := readTaintDecisionScopeTxn(
			txn,
			decisionID,
			header.PayloadHash,
			meter,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf(
				"%w: active taint decision has no canonical scope",
				ErrTaintStateMalformed,
			)
		}
		scopes = append(scopes, activeTaintDecisionScope{
			DecisionID: decisionID,
			Header:     header,
			Scope:      decisionScope,
		})
		if len(scopes) > taint.MaxActiveDecisions {
			return nil, fmt.Errorf("%w: active taint index exceeds cap", ErrTaintStateMalformed)
		}
	}
	return scopes, nil
}

func (s *BadgerStore) AuthorProvenance(memoryID string) (AuthorProvenance, error) {
	var provenance AuthorProvenance
	err := s.view(func(txn *badger.Txn) error {
		meter := newTaintUnitMeter(taint.MaxServingRequestUnits, ErrTaintServingWalkBudget)
		var err error
		provenance, err = authorProvenanceTxn(txn, memoryID, meter)
		return err
	})
	return provenance, err
}

func authorProvenanceTxn(
	txn *badger.Txn,
	memoryID string,
	meter *taintUnitMeter,
) (AuthorProvenance, error) {
	var provenance AuthorProvenance
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return provenance, fmt.Errorf("%w: invalid author provenance memory ID", ErrTaintServingState)
	}
	raw, found, err := readMeteredTaintValueTxn(txn, AppV30ProvenanceKey(memoryID), meter)
	if err != nil {
		return provenance, err
	}
	if found {
		signers, err := decodeAppV30Provenance(raw)
		if err != nil {
			return provenance, fmt.Errorf("%w: %v", ErrTaintServingState, err)
		}
		if meter != nil {
			if err := meter.charge(len(signers)); err != nil {
				return provenance, err
			}
		}
		provenance.Signers = signers
		provenance.Exact = true
		provenance.CoverageKnown = true
		return provenance, nil
	}

	migratedValue, migrated, err := readMeteredTaintValueTxn(
		txn,
		appV30MigrationMarkerKey(memoryID),
		meter,
	)
	if err != nil {
		return provenance, err
	}
	if migrated && !bytes.Equal(migratedValue, []byte{1}) {
		return provenance, fmt.Errorf("%w: malformed migration marker", ErrTaintServingState)
	}
	adoptionReceipt, adopted, err := readMeteredTaintValueTxn(
		txn,
		memoryLegacyAdoptionReceiptKey(memoryID),
		meter,
	)
	if err != nil {
		return provenance, err
	}
	if adopted && len(adoptionReceipt) != sha256.Size {
		return provenance, fmt.Errorf("%w: malformed adoption receipt", ErrTaintServingState)
	}
	principal, principalFound, err := readMeteredTaintValueTxn(
		txn,
		memoryAuthorPrincipalKey(memoryID),
		meter,
	)
	if err != nil {
		return provenance, err
	}
	if migrated || adopted || !principalFound || len(principal) == 0 {
		return provenance, nil
	}

	coauthors, coauthorFound, err := readMeteredTaintValueTxn(
		txn,
		cocommitCoauthorsKey(memoryID),
		meter,
	)
	if err != nil {
		return provenance, err
	}
	if coauthorFound {
		decoded, err := tx.DecodeCoauthorsCanonical(coauthors)
		if err != nil {
			return provenance, fmt.Errorf("%w: decode coauthors: %v", ErrTaintServingState, err)
		}
		if len(decoded) == 0 || len(decoded) > taint.MaxSigners {
			return provenance, fmt.Errorf("%w: invalid coauthor count", ErrTaintServingState)
		}
		if meter != nil {
			if err := meter.charge(len(decoded)); err != nil {
				return provenance, err
			}
		}
		provenance.Signers = make([]AppV30ProvenanceSigner, 0, len(decoded))
		for _, coauthor := range decoded {
			credentialID := hex.EncodeToString(coauthor.PubKey)
			if !isCanonicalAgentID(credentialID) {
				return provenance, fmt.Errorf("%w: non-canonical coauthor", ErrTaintServingState)
			}
			provenance.Signers = append(provenance.Signers, AppV30ProvenanceSigner{
				CredentialID: credentialID,
			})
		}
		provenance.CoverageKnown = true
		return provenance, nil
	}

	scopedRaw, scopedFound, err := readMeteredTaintValueTxn(txn, scopedContentKey(memoryID), meter)
	if err != nil {
		return provenance, err
	}
	if scopedFound {
		content, err := scope.DecodeContent(scopedRaw)
		if err != nil || content.MemoryID != memoryID {
			return provenance, fmt.Errorf("%w: invalid scoped author provenance", ErrTaintServingState)
		}
		if !isCanonicalAgentID(content.SubmittingAgentID) {
			return provenance, nil
		}
		provenance.Signers = []AppV30ProvenanceSigner{{
			CredentialID: content.SubmittingAgentID,
		}}
		provenance.CoverageKnown = true
		return provenance, nil
	}

	author, authorFound, err := readMeteredTaintValueTxn(txn, memoryAuthorKey(memoryID), meter)
	if err != nil {
		return provenance, err
	}
	if !authorFound || len(author) == 0 || !isCanonicalAgentID(string(author)) {
		return provenance, nil
	}
	provenance.Signers = []AppV30ProvenanceSigner{{
		CredentialID: string(author),
	}}
	provenance.CoverageKnown = true
	return provenance, nil
}

func appendDecisionIDUnique(decisionIDs []string, decisionID string) []string {
	for _, existing := range decisionIDs {
		if existing == decisionID {
			return decisionIDs
		}
	}
	return append(decisionIDs, decisionID)
}

func sortedUniqueDecisionIDs(decisionIDs []string) []string {
	if len(decisionIDs) < 2 {
		return decisionIDs
	}
	sort.Strings(decisionIDs)
	write := 1
	for read := 1; read < len(decisionIDs); read++ {
		if decisionIDs[read] == decisionIDs[write-1] {
			continue
		}
		decisionIDs[write] = decisionIDs[read]
		write++
	}
	return decisionIDs[:write]
}
