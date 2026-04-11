package tx

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Wire format: [1-byte type][payload][64-byte signature][32-byte pubkey][8-byte nonce][8-byte unix-nano timestamp]
// The payload is type-specific, each field length-prefixed with a 4-byte big-endian uint32.

var (
	ErrInvalidTxData   = errors.New("invalid transaction data")
	ErrUnknownTxType   = errors.New("unknown transaction type")
	ErrSignatureLength = errors.New("invalid signature length")
	ErrPublicKeyLength = errors.New("invalid public key length")
)

// EncodeTx serializes a ParsedTx to deterministic bytes.
func EncodeTx(tx *ParsedTx) ([]byte, error) {
	payload, err := encodePayload(tx)
	if err != nil {
		return nil, err
	}

	sig := tx.Signature
	if sig == nil {
		sig = make([]byte, ed25519.SignatureSize)
	}
	pub := tx.PublicKey
	if pub == nil {
		pub = make([]byte, ed25519.PublicKeySize)
	}

	// Agent auth fields (may be nil for legacy/unsigned txs)
	agentPub := tx.AgentPubKey
	if agentPub == nil {
		agentPub = make([]byte, ed25519.PublicKeySize)
	}
	agentSig := tx.AgentSig
	if agentSig == nil {
		agentSig = make([]byte, ed25519.SignatureSize)
	}
	agentBodyHash := tx.AgentBodyHash
	if agentBodyHash == nil {
		agentBodyHash = make([]byte, 32)
	}

	agentNonce := tx.AgentNonce
	if agentNonce == nil {
		agentNonce = []byte{}
	}

	// type(1) + payloadLen(4) + payload + nodeSig(64) + nodePub(32) + nonce(8) + timestamp(8)
	// + agentPub(32) + agentSig(64) + agentTs(8) + agentBodyHash(32) + agentNonceLen(4) + agentNonce(var)
	totalLen := 1 + 4 + len(payload) + ed25519.SignatureSize + ed25519.PublicKeySize + 8 + 8 +
		ed25519.PublicKeySize + ed25519.SignatureSize + 8 + 32 + 4 + len(agentNonce)
	buf := make([]byte, 0, totalLen)
	buf = append(buf, byte(tx.Type))
	buf = appendUint32(buf, uint32(len(payload))) // #nosec G115 -- payload length fits in uint32
	buf = append(buf, payload...)
	buf = append(buf, sig...)
	buf = append(buf, pub...)
	buf = appendUint64(buf, tx.Nonce)
	buf = appendInt64(buf, tx.Timestamp.UnixNano())
	// Agent identity proof (new fields)
	buf = append(buf, agentPub...)
	buf = append(buf, agentSig...)
	buf = appendInt64(buf, tx.AgentTimestamp)
	buf = append(buf, agentBodyHash...)
	// Agent nonce (variable length, length-prefixed)
	buf = appendUint32(buf, uint32(len(agentNonce))) // #nosec G115 -- nonce length fits in uint32
	buf = append(buf, agentNonce...)

	return buf, nil
}

// DecodeTx deserializes bytes into a ParsedTx.
func DecodeTx(data []byte) (*ParsedTx, error) {
	if len(data) < 1+4 {
		return nil, ErrInvalidTxData
	}

	txType := TxType(data[0])
	payloadLen := binary.BigEndian.Uint32(data[1:5])
	if payloadLen > math.MaxInt32 {
		return nil, ErrInvalidTxData
	}

	// Minimum (legacy): type(1) + payloadLen(4) + payload + sig(64) + pub(32) + nonce(8) + ts(8)
	legacyLen := 1 + 4 + int(payloadLen) + ed25519.SignatureSize + ed25519.PublicKeySize + 8 + 8 // #nosec G115 -- payloadLen validated
	// Full: legacy + agentPub(32) + agentSig(64) + agentTs(8) + agentBodyHash(32)
	fullLen := legacyLen + ed25519.PublicKeySize + ed25519.SignatureSize + 8 + 32
	if len(data) < legacyLen {
		return nil, ErrInvalidTxData
	}

	offset := 5
	payload := data[offset : offset+int(payloadLen)]  // #nosec G115 -- payloadLen validated above
	offset += int(payloadLen)                          // #nosec G115 -- payloadLen validated above

	sig := make([]byte, ed25519.SignatureSize)
	copy(sig, data[offset:offset+ed25519.SignatureSize])
	offset += ed25519.SignatureSize

	pub := make([]byte, ed25519.PublicKeySize)
	copy(pub, data[offset:offset+ed25519.PublicKeySize])
	offset += ed25519.PublicKeySize

	nonce := binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	tsNano := int64(binary.BigEndian.Uint64(data[offset : offset+8])) // #nosec G115 -- timestamp conversion safe
	offset += 8

	tx := &ParsedTx{
		Type:      txType,
		Signature: sig,
		PublicKey: pub,
		Nonce:     nonce,
		Timestamp: time.Unix(0, tsNano),
	}

	// Decode agent auth fields if present (new format)
	if len(data) >= fullLen {
		tx.AgentPubKey = make([]byte, ed25519.PublicKeySize)
		copy(tx.AgentPubKey, data[offset:offset+ed25519.PublicKeySize])
		offset += ed25519.PublicKeySize

		tx.AgentSig = make([]byte, ed25519.SignatureSize)
		copy(tx.AgentSig, data[offset:offset+ed25519.SignatureSize])
		offset += ed25519.SignatureSize

		tx.AgentTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8])) // #nosec G115 -- timestamp conversion safe
		offset += 8

		tx.AgentBodyHash = make([]byte, 32)
		copy(tx.AgentBodyHash, data[offset:offset+32])
		offset += 32

		// Decode agent nonce if present (length-prefixed, variable length)
		if offset+4 <= len(data) {
			nonceLen := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
			if nonceLen > 0 && offset+int(nonceLen) <= len(data) { // #nosec G115 -- nonceLen validated
				tx.AgentNonce = make([]byte, nonceLen)
				copy(tx.AgentNonce, data[offset:offset+int(nonceLen)]) // #nosec G115 -- nonceLen validated
			}
		}
	}

	if err := decodePayload(tx, payload); err != nil {
		return nil, err
	}

	return tx, nil
}

// SignTx computes an Ed25519 signature over the transaction payload and sets
// the Signature and PublicKey fields on the transaction.
func SignTx(tx *ParsedTx, privateKey ed25519.PrivateKey) error {
	payload, err := signingPayload(tx)
	if err != nil {
		return fmt.Errorf("signing payload: %w", err)
	}
	pub, _ := privateKey.Public().(ed25519.PublicKey)
	tx.PublicKey = pub
	tx.Signature = ed25519.Sign(privateKey, payload)
	return nil
}

// VerifyTx verifies the Ed25519 signature on a transaction.
func VerifyTx(tx *ParsedTx) (bool, error) {
	if len(tx.PublicKey) != ed25519.PublicKeySize {
		return false, ErrPublicKeyLength
	}
	if len(tx.Signature) != ed25519.SignatureSize {
		return false, ErrSignatureLength
	}
	payload, err := signingPayload(tx)
	if err != nil {
		return false, fmt.Errorf("signing payload: %w", err)
	}
	return ed25519.Verify(ed25519.PublicKey(tx.PublicKey), payload, tx.Signature), nil
}

// signingPayload computes the deterministic bytes-to-sign for a transaction.
// It encodes the tx with node signature/pubkey zeroed, then takes a SHA-256 hash.
// Agent auth fields are preserved in the hash — the node signs over the agent's
// identity proof, binding the two signatures together.
func signingPayload(tx *ParsedTx) ([]byte, error) {
	// Create a copy with node signature/pubkey zeroed (NOT agent fields)
	clone := *tx
	clone.Signature = make([]byte, ed25519.SignatureSize)
	clone.PublicKey = make([]byte, ed25519.PublicKeySize)

	encoded, err := EncodeTx(&clone)
	if err != nil {
		return nil, fmt.Errorf("encode failed: %w", err)
	}

	hash := sha256.Sum256(encoded)
	return hash[:], nil
}

// --- payload encoding helpers ---

func encodePayload(tx *ParsedTx) ([]byte, error) {
	switch tx.Type {
	case TxTypeMemorySubmit:
		if tx.MemorySubmit == nil {
			return nil, fmt.Errorf("MemorySubmit is nil for submit tx")
		}
		return encodeMemorySubmit(tx.MemorySubmit), nil
	case TxTypeMemoryVote:
		if tx.MemoryVote == nil {
			return nil, fmt.Errorf("MemoryVote is nil for vote tx")
		}
		return encodeMemoryVote(tx.MemoryVote), nil
	case TxTypeMemoryChallenge:
		if tx.MemoryChallenge == nil {
			return nil, fmt.Errorf("MemoryChallenge is nil for challenge tx")
		}
		return encodeMemoryChallenge(tx.MemoryChallenge), nil
	case TxTypeMemoryCorroborate:
		if tx.MemoryCorroborate == nil {
			return nil, fmt.Errorf("MemoryCorroborate is nil for corroborate tx")
		}
		return encodeMemoryCorroborate(tx.MemoryCorroborate), nil
	case TxTypeAccessRequest:
		if tx.AccessRequest == nil {
			return nil, fmt.Errorf("AccessRequest is nil for access request tx")
		}
		return encodeAccessRequest(tx.AccessRequest), nil
	case TxTypeAccessGrant:
		if tx.AccessGrant == nil {
			return nil, fmt.Errorf("AccessGrant is nil for access grant tx")
		}
		return encodeAccessGrant(tx.AccessGrant), nil
	case TxTypeAccessRevoke:
		if tx.AccessRevoke == nil {
			return nil, fmt.Errorf("AccessRevoke is nil for access revoke tx")
		}
		return encodeAccessRevoke(tx.AccessRevoke), nil
	case TxTypeAccessQuery:
		if tx.AccessQuery == nil {
			return nil, fmt.Errorf("AccessQuery is nil for access query tx")
		}
		return encodeAccessQuery(tx.AccessQuery), nil
	case TxTypeDomainRegister:
		if tx.DomainRegister == nil {
			return nil, fmt.Errorf("DomainRegister is nil for domain register tx")
		}
		return encodeDomainRegister(tx.DomainRegister), nil
	case TxTypeOrgRegister:
		if tx.OrgRegister == nil {
			return nil, fmt.Errorf("OrgRegister is nil for org register tx")
		}
		return encodeOrgRegister(tx.OrgRegister), nil
	case TxTypeOrgAddMember:
		if tx.OrgAddMember == nil {
			return nil, fmt.Errorf("OrgAddMember is nil for org add member tx")
		}
		return encodeOrgAddMember(tx.OrgAddMember), nil
	case TxTypeOrgRemoveMember:
		if tx.OrgRemoveMember == nil {
			return nil, fmt.Errorf("OrgRemoveMember is nil for org remove member tx")
		}
		return encodeOrgRemoveMember(tx.OrgRemoveMember), nil
	case TxTypeOrgSetClearance:
		if tx.OrgSetClearance == nil {
			return nil, fmt.Errorf("OrgSetClearance is nil for org set clearance tx")
		}
		return encodeOrgSetClearance(tx.OrgSetClearance), nil
	case TxTypeFederationPropose:
		if tx.FederationPropose == nil {
			return nil, fmt.Errorf("FederationPropose is nil for federation propose tx")
		}
		return encodeFederationPropose(tx.FederationPropose), nil
	case TxTypeFederationApprove:
		if tx.FederationApprove == nil {
			return nil, fmt.Errorf("FederationApprove is nil for federation approve tx")
		}
		return encodeFederationApprove(tx.FederationApprove), nil
	case TxTypeFederationRevoke:
		if tx.FederationRevoke == nil {
			return nil, fmt.Errorf("FederationRevoke is nil for federation revoke tx")
		}
		return encodeFederationRevoke(tx.FederationRevoke), nil
	case TxTypeDeptRegister:
		if tx.DeptRegister == nil {
			return nil, fmt.Errorf("DeptRegister is nil for dept register tx")
		}
		return encodeDeptRegister(tx.DeptRegister), nil
	case TxTypeDeptAddMember:
		if tx.DeptAddMember == nil {
			return nil, fmt.Errorf("DeptAddMember is nil for dept add member tx")
		}
		return encodeDeptAddMember(tx.DeptAddMember), nil
	case TxTypeDeptRemoveMember:
		if tx.DeptRemoveMember == nil {
			return nil, fmt.Errorf("DeptRemoveMember is nil for dept remove member tx")
		}
		return encodeDeptRemoveMember(tx.DeptRemoveMember), nil
	case TxTypeAgentRegister:
		if tx.AgentRegister == nil {
			return nil, fmt.Errorf("AgentRegister is nil for agent register tx")
		}
		return encodeAgentRegister(tx.AgentRegister), nil
	case TxTypeAgentUpdate:
		if tx.AgentUpdateTx == nil {
			return nil, fmt.Errorf("AgentUpdate is nil for agent update tx")
		}
		return encodeAgentUpdate(tx.AgentUpdateTx), nil
	case TxTypeAgentSetPermission:
		if tx.AgentSetPermission == nil {
			return nil, fmt.Errorf("AgentSetPermission is nil for agent set permission tx")
		}
		return encodeAgentSetPermission(tx.AgentSetPermission), nil
	case TxTypeMemoryReassign:
		if tx.MemoryReassign == nil {
			return nil, fmt.Errorf("MemoryReassign is nil for memory reassign tx")
		}
		return encodeMemoryReassign(tx.MemoryReassign), nil
	case TxTypeGovPropose:
		if tx.GovPropose == nil {
			return nil, fmt.Errorf("GovPropose is nil for gov propose tx")
		}
		return encodeGovPropose(tx.GovPropose), nil
	case TxTypeGovVote:
		if tx.GovVote == nil {
			return nil, fmt.Errorf("GovVote is nil for gov vote tx")
		}
		return encodeGovVote(tx.GovVote), nil
	case TxTypeGovCancel:
		if tx.GovCancel == nil {
			return nil, fmt.Errorf("GovCancel is nil for gov cancel tx")
		}
		return encodeGovCancel(tx.GovCancel), nil
	default:
		return nil, ErrUnknownTxType
	}
}

func decodePayload(tx *ParsedTx, data []byte) error {
	switch tx.Type {
	case TxTypeMemorySubmit:
		s, err := decodeMemorySubmit(data)
		if err != nil {
			return err
		}
		tx.MemorySubmit = s
		return nil
	case TxTypeMemoryVote:
		v, err := decodeMemoryVote(data)
		if err != nil {
			return err
		}
		tx.MemoryVote = v
		return nil
	case TxTypeMemoryChallenge:
		c, err := decodeMemoryChallenge(data)
		if err != nil {
			return err
		}
		tx.MemoryChallenge = c
		return nil
	case TxTypeMemoryCorroborate:
		c, err := decodeMemoryCorroborate(data)
		if err != nil {
			return err
		}
		tx.MemoryCorroborate = c
		return nil
	case TxTypeAccessRequest:
		a, err := decodeAccessRequest(data)
		if err != nil {
			return err
		}
		tx.AccessRequest = a
		return nil
	case TxTypeAccessGrant:
		a, err := decodeAccessGrant(data)
		if err != nil {
			return err
		}
		tx.AccessGrant = a
		return nil
	case TxTypeAccessRevoke:
		a, err := decodeAccessRevoke(data)
		if err != nil {
			return err
		}
		tx.AccessRevoke = a
		return nil
	case TxTypeAccessQuery:
		a, err := decodeAccessQuery(data)
		if err != nil {
			return err
		}
		tx.AccessQuery = a
		return nil
	case TxTypeDomainRegister:
		d, err := decodeDomainRegister(data)
		if err != nil {
			return err
		}
		tx.DomainRegister = d
		return nil
	case TxTypeOrgRegister:
		o, err := decodeOrgRegister(data)
		if err != nil {
			return err
		}
		tx.OrgRegister = o
		return nil
	case TxTypeOrgAddMember:
		o, err := decodeOrgAddMember(data)
		if err != nil {
			return err
		}
		tx.OrgAddMember = o
		return nil
	case TxTypeOrgRemoveMember:
		o, err := decodeOrgRemoveMember(data)
		if err != nil {
			return err
		}
		tx.OrgRemoveMember = o
		return nil
	case TxTypeOrgSetClearance:
		o, err := decodeOrgSetClearance(data)
		if err != nil {
			return err
		}
		tx.OrgSetClearance = o
		return nil
	case TxTypeFederationPropose:
		f, err := decodeFederationPropose(data)
		if err != nil {
			return err
		}
		tx.FederationPropose = f
		return nil
	case TxTypeFederationApprove:
		f, err := decodeFederationApprove(data)
		if err != nil {
			return err
		}
		tx.FederationApprove = f
		return nil
	case TxTypeFederationRevoke:
		f, err := decodeFederationRevoke(data)
		if err != nil {
			return err
		}
		tx.FederationRevoke = f
		return nil
	case TxTypeDeptRegister:
		d, err := decodeDeptRegister(data)
		if err != nil {
			return err
		}
		tx.DeptRegister = d
		return nil
	case TxTypeDeptAddMember:
		d, err := decodeDeptAddMember(data)
		if err != nil {
			return err
		}
		tx.DeptAddMember = d
		return nil
	case TxTypeDeptRemoveMember:
		d, err := decodeDeptRemoveMember(data)
		if err != nil {
			return err
		}
		tx.DeptRemoveMember = d
		return nil
	case TxTypeAgentRegister:
		a, err := decodeAgentRegister(data)
		if err != nil {
			return err
		}
		tx.AgentRegister = a
		return nil
	case TxTypeAgentUpdate:
		a, err := decodeAgentUpdate(data)
		if err != nil {
			return err
		}
		tx.AgentUpdateTx = a
		return nil
	case TxTypeAgentSetPermission:
		a, err := decodeAgentSetPermission(data)
		if err != nil {
			return err
		}
		tx.AgentSetPermission = a
		return nil
	case TxTypeMemoryReassign:
		m, err := decodeMemoryReassign(data)
		if err != nil {
			return err
		}
		tx.MemoryReassign = m
		return nil
	case TxTypeGovPropose:
		g, err := decodeGovPropose(data)
		if err != nil {
			return err
		}
		tx.GovPropose = g
		return nil
	case TxTypeGovVote:
		g, err := decodeGovVote(data)
		if err != nil {
			return err
		}
		tx.GovVote = g
		return nil
	case TxTypeGovCancel:
		g, err := decodeGovCancel(data)
		if err != nil {
			return err
		}
		tx.GovCancel = g
		return nil
	default:
		return ErrUnknownTxType
	}
}

// --- MemorySubmit ---

func encodeMemorySubmit(s *MemorySubmit) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(s.MemoryID))
	buf = appendBytes(buf, s.ContentHash)
	buf = appendBytes(buf, s.EmbeddingHash)
	buf = append(buf, byte(s.MemoryType))
	buf = appendBytes(buf, []byte(s.DomainTag))
	buf = appendFloat64(buf, s.ConfidenceScore)
	buf = appendBytes(buf, []byte(s.Content))
	buf = appendBytes(buf, []byte(s.ParentHash))
	buf = append(buf, byte(s.Classification))
	buf = appendBytes(buf, []byte(s.TaskStatus))
	return buf
}

func decodeMemorySubmit(data []byte) (*MemorySubmit, error) {
	s := &MemorySubmit{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	s.MemoryID = string(b)

	s.ContentHash, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}

	s.EmbeddingHash, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	s.MemoryType = MemoryType(data[off])
	off++

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	s.DomainTag = string(b)

	s.ConfidenceScore, off, err = readFloat64(data, off)
	if err != nil {
		return nil, err
	}

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	s.Content = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	s.ParentHash = string(b)

	// Classification: backward compatible — default to ClearanceInternal if absent
	if off < len(data) {
		s.Classification = ClearanceLevel(data[off])
		off++
	} else {
		s.Classification = ClearanceInternal
	}

	// TaskStatus: backward compatible — empty string if absent
	if off < len(data) {
		b, _, err = readBytes(data, off)
		if err == nil {
			s.TaskStatus = string(b)
		}
	}

	return s, nil
}

// --- MemoryVote ---

func encodeMemoryVote(v *MemoryVote) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(v.MemoryID))
	buf = append(buf, byte(v.Decision))
	buf = appendBytes(buf, []byte(v.Rationale))
	return buf
}

func decodeMemoryVote(data []byte) (*MemoryVote, error) {
	v := &MemoryVote{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	v.MemoryID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	v.Decision = VoteDecision(data[off])
	off++

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	v.Rationale = string(b)

	return v, nil
}

// --- MemoryChallenge ---

func encodeMemoryChallenge(c *MemoryChallenge) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(c.MemoryID))
	buf = appendBytes(buf, []byte(c.Reason))
	buf = appendBytes(buf, []byte(c.Evidence))
	return buf
}

func decodeMemoryChallenge(data []byte) (*MemoryChallenge, error) {
	c := &MemoryChallenge{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	c.MemoryID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	c.Reason = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	c.Evidence = string(b)

	return c, nil
}

// --- MemoryCorroborate ---

func encodeMemoryCorroborate(c *MemoryCorroborate) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(c.MemoryID))
	buf = appendBytes(buf, []byte(c.Evidence))
	return buf
}

func decodeMemoryCorroborate(data []byte) (*MemoryCorroborate, error) {
	c := &MemoryCorroborate{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	c.MemoryID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	c.Evidence = string(b)

	return c, nil
}

// --- binary encoding primitives ---

func appendBytes(buf []byte, data []byte) []byte {
	buf = appendUint32(buf, uint32(len(data))) // #nosec G115 -- field data fits in uint32
	buf = append(buf, data...)
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return append(buf, b...)
}

func appendUint64(buf []byte, v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return append(buf, b...)
}

func appendInt64(buf []byte, v int64) []byte {
	return appendUint64(buf, uint64(v)) // #nosec G115 -- int64 to uint64 preserves bits
}

func appendFloat64(buf []byte, v float64) []byte {
	return appendUint64(buf, math.Float64bits(v))
}

func readBytes(data []byte, off int) ([]byte, int, error) {
	if off+4 > len(data) {
		return nil, 0, ErrInvalidTxData
	}
	l := int(binary.BigEndian.Uint32(data[off : off+4])) // #nosec G115 -- uint32 fits in int
	off += 4
	if off+l > len(data) {
		return nil, 0, ErrInvalidTxData
	}
	result := make([]byte, l)
	copy(result, data[off:off+l])
	return result, off + l, nil
}

func readFloat64(data []byte, off int) (float64, int, error) {
	if off+8 > len(data) {
		return 0, 0, ErrInvalidTxData
	}
	bits := binary.BigEndian.Uint64(data[off : off+8])
	return math.Float64frombits(bits), off + 8, nil
}

func appendFloat32(buf []byte, v float32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, math.Float32bits(v))
	return append(buf, b...)
}

func readFloat32(data []byte, off int) (float32, int, error) {
	if off+4 > len(data) {
		return 0, 0, ErrInvalidTxData
	}
	bits := binary.BigEndian.Uint32(data[off : off+4])
	return math.Float32frombits(bits), off + 4, nil
}

func readUint32(data []byte, off int) (uint32, int, error) {
	if off+4 > len(data) {
		return 0, 0, ErrInvalidTxData
	}
	v := binary.BigEndian.Uint32(data[off : off+4])
	return v, off + 4, nil
}

func readInt64(data []byte, off int) (int64, int, error) {
	if off+8 > len(data) {
		return 0, 0, ErrInvalidTxData
	}
	v := int64(binary.BigEndian.Uint64(data[off : off+8])) // #nosec G115 -- safe conversion
	return v, off + 8, nil
}

// --- AccessRequest ---

func encodeAccessRequest(a *AccessRequest) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.RequesterID))
	buf = appendBytes(buf, []byte(a.TargetDomain))
	buf = appendBytes(buf, []byte(a.Justification))
	buf = append(buf, a.RequestedLevel)
	return buf
}

func decodeAccessRequest(data []byte) (*AccessRequest, error) {
	a := &AccessRequest{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.RequesterID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.TargetDomain = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Justification = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	a.RequestedLevel = data[off]

	return a, nil
}

// --- AccessGrant ---

func encodeAccessGrant(a *AccessGrant) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.GranterID))
	buf = appendBytes(buf, []byte(a.GranteeID))
	buf = appendBytes(buf, []byte(a.Domain))
	buf = append(buf, a.Level)
	buf = appendInt64(buf, a.ExpiresAt)
	buf = appendBytes(buf, []byte(a.RequestID))
	return buf
}

func decodeAccessGrant(data []byte) (*AccessGrant, error) {
	a := &AccessGrant{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.GranterID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.GranteeID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Domain = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	a.Level = data[off]
	off++

	a.ExpiresAt, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.RequestID = string(b)

	return a, nil
}

// --- AccessRevoke ---

func encodeAccessRevoke(a *AccessRevoke) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.RevokerID))
	buf = appendBytes(buf, []byte(a.GranteeID))
	buf = appendBytes(buf, []byte(a.Domain))
	buf = appendBytes(buf, []byte(a.Reason))
	return buf
}

func decodeAccessRevoke(data []byte) (*AccessRevoke, error) {
	a := &AccessRevoke{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.RevokerID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.GranteeID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Domain = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Reason = string(b)

	return a, nil
}

// --- AccessQuery ---

func encodeAccessQuery(a *AccessQuery) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.AgentID))
	buf = appendBytes(buf, []byte(a.Domain))
	// Encode embedding: uint32 length, then each float32
	buf = appendUint32(buf, uint32(len(a.Embedding))) // #nosec G115 -- embedding length fits in uint32
	for _, v := range a.Embedding {
		buf = appendFloat32(buf, v)
	}
	buf = appendUint32(buf, uint32(a.TopK)) // #nosec G115 -- topK is a small query limit value
	return buf
}

func decodeAccessQuery(data []byte) (*AccessQuery, error) {
	a := &AccessQuery{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.AgentID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Domain = string(b)

	// Decode embedding
	var embLen uint32
	embLen, off, err = readUint32(data, off)
	if err != nil {
		return nil, err
	}
	a.Embedding = make([]float32, embLen)
	for i := uint32(0); i < embLen; i++ {
		a.Embedding[i], off, err = readFloat32(data, off)
		if err != nil {
			return nil, err
		}
	}

	var topK uint32
	topK, _, err = readUint32(data, off)
	if err != nil {
		return nil, err
	}
	a.TopK = int32(topK) // #nosec G115 -- topK is a small query limit value

	return a, nil
}

// --- DomainRegister ---

func encodeDomainRegister(d *DomainRegister) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(d.DomainName))
	buf = appendBytes(buf, []byte(d.OwnerAgentID))
	buf = appendBytes(buf, []byte(d.Description))
	buf = appendBytes(buf, []byte(d.ParentDomain))
	return buf
}

func decodeDomainRegister(data []byte) (*DomainRegister, error) {
	d := &DomainRegister{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.DomainName = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.OwnerAgentID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.Description = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.ParentDomain = string(b)

	return d, nil
}

// --- bool/string-slice helpers ---

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func byteToBool(b byte) bool {
	return b != 0
}

func appendStringSlice(buf []byte, ss []string) []byte {
	buf = appendUint32(buf, uint32(len(ss))) // #nosec G115 -- slice length fits in uint32
	for _, s := range ss {
		buf = appendBytes(buf, []byte(s))
	}
	return buf
}

func readStringSlice(data []byte, off int) ([]string, int, error) {
	if off+4 > len(data) {
		return nil, 0, ErrInvalidTxData
	}
	count := int(binary.BigEndian.Uint32(data[off : off+4])) // #nosec G115 -- uint32 fits in int
	off += 4
	ss := make([]string, count)
	for i := 0; i < count; i++ {
		b, newOff, err := readBytes(data, off)
		if err != nil {
			return nil, 0, err
		}
		ss[i] = string(b)
		off = newOff
	}
	return ss, off, nil
}

// --- OrgRegister ---

func encodeOrgRegister(o *OrgRegister) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(o.OrgID))
	buf = appendBytes(buf, []byte(o.Name))
	buf = appendBytes(buf, []byte(o.Description))
	buf = appendBytes(buf, []byte(o.AdminAgent))
	return buf
}

func decodeOrgRegister(data []byte) (*OrgRegister, error) {
	o := &OrgRegister{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.Name = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.Description = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.AdminAgent = string(b)

	return o, nil
}

// --- OrgAddMember ---

func encodeOrgAddMember(o *OrgAddMember) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(o.OrgID))
	buf = appendBytes(buf, []byte(o.AgentID))
	buf = append(buf, byte(o.Clearance))
	buf = appendBytes(buf, []byte(o.Role))
	return buf
}

func decodeOrgAddMember(data []byte) (*OrgAddMember, error) {
	o := &OrgAddMember{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.AgentID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	o.Clearance = ClearanceLevel(data[off])
	off++

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.Role = string(b)

	return o, nil
}

// --- OrgRemoveMember ---

func encodeOrgRemoveMember(o *OrgRemoveMember) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(o.OrgID))
	buf = appendBytes(buf, []byte(o.AgentID))
	buf = appendBytes(buf, []byte(o.Reason))
	return buf
}

func decodeOrgRemoveMember(data []byte) (*OrgRemoveMember, error) {
	o := &OrgRemoveMember{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.AgentID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.Reason = string(b)

	return o, nil
}

// --- OrgSetClearance ---

func encodeOrgSetClearance(o *OrgSetClearance) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(o.OrgID))
	buf = appendBytes(buf, []byte(o.AgentID))
	buf = append(buf, byte(o.Clearance))
	return buf
}

func decodeOrgSetClearance(data []byte) (*OrgSetClearance, error) {
	o := &OrgSetClearance{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	o.AgentID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	o.Clearance = ClearanceLevel(data[off])

	return o, nil
}

// --- FederationPropose ---

func encodeFederationPropose(f *FederationPropose) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(f.ProposerOrgID))
	buf = appendBytes(buf, []byte(f.TargetOrgID))
	buf = appendStringSlice(buf, f.AllowedDomains)
	buf = append(buf, byte(f.MaxClearance))
	buf = appendInt64(buf, f.ExpiresAt)
	buf = append(buf, boolToByte(f.RequiresApproval))
	buf = appendStringSlice(buf, f.AllowedDepts)
	return buf
}

func decodeFederationPropose(data []byte) (*FederationPropose, error) {
	f := &FederationPropose{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.ProposerOrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.TargetOrgID = string(b)

	f.AllowedDomains, off, err = readStringSlice(data, off)
	if err != nil {
		return nil, err
	}

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	f.MaxClearance = ClearanceLevel(data[off])
	off++

	f.ExpiresAt, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	f.RequiresApproval = byteToBool(data[off])
	off++

	// AllowedDepts: backward compatible — empty if absent
	if off < len(data) {
		f.AllowedDepts, _, err = readStringSlice(data, off)
		if err != nil {
			return nil, err
		}
	}

	return f, nil
}

// --- FederationApprove ---

func encodeFederationApprove(f *FederationApprove) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(f.FederationID))
	buf = appendBytes(buf, []byte(f.ApproverOrgID))
	return buf
}

func decodeFederationApprove(data []byte) (*FederationApprove, error) {
	f := &FederationApprove{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.FederationID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.ApproverOrgID = string(b)

	return f, nil
}

// --- FederationRevoke ---

func encodeFederationRevoke(f *FederationRevoke) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(f.FederationID))
	buf = appendBytes(buf, []byte(f.RevokerOrgID))
	buf = appendBytes(buf, []byte(f.Reason))
	return buf
}

func decodeFederationRevoke(data []byte) (*FederationRevoke, error) {
	f := &FederationRevoke{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.FederationID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.RevokerOrgID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	f.Reason = string(b)

	return f, nil
}

// --- DeptRegister ---

func encodeDeptRegister(d *DeptRegister) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(d.OrgID))
	buf = appendBytes(buf, []byte(d.DeptID))
	buf = appendBytes(buf, []byte(d.DeptName))
	buf = appendBytes(buf, []byte(d.Description))
	buf = appendBytes(buf, []byte(d.ParentDept))
	return buf
}

func decodeDeptRegister(data []byte) (*DeptRegister, error) {
	d := &DeptRegister{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.DeptID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.DeptName = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.Description = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.ParentDept = string(b)

	return d, nil
}

// --- DeptAddMember ---

func encodeDeptAddMember(d *DeptAddMember) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(d.OrgID))
	buf = appendBytes(buf, []byte(d.DeptID))
	buf = appendBytes(buf, []byte(d.AgentID))
	buf = append(buf, byte(d.Clearance))
	buf = appendBytes(buf, []byte(d.Role))
	return buf
}

func decodeDeptAddMember(data []byte) (*DeptAddMember, error) {
	d := &DeptAddMember{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.DeptID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.AgentID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	d.Clearance = ClearanceLevel(data[off])
	off++

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.Role = string(b)

	return d, nil
}

// --- DeptRemoveMember ---

func encodeDeptRemoveMember(d *DeptRemoveMember) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(d.OrgID))
	buf = appendBytes(buf, []byte(d.DeptID))
	buf = appendBytes(buf, []byte(d.AgentID))
	buf = appendBytes(buf, []byte(d.Reason))
	return buf
}

func decodeDeptRemoveMember(data []byte) (*DeptRemoveMember, error) {
	d := &DeptRemoveMember{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.OrgID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.DeptID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.AgentID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	d.Reason = string(b)

	return d, nil
}

// --- AgentRegister ---

func encodeAgentRegister(a *AgentRegister) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.AgentID))
	buf = appendBytes(buf, []byte(a.Name))
	buf = appendBytes(buf, []byte(a.Role))
	buf = appendBytes(buf, []byte(a.BootBio))
	buf = appendBytes(buf, []byte(a.Provider))
	buf = appendBytes(buf, []byte(a.P2PAddress))
	return buf
}

func decodeAgentRegister(data []byte) (*AgentRegister, error) {
	a := &AgentRegister{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.AgentID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Name = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Role = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.BootBio = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Provider = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.P2PAddress = string(b)

	return a, nil
}

// --- AgentUpdate ---

func encodeAgentUpdate(a *AgentUpdate) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.AgentID))
	buf = appendBytes(buf, []byte(a.Name))
	buf = appendBytes(buf, []byte(a.BootBio))
	return buf
}

func decodeAgentUpdate(data []byte) (*AgentUpdate, error) {
	a := &AgentUpdate{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.AgentID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.Name = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.BootBio = string(b)

	return a, nil
}

// --- AgentSetPermission ---

func encodeAgentSetPermission(a *AgentSetPermission) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(a.AgentID))
	buf = append(buf, a.Clearance)
	buf = appendBytes(buf, []byte(a.DomainAccess))
	buf = appendBytes(buf, []byte(a.VisibleAgents))
	buf = appendBytes(buf, []byte(a.OrgID))
	buf = appendBytes(buf, []byte(a.DeptID))
	return buf
}

func decodeAgentSetPermission(data []byte) (*AgentSetPermission, error) {
	a := &AgentSetPermission{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.AgentID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	a.Clearance = data[off]
	off++

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.DomainAccess = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.VisibleAgents = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.OrgID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	a.DeptID = string(b)

	return a, nil
}

// --- MemoryReassign ---

func encodeMemoryReassign(m *MemoryReassign) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(m.SourceAgentID))
	buf = appendBytes(buf, []byte(m.TargetAgentID))
	return buf
}

func decodeMemoryReassign(data []byte) (*MemoryReassign, error) {
	m := &MemoryReassign{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	m.SourceAgentID = string(b)

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	m.TargetAgentID = string(b)

	return m, nil
}

// --- GovPropose ---

func encodeGovPropose(g *GovPropose) []byte {
	var buf []byte
	buf = append(buf, byte(g.Operation))
	buf = appendBytes(buf, []byte(g.TargetID))
	buf = appendBytes(buf, g.TargetPubKey)
	buf = appendInt64(buf, g.TargetPower)
	buf = appendInt64(buf, g.ExpiryBlocks)
	buf = appendBytes(buf, []byte(g.Reason))
	return buf
}

func decodeGovPropose(data []byte) (*GovPropose, error) {
	g := &GovPropose{}
	var err error
	var b []byte
	off := 0

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	g.Operation = GovProposalOp(data[off])
	off++

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	g.TargetID = string(b)

	g.TargetPubKey, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}

	g.TargetPower, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	g.ExpiryBlocks, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	g.Reason = string(b)

	return g, nil
}

// --- GovVote ---

func encodeGovVote(g *GovVote) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(g.ProposalID))
	buf = append(buf, byte(g.Decision))
	return buf
}

func decodeGovVote(data []byte) (*GovVote, error) {
	g := &GovVote{}
	var err error
	var b []byte
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	g.ProposalID = string(b)

	if off >= len(data) {
		return nil, ErrInvalidTxData
	}
	g.Decision = VoteDecision(data[off])

	return g, nil
}

// --- GovCancel ---

func encodeGovCancel(g *GovCancel) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(g.ProposalID))
	return buf
}

func decodeGovCancel(data []byte) (*GovCancel, error) {
	g := &GovCancel{}
	var err error
	var b []byte
	off := 0

	b, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	g.ProposalID = string(b)

	return g, nil
}
