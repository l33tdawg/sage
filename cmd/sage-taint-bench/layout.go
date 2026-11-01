package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

// Limits copied from the governance taint RFC (#460). All are provisional; the
// point of this command is to measure them.
const (
	maxActiveDecisions  = 32
	maxParents          = 16
	maxSigners          = 64
	maxServingWalk      = 256
	maxDepth            = 64
	txUnitBudget        = 1024
	maxNewPerExecution  = 256
	submitUnitBound     = 625
	maxBlockTxs         = 64
	maxExpandCleanupTxs = 4
	expansionCap        = 16384
	candidateBudget     = 8192
	memoryIDCap         = 512
	decisionIDLen       = 64
	credentialLen       = 64
	defaultUnitCeiling  = 65536
	defaultBlockCeiling = 4 << 20
	uuidIDLen           = 36
	memberValueLen      = 11
	queueIDPrefixLen    = 2
	queueDepthLen       = 2
	provenanceSignerLen = credentialLen + credentialLen + 8 + 1
)

const (
	statusExpanding byte = iota + 1
	statusComplete
	statusDepthExhausted
	statusCleaning
	statusClean
)

const (
	originTarget byte = iota + 1
	originExpand
	originAdmit
)

// The planned key layouts. Nothing below exists in the node yet.
func memProvKey(id string) []byte       { return []byte("memprov:v30:" + id) }
func migratedKey(id string) []byte      { return []byte("memprov:migrated:" + id) }
func lineagePrefix(child string) []byte { return identityPrefix("lineage:", child) }
func lineageKey(child, parent string) []byte {
	return append(lineagePrefix(child), parent...)
}
func lineageRefPrefix(parent string) []byte { return []byte("lineage-ref:" + parent + ":") }
func lineageRefKey(parent, child string) []byte {
	return []byte("lineage-ref:" + parent + ":" + child)
}
func memberPrefix(memory string) []byte { return identityPrefix("taintm:", memory) }
func memberKey(memory, decision string) []byte {
	return append(memberPrefix(memory), decision...)
}

func identityPrefix(namespace, id string) []byte {
	prefix := append(make([]byte, 0, len(namespace)+2+len(id)+1), namespace...)
	prefix = binary.BigEndian.AppendUint16(prefix, uint16(len(id))) // #nosec G115 -- fixture IDs are bounded at memoryIDCap
	prefix = append(prefix, id...)
	return append(prefix, ':')
}
func indexPrefix(decision string) []byte { return []byte("taintd:" + decision + ":") }
func indexKey(decision, memory string) []byte {
	return []byte("taintd:" + decision + ":" + memory)
}
func queuePrefix(decision string) []byte { return []byte("taintq:" + decision + ":") }
func queueKey(decision string, generation, seq uint64) []byte {
	key := make([]byte, 0, 7+len(decision)+1+8+1+8)
	key = append(key, "taintq:"...)
	key = append(key, decision...)
	key = append(key, ':')
	key = binary.BigEndian.AppendUint64(key, generation)
	key = append(key, ':')
	return binary.BigEndian.AppendUint64(key, seq)
}
func headerKey(decision string) []byte { return []byte("taintdec:" + decision) }

var (
	revisionKey = []byte("taintrev")
	activeKey   = []byte("taintactive")
)

// Keys the node already writes, used by the pre-app-v30 provenance fallback.
func memoryKey(id string) []byte          { return []byte("memory:" + id) }
func authorKey(id string) []byte          { return []byte("memauthor:" + id) }
func authorPrincipalKey(id string) []byte { return []byte("memauthorprincipal:" + id) }
func heightKey(id string) []byte          { return []byte("memheight:" + id) }
func coauthorsKey(id string) []byte       { return []byte("cocommit:coauthors:" + id) }
func rootMarkerKey(credential string) []byte {
	return []byte("appv23:root_credential:" + credential)
}

const adoptionReceiptPrefix = "appv25:legacy-adoption:"

func adoptionKey(id string) []byte { return []byte(adoptionReceiptPrefix + id) }

type decisionHeader struct {
	Status       byte
	Generation   uint64
	Head         uint64
	Tail         uint64
	Scanned      uint32
	Materialized uint32
	MaxDepth     uint32
	Parent       string
	ParentDepth  uint16
	LastEdge     string
	Digest       [32]byte
}

func (h decisionHeader) encode() []byte {
	out := make([]byte, 0, 80+len(h.Parent)+len(h.LastEdge))
	out = append(out, h.Status)
	out = binary.BigEndian.AppendUint64(out, h.Generation)
	out = binary.BigEndian.AppendUint64(out, h.Head)
	out = binary.BigEndian.AppendUint64(out, h.Tail)
	out = binary.BigEndian.AppendUint32(out, h.Scanned)
	out = binary.BigEndian.AppendUint32(out, h.Materialized)
	out = binary.BigEndian.AppendUint32(out, h.MaxDepth)
	out = binary.BigEndian.AppendUint16(out, h.ParentDepth)
	out = binary.BigEndian.AppendUint16(out, uint16(len(h.Parent)))
	out = append(out, h.Parent...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(h.LastEdge)))
	out = append(out, h.LastEdge...)
	return append(out, h.Digest[:]...)
}

func decodeHeader(raw []byte) (decisionHeader, error) {
	var h decisionHeader
	if len(raw) < 75 {
		return h, fmt.Errorf("short decision header: %d bytes", len(raw))
	}
	h.Status = raw[0]
	h.Generation = binary.BigEndian.Uint64(raw[1:])
	h.Head = binary.BigEndian.Uint64(raw[9:])
	h.Tail = binary.BigEndian.Uint64(raw[17:])
	h.Scanned = binary.BigEndian.Uint32(raw[25:])
	h.Materialized = binary.BigEndian.Uint32(raw[29:])
	h.MaxDepth = binary.BigEndian.Uint32(raw[33:])
	h.ParentDepth = binary.BigEndian.Uint16(raw[37:])
	parentLen := int(binary.BigEndian.Uint16(raw[39:]))
	h.Parent = string(raw[41 : 41+parentLen])
	rest := raw[41+parentLen:]
	edgeLen := int(binary.BigEndian.Uint16(rest))
	h.LastEdge = string(rest[2 : 2+edgeLen])
	copy(h.Digest[:], rest[2+edgeLen:])
	return h, nil
}

func encodeMember(generation uint64, depth uint16, origin byte) []byte {
	out := binary.BigEndian.AppendUint64(make([]byte, 0, memberValueLen), generation)
	out = binary.BigEndian.AppendUint16(out, depth)
	return append(out, origin)
}

func decodeMemberGeneration(raw []byte) (uint64, error) {
	if len(raw) != memberValueLen {
		return 0, fmt.Errorf("membership length mismatch: %d bytes", len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

func encodeQueueEntry(memory string, depth uint16) []byte {
	// The stand-in does not resolve canonical content hashes. Its deterministic
	// parent-ref surrogate nevertheless occupies the production codec's 64 bytes.
	parent := sha256.Sum256([]byte(memory))
	parentID := hex.EncodeToString(parent[:])
	out := binary.BigEndian.AppendUint16(make([]byte, 0, len(memory)+70), uint16(len(parentID)))
	out = append(out, parentID...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(memory)))
	out = append(out, memory...)
	return binary.BigEndian.AppendUint16(out, depth)
}

func decodeQueueEntry(raw []byte) (string, uint16, error) {
	if len(raw) < 70 || binary.BigEndian.Uint16(raw) != decisionIDLen {
		return "", 0, fmt.Errorf("short queue entry: %d bytes", len(raw))
	}
	idLen := int(binary.BigEndian.Uint16(raw[66:]))
	if len(raw) != idLen+70 {
		return "", 0, fmt.Errorf("queue entry length mismatch: %d", len(raw))
	}
	return string(raw[68 : 68+idLen]), binary.BigEndian.Uint16(raw[68+idLen:]), nil
}

// encodeProvenance is the planned memprov:v30 value: one fixed-size entry per
// signer of {credential, principal, root_generation, exact}.
func encodeProvenance(ns string, signers int) []byte {
	out := make([]byte, 0, 1+signers*provenanceSignerLen)
	out = append(out, byte(signers))
	for i := range signers {
		out = append(out, makeID(credentialLen, ns+"/credential", i)...)
		out = append(out, makeID(credentialLen, ns+"/principal", i)...)
		out = binary.BigEndian.AppendUint64(out, uint64(i))
		out = append(out, 1)
	}
	return out
}

func decodeProvenanceCredentials(raw []byte) ([]string, error) {
	if len(raw) < 1 || len(raw) != 1+int(raw[0])*provenanceSignerLen {
		return nil, fmt.Errorf("provenance length mismatch: %d bytes", len(raw))
	}
	credentials := make([]string, int(raw[0]))
	for i := range credentials {
		credentials[i] = string(raw[1+i*provenanceSignerLen : 1+i*provenanceSignerLen+credentialLen])
	}
	return credentials, nil
}

// makeID returns a deterministic hex ID of an exact length, so both
// architectures generate byte-identical state.
func makeID(length int, namespace string, n int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s/%d", namespace, n))
	encoded := hex.EncodeToString(sum[:])
	return strings.Repeat(encoded, length/len(encoded)+1)[:length]
}

// sizes holds maximum encoded sizes for the estimator.
type sizes struct {
	memberEntry int64
	indexEntry  int64
	memberKey   int64
	indexKey    int64
	queueEntry  int64
	queueKey    int64
	triple      int64
	header      int64
	revision    int64
}

func maximumSizes() sizes {
	id := strings.Repeat("x", memoryIDCap)
	decision := strings.Repeat("d", decisionIDLen)
	memberLen := int64(len(memberKey(id, decision)))
	indexLen := int64(len(indexKey(decision, id)))
	queueKeyLen := int64(len(queueKey(decision, 1, 1)))
	s := sizes{
		memberKey:   memberLen,
		indexKey:    indexLen,
		memberEntry: memberLen + memberValueLen,
		indexEntry:  indexLen,
		queueKey:    queueKeyLen,
		queueEntry:  queueKeyLen + int64(len(encodeQueueEntry(id, 0))),
		header:      int64(len(headerKey(decision))) + int64(len(decisionHeader{Parent: id, LastEdge: id}.encode())),
		revision:    int64(len(revisionKey)) + 8,
	}
	s.triple = s.memberEntry + s.indexEntry + s.queueEntry
	return s
}
