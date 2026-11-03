package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"

	"github.com/cometbft/cometbft/p2p"
)

const (
	stateSyncProjectionBaselineRawVersion = 2
	stateSyncProjectionRawIdentityRules   = "app-v30-raw"
	stateSyncProjectionRawIDEncoding      = "base64-std-padded"
)

// V2 is a distinct wire/signature payload. Never add its fields to the V1
// payload or marshal decoded arbitrary-byte IDs through the V1 JSON fields.
type stateSyncProjectionBaselineV2Payload struct {
	Version               int      `json:"version"`
	ChainID               string   `json:"chain_id"`
	NodeID                string   `json:"node_id"`
	NodePublicKey         string   `json:"node_public_key"`
	ValidatorPublicKey    string   `json:"validator_public_key"`
	SealedHeight          int64    `json:"sealed_height"`
	SealedAppHash         string   `json:"sealed_app_hash"`
	IdentityRules         string   `json:"identity_rules"`
	IDEncoding            string   `json:"id_encoding"`
	AllowedMissingIDBytes []string `json:"allowed_missing_id_bytes"`
}

type stateSyncProjectionBaselineV2 struct {
	stateSyncProjectionBaselineV2Payload
	Digest    string `json:"digest"`
	Signature string `json:"signature"`
}

func encodeStateSyncProjectionRawIDs(ids []string) ([]string, error) {
	if len(ids) > maxStateSyncProjectionBaselineIDs {
		return nil, errors.New("state-sync projection baseline contains too many memory IDs")
	}
	encoded := make([]string, 0, len(ids))
	total := 0
	encodedTotal := 0
	previous := ""
	for _, id := range ids {
		// Go string ordering compares exact bytes, including invalid UTF-8.
		if len(id) == 0 || (previous != "" && previous >= id) {
			return nil, errors.New("state-sync raw IDs must be nonempty and strictly byte-sorted")
		}
		previous = id
		// Admission's512-byte bound is intentionally absent: historical IDs
		// remain recoverable under the preexisting total/count resource caps.
		if len(id) > maxStateSyncProjectionBaselineBytes || total > maxStateSyncProjectionBaselineBytes-len(id) {
			return nil, errors.New("state-sync raw IDs exceed the byte budget")
		}
		total += len(id)
		encodedSize := base64.StdEncoding.EncodedLen(len(id))
		if encodedSize > maxStateSyncProjectionBaselineBytes || encodedTotal > maxStateSyncProjectionBaselineBytes-encodedSize {
			return nil, errors.New("state-sync encoded IDs exceed the byte budget")
		}
		encodedTotal += encodedSize
		value := base64.StdEncoding.EncodeToString([]byte(id))
		encoded = append(encoded, value)
	}
	return encoded, nil
}

func decodeStateSyncProjectionRawIDs(encoded []string) ([]string, error) {
	if len(encoded) > maxStateSyncProjectionBaselineIDs {
		return nil, errors.New("state-sync projection baseline contains too many memory IDs")
	}
	ids := make([]string, 0, len(encoded))
	total := 0
	for _, value := range encoded {
		if len(value) == 0 || len(value) > maxStateSyncProjectionBaselineBytes || total > maxStateSyncProjectionBaselineBytes-len(value) {
			return nil, errors.New("state-sync encoded IDs exceed the byte budget")
		}
		total += len(value)
		raw, err := base64.StdEncoding.Strict().DecodeString(value)
		if err != nil || len(raw) == 0 || base64.StdEncoding.EncodeToString(raw) != value {
			return nil, errors.New("state-sync memory ID is not canonical padded standard-base64")
		}
		if len(ids) > 0 && bytes.Compare([]byte(ids[len(ids)-1]), raw) >= 0 {
			return nil, errors.New("state-sync decoded memory IDs are not strictly byte-sorted")
		}
		ids = append(ids, string(raw))
	}
	return ids, nil
}

func stateSyncProjectionBaselineV2Digest(payload stateSyncProjectionBaselineV2Payload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	if len(encoded) > maxStateSyncProjectionBaselineBytes {
		return "", errors.New("state-sync V2 payload exceeds the byte budget")
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stateSyncProjectionBaselineV2SignatureMessage(digest string) ([]byte, error) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return nil, errors.New("state-sync V2 digest must be canonical lowercase SHA-256")
	}
	return append([]byte("SAGE-STATE-SYNC-PROJECTION-BASELINE-V2\x00"), decoded...), nil
}

func (v2 *stateSyncProjectionBaselineV2) validateShape() ([]string, error) {
	if v2 == nil || v2.Version != stateSyncProjectionBaselineRawVersion || v2.IdentityRules != stateSyncProjectionRawIdentityRules || v2.IDEncoding != stateSyncProjectionRawIDEncoding {
		return nil, errors.New("unsupported state-sync V2 identity encoding")
	}
	ids, err := decodeStateSyncProjectionRawIDs(v2.AllowedMissingIDBytes)
	if err != nil {
		return nil, err
	}
	want, err := stateSyncProjectionBaselineV2Digest(v2.stateSyncProjectionBaselineV2Payload)
	if err != nil {
		return nil, err
	}
	if v2.Digest != want {
		return nil, errors.New("state-sync V2 digest mismatch")
	}
	return ids, nil
}

func (v2 *stateSyncProjectionBaselineV2) sign(nodeKey *p2p.NodeKey) error {
	if nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("state-sync V2 signer is missing")
	}
	if _, err := v2.validateShape(); err != nil {
		return err
	}
	message, err := stateSyncProjectionBaselineV2SignatureMessage(v2.Digest)
	if err != nil {
		return err
	}
	sig, err := nodeKey.PrivKey.Sign(message)
	if err != nil {
		return err
	}
	v2.Signature = hex.EncodeToString(sig)
	return nil
}

func (v2 *stateSyncProjectionBaselineV2) verify(nodeKey *p2p.NodeKey) error {
	if nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("state-sync V2 verifier is missing")
	}
	if _, err := v2.validateShape(); err != nil {
		return err
	}
	message, err := stateSyncProjectionBaselineV2SignatureMessage(v2.Digest)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(v2.Signature)
	if err != nil || len(sig) == 0 || hex.EncodeToString(sig) != v2.Signature || !nodeKey.PubKey().VerifySignature(message, sig) {
		return errors.New("state-sync V2 signature verification failed")
	}
	return nil
}

// The legacy in-memory facade keeps exact decoded Go-string bytes for existing
// set lookups. Its V2 pointer is unexported/json-ignored; writers must marshal
// this separate V2 wire object, never the facade's AllowedMissingIDs strings.
func (v2 *stateSyncProjectionBaselineV2) legacyFacade() (*stateSyncProjectionBaseline, error) {
	ids, err := v2.validateShape()
	if err != nil {
		return nil, err
	}
	return &stateSyncProjectionBaseline{Version: v2.Version, ChainID: v2.ChainID, NodeID: v2.NodeID, NodePublicKey: v2.NodePublicKey, ValidatorPublicKey: v2.ValidatorPublicKey, SealedHeight: v2.SealedHeight, SealedAppHash: v2.SealedAppHash, AllowedMissingIDs: ids, Digest: v2.Digest, Signature: v2.Signature, rawV2: v2}, nil
}

func (b *stateSyncProjectionBaseline) verifiedRawV2() (*stateSyncProjectionBaselineV2, error) {
	if b == nil || b.Version != stateSyncProjectionBaselineRawVersion || b.rawV2 == nil {
		return nil, errors.New("state-sync V2 wire payload is missing")
	}
	facade, err := b.rawV2.legacyFacade()
	if err != nil {
		return nil, err
	}
	if b.Version != facade.Version || b.ChainID != facade.ChainID || b.NodeID != facade.NodeID || b.NodePublicKey != facade.NodePublicKey || b.ValidatorPublicKey != facade.ValidatorPublicKey || b.SealedHeight != facade.SealedHeight || b.SealedAppHash != facade.SealedAppHash || b.Digest != facade.Digest || b.Signature != facade.Signature || !reflect.DeepEqual(b.AllowedMissingIDs, facade.AllowedMissingIDs) {
		return nil, errors.New("state-sync V2 decoded facade differs from its signed payload")
	}
	return b.rawV2, nil
}

func newStateSyncProjectionBaselineV2Pending(chainID string, nodeKey *p2p.NodeKey, validatorPublicKey []byte, sealedHeight uint64, sealedAppHash []byte, ids []string) (*stateSyncProjectionBaseline, error) {
	if sealedHeight == 0 || sealedHeight > math.MaxInt64 || len(sealedAppHash) != sha256.Size {
		return nil, errors.New("state-sync V2 authorization point is invalid")
	}
	if nodeKey == nil || nodeKey.PrivKey == nil || len(validatorPublicKey) != 32 {
		return nil, errors.New("state-sync V2 local identity is missing")
	}
	encoded, err := encodeStateSyncProjectionRawIDs(ids)
	if err != nil {
		return nil, err
	}
	v2 := &stateSyncProjectionBaselineV2{stateSyncProjectionBaselineV2Payload: stateSyncProjectionBaselineV2Payload{Version: stateSyncProjectionBaselineRawVersion, ChainID: chainID, NodeID: string(nodeKey.ID()), NodePublicKey: hex.EncodeToString(nodeKey.PubKey().Bytes()), ValidatorPublicKey: hex.EncodeToString(validatorPublicKey), SealedHeight: int64(sealedHeight), SealedAppHash: hex.EncodeToString(sealedAppHash), IdentityRules: stateSyncProjectionRawIdentityRules, IDEncoding: stateSyncProjectionRawIDEncoding, AllowedMissingIDBytes: encoded}} // #nosec G115 -- bounded above
	v2.Digest, err = stateSyncProjectionBaselineV2Digest(v2.stateSyncProjectionBaselineV2Payload)
	if err != nil {
		return nil, err
	}
	if signErr := v2.sign(nodeKey); signErr != nil {
		return nil, signErr
	}
	baseline, err := v2.legacyFacade()
	if err != nil {
		return nil, err
	}
	if err := validateStateSyncProjectionBaselineIdentity(baseline, chainID, nodeKey, validatorPublicKey, int64(sealedHeight), sealedAppHash); err != nil {
		return nil, err
	} // #nosec G115 -- bounded above
	return baseline, nil
}

func decodeStateSyncProjectionBaselineDocument(encoded []byte) (*stateSyncProjectionBaseline, error) {
	if len(encoded) == 0 || len(encoded) > maxStateSyncProjectionBaselineBytes {
		return nil, errors.New("state-sync projection baseline has an invalid size")
	}
	var version struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(encoded, &version); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var baseline *stateSyncProjectionBaseline
	switch version.Version {
	case stateSyncProjectionBaselineVersion:
		// Preserve the exact V1 type/decoder and signed payload representation.
		var legacy stateSyncProjectionBaseline
		if err := decoder.Decode(&legacy); err != nil {
			return nil, err
		}
		baseline = &legacy
	case stateSyncProjectionBaselineRawVersion:
		if err := validateStateSyncV2ObjectKeys(encoded, []string{"version", "chain_id", "node_id", "node_public_key", "validator_public_key", "sealed_height", "sealed_app_hash", "identity_rules", "id_encoding", "allowed_missing_id_bytes", "digest", "signature"}); err != nil {
			return nil, err
		}
		var raw stateSyncProjectionBaselineV2
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		var err error
		baseline, err = raw.legacyFacade()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported state-sync projection baseline version %d", version.Version)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("state-sync projection baseline contains trailing data")
	}
	return baseline, nil
}

func encodeStateSyncProjectionBaselineDocument(baseline *stateSyncProjectionBaseline) ([]byte, error) {
	if baseline == nil {
		return nil, errors.New("state-sync projection baseline is missing")
	}
	var target any = baseline
	if baseline.Version == stateSyncProjectionBaselineRawVersion {
		v2, err := baseline.verifiedRawV2()
		if err != nil {
			return nil, err
		}
		target = v2
	} else if baseline.Version != stateSyncProjectionBaselineVersion {
		return nil, errors.New("unsupported state-sync projection baseline version")
	}
	encoded, err := json.MarshalIndent(target, "", "  ")
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxStateSyncProjectionBaselineBytes {
		return nil, errors.New("state-sync projection baseline exceeds the maximum size")
	}
	return encoded, nil
}

// V2 rejects duplicate/unknown/missing fields without changing V1 decoding.
func validateStateSyncV2ObjectKeys(encoded []byte, required []string) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return errors.New("state-sync V2 baseline must be an object")
	}
	allowed := make(map[string]bool, len(required))
	for _, name := range required {
		allowed[name] = false
	}
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return tokenErr
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("state-sync V2 field name is invalid")
		}
		seen, ok := allowed[name]
		if !ok || seen {
			return errors.New("state-sync V2 has an unknown or duplicate field")
		}
		allowed[name] = true
		var value json.RawMessage
		if valueErr := decoder.Decode(&value); valueErr != nil {
			return valueErr
		}
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return errors.New("state-sync V2 object is incomplete")
	}
	for _, seen := range allowed {
		if !seen {
			return errors.New("state-sync V2 has a missing field")
		}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("state-sync V2 has trailing data")
	}
	return nil
}
