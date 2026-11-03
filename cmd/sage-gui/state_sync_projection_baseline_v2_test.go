package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func rawProjectionBaselineForTest(t *testing.T) *stateSyncProjectionBaseline {
	t.Helper()
	ids := []string{" ", "\x00", string([]byte{0xff, 0xfe}), strings.Repeat("historical", 80)}
	sort.Strings(ids)
	hash := sha256.Sum256([]byte("raw-projection-snapshot"))
	baseline, err := newStateSyncProjectionBaselineV2Pending("raw-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], 31, hash[:], ids)
	require.NoError(t, err)
	return baseline
}

func TestStateSyncProjectionBaselineV2PreservesRawBytes(t *testing.T) {
	baseline := rawProjectionBaselineForTest(t)
	encoded, err := encodeStateSyncProjectionBaselineDocument(baseline)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "allowed_missing_ids")
	require.Contains(t, string(encoded), `"allowed_missing_id_bytes"`)
	require.NotContains(t, string(encoded), "\\ufffd")
	decoded, err := decodeStateSyncProjectionBaselineDocument(encoded)
	require.NoError(t, err)
	require.Equal(t, baseline.AllowedMissingIDs, decoded.AllowedMissingIDs)
	require.NoError(t, verifyStateSyncProjectionBaselineSignature(decoded, stateSyncProjectionTestNodeKey))
	for _, id := range baseline.AllowedMissingIDs {
		_, ok := baselineAllowedMissingIDs(decoded)[id]
		require.True(t, ok)
	}
	reencoded, err := encodeStateSyncProjectionBaselineDocument(decoded)
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
	decoded.AllowedMissingIDs[0] = "changed"
	require.Error(t, verifyStateSyncProjectionBaselineSignature(decoded, stateSyncProjectionTestNodeKey))
	_, err = encodeStateSyncProjectionBaselineDocument(decoded)
	require.Error(t, err)
}

func TestStateSyncProjectionBaselineV2RejectsAmbiguousEncoding(t *testing.T) {
	for _, encoded := range [][]string{{"YQ"}, {"YQ==\n"}, {"YR=="}, {"_w=="}, {""}, {"Yg==", "YQ=="}, {"YQ==", "YQ=="}} {
		_, err := decodeStateSyncProjectionRawIDs(encoded)
		require.Error(t, err, "%q", encoded)
	}
	raw := []string{"\x00", string([]byte{0xff}), strings.Repeat("x", 513)}
	sort.Strings(raw)
	encoded, err := encodeStateSyncProjectionRawIDs(raw)
	require.NoError(t, err)
	decoded, err := decodeStateSyncProjectionRawIDs(encoded)
	require.NoError(t, err)
	require.Equal(t, raw, decoded)
	baseline := rawProjectionBaselineForTest(t)
	document, err := encodeStateSyncProjectionBaselineDocument(baseline)
	require.NoError(t, err)
	duplicate := bytes.Replace(document, []byte(`"version": 2,`), []byte(`"version": 2, "version": 2,`), 1)
	_, err = decodeStateSyncProjectionBaselineDocument(duplicate)
	require.Error(t, err)
	missing := bytes.Replace(document, []byte(`"id_encoding": "base64-std-padded",`), nil, 1)
	_, err = decodeStateSyncProjectionBaselineDocument(missing)
	require.Error(t, err)
	_, err = decodeStateSyncProjectionBaselineDocument(append(document, []byte("{}")...))
	require.Error(t, err)
}

func TestStateSyncProjectionBaselineV1SigningBytesRemainStable(t *testing.T) {
	hash := sha256.Sum256([]byte("legacy-snapshot"))
	baseline, err := newStateSyncProjectionBaselinePending("legacy-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], 9, hash[:], []string{"legacy-a", "legacy-b"})
	require.NoError(t, err)
	// Reconstruct the historical payload explicitly, independent of the new
	// V2 facade/writer path. No new field is admitted to the V1 signed bytes.
	wantPayload := `{"version":1,"chain_id":"legacy-chain","node_id":"` + baseline.NodeID + `","node_public_key":"` + baseline.NodePublicKey + `","validator_public_key":"` + baseline.ValidatorPublicKey + `","sealed_height":9,"sealed_app_hash":"` + baseline.SealedAppHash + `","allowed_missing_ids":["legacy-a","legacy-b"]}`
	payload, err := json.Marshal(baseline.payload())
	require.NoError(t, err)
	require.Equal(t, []byte(wantPayload), payload)
	wantDigest := sha256.Sum256([]byte(wantPayload))
	require.Equal(t, hex.EncodeToString(wantDigest[:]), baseline.Digest)
	message, err := stateSyncProjectionBaselineSignatureMessage(baseline.Digest)
	require.NoError(t, err)
	require.Equal(t, append([]byte("SAGE-STATE-SYNC-PROJECTION-BASELINE-V1\x00"), wantDigest[:]...), message)
	legacyDocument, err := json.MarshalIndent(baseline, "", "  ")
	require.NoError(t, err)
	legacyDocument = append(legacyDocument, '\n')
	document, err := encodeStateSyncProjectionBaselineDocument(baseline)
	require.NoError(t, err)
	require.Equal(t, legacyDocument, document)
	decoded, err := decodeStateSyncProjectionBaselineDocument(document)
	require.NoError(t, err)
	require.NoError(t, verifyStateSyncProjectionBaselineSignature(decoded, stateSyncProjectionTestNodeKey))
	wrongDomain, err := stateSyncProjectionBaselineV2SignatureMessage(baseline.Digest)
	require.NoError(t, err)
	signature, err := hex.DecodeString(baseline.Signature)
	require.NoError(t, err)
	require.False(t, stateSyncProjectionTestNodeKey.PubKey().VerifySignature(wrongDomain, signature))
}

func TestStateSyncProjectionBaselineV2SignatureBindsEncodingAndIDs(t *testing.T) {
	baseline := rawProjectionBaselineForTest(t)
	raw, err := baseline.verifiedRawV2()
	require.NoError(t, err)
	changed := *raw
	changed.AllowedMissingIDBytes = append([]string(nil), raw.AllowedMissingIDBytes...)
	changed.AllowedMissingIDBytes[0] = base64.StdEncoding.EncodeToString([]byte{1})
	changed.Digest, err = stateSyncProjectionBaselineV2Digest(changed.stateSyncProjectionBaselineV2Payload)
	require.NoError(t, err)
	_, err = changed.validateShape()
	require.NoError(t, err)
	// Even a well-formed replacement with a recomputed digest cannot reuse
	// the signed original's authorization.
	require.Error(t, changed.verify(stateSyncProjectionTestNodeKey))
	changed.IDEncoding = "base64-raw"
	require.Error(t, changed.verify(stateSyncProjectionTestNodeKey))
}

func TestStateSyncProjectionBaselineSelectsVersionFromSnapshot(t *testing.T) {
	for _, height := range []uint64{10, 11} {
		t.Run(map[uint64]string{10: "activation-block-V1", 11: "post-activation-V2"}[height], func(t *testing.T) {
			dataDir := t.TempDir()
			preparedPath := filepath.Join(dataDir, "prepared")
			canonical, err := store.NewBadgerStore(preparedPath)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					require.NoError(t, canonical.CloseBadger())
				}
			})
			require.NoError(t, canonical.MarkUpgradeApplied("app-v30", 30, 10))
			persisted := make([]byte, 8)
			binary.BigEndian.PutUint64(persisted, height)
			require.NoError(t, canonical.SetState("height", persisted))
			hash := sha256.Sum256([]byte("qualified-projection-snapshot"))
			require.NoError(t, canonical.SetState("app_hash", hash[:]))
			ids := []string{"ordinary-history"}
			if height == 11 {
				ids = []string{"\x00", " ", strings.Repeat("x", 513), string([]byte{0xff})}
			}
			for _, id := range ids {
				publishBaselineCanonicalMemory(t, canonical, id)
			}
			publishBaselineScopedMemory(t, canonical, "scoped-recoverable")
			require.NoError(t, canonical.CloseBadger())
			closed = true
			require.NoError(t, captureStateSyncProjectionBaselinePending(preparedPath, dataDir, "qualified-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], height, hash[:]))
			pending, err := loadStateSyncProjectionBaseline(stateSyncProjectionBaselinePendingPath(dataDir))
			require.NoError(t, err)
			require.Equal(t, ids, pending.AllowedMissingIDs)
			wantVersion := stateSyncProjectionBaselineVersion
			if height == 11 {
				wantVersion = stateSyncProjectionBaselineRawVersion
			}
			require.Equal(t, wantVersion, pending.Version)
			if height == 11 {
				require.Error(t, captureStateSyncProjectionBaselinePending(preparedPath, t.TempDir(), "qualified-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], 12, hash[:]))
			}
			reopened, err := store.OpenBadgerStoreWithoutMigrations(preparedPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.CloseBadger()) })
			require.NoError(t, validateStateSyncProjectionBaseline(pending, "qualified-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], int64(height), hash[:], reopened))
			if height == 11 {
				// A valid local signature cannot claim raw rules for the activation
				// block; qualification is independently checked from canonical state.
				wrong, err := newStateSyncProjectionBaselineV2Pending("qualified-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], 10, hash[:], ids)
				require.NoError(t, err)
				require.Error(t, validateStateSyncProjectionBaseline(wrong, "qualified-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], 11, hash[:], reopened))
			}
		})
	}
}

func TestStateSyncProjectionBaselineV1CaptureRequiresExactPoint(t *testing.T) {
	appHash := sha256.Sum256([]byte("legacy-qualified-snapshot"))
	for _, tc := range []struct {
		name          string
		heightPresent bool
		hashPresent   bool
		height        uint64
		sealedHeight  uint64
		mismatchHash  bool
		wantSuccess   bool
	}{
		{name: "missing_height", hashPresent: true, sealedHeight: 9},
		{name: "missing_app_hash", heightPresent: true, height: 9, sealedHeight: 9},
		{name: "height_mismatch", heightPresent: true, hashPresent: true, height: 9, sealedHeight: 10},
		{name: "hash_mismatch", heightPresent: true, hashPresent: true, height: 9, sealedHeight: 9, mismatchHash: true},
		{name: "exact_point", heightPresent: true, hashPresent: true, height: 9, sealedHeight: 9, wantSuccess: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			preparedPath := filepath.Join(dataDir, "prepared")
			canonical, err := store.NewBadgerStore(preparedPath)
			require.NoError(t, err)
			closed := false
			t.Cleanup(func() {
				if !closed {
					require.NoError(t, canonical.CloseBadger())
				}
			})
			publishBaselineCanonicalMemory(t, canonical, "historical-before-v30")
			if tc.heightPresent {
				persisted := make([]byte, 8)
				binary.BigEndian.PutUint64(persisted, tc.height)
				require.NoError(t, canonical.SetState("height", persisted))
			}
			if tc.hashPresent {
				storedHash := append([]byte(nil), appHash[:]...)
				if tc.mismatchHash {
					storedHash[0] ^= 1
				}
				require.NoError(t, canonical.SetState("app_hash", storedHash))
			}
			require.NoError(t, canonical.CloseBadger())
			closed = true
			err = captureStateSyncProjectionBaselinePending(preparedPath, dataDir, "legacy-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], tc.sealedHeight, appHash[:])
			if !tc.wantSuccess {
				require.ErrorContains(t, err, "does not match its authenticated finalized snapshot")
				_, statErr := os.Stat(stateSyncProjectionBaselinePendingPath(dataDir))
				require.ErrorIs(t, statErr, os.ErrNotExist, "mismatched point must not mint an omission authorization")
				return
			}
			require.NoError(t, err)
			pending, err := loadStateSyncProjectionBaseline(stateSyncProjectionBaselinePendingPath(dataDir))
			require.NoError(t, err)
			require.Equal(t, stateSyncProjectionBaselineVersion, pending.Version)
			require.Equal(t, []string{"historical-before-v30"}, pending.AllowedMissingIDs)
			require.NoError(t, validateStateSyncProjectionBaselineIdentity(pending, "legacy-chain", stateSyncProjectionTestNodeKey, stateSyncProjectionTestValidatorKey[:], int64(tc.sealedHeight), appHash[:])) // #nosec G115 -- table heights are bounded literals
		})
	}
}
