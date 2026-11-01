package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func testCredential(seed string) string {
	return testAppV30Signer(seed).CredentialID
}

func testCredentialScope(credentials ...string) TaintDecisionScope {
	return TaintDecisionScope{
		Kind:        tx.TaintScopeCredentialSet,
		Credentials: credentials,
	}
}

func TestTaintDecisionScopeRoundTripAndReplay(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("scope-round-trip-target")
	decisionID := testTaintDecisionID("scope-round-trip")
	payloadHash := testTaintPayloadHash(decisionID)
	decisionScope := testCredentialScope(testCredential("a"), testCredential("b"))

	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		payloadHash,
		decisionScope,
	)
	require.NoError(t, err)
	require.True(t, created)

	stored, found, err := s.GetTaintDecisionScope(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, decisionScope, stored)

	created, err = s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		payloadHash,
		decisionScope,
	)
	require.NoError(t, err)
	require.False(t, created)

	_, err = s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		payloadHash,
		testCredentialScope(testCredential("c")),
	)
	require.ErrorIs(t, err, ErrTaintConflict)

	encoded, err := encodeTaintDecisionScope(payloadHash, decisionScope)
	require.NoError(t, err)
	storedHash, decoded, err := decodeTaintDecisionScope(encoded)
	require.NoError(t, err)
	require.Equal(t, payloadHash, storedHash)
	require.Equal(t, decisionScope, decoded)

	encoded[0] = taintScopeStateVersion + 1
	_, _, err = decodeTaintDecisionScope(encoded)
	require.ErrorIs(t, err, ErrTaintStateMalformed)
}

func TestAuthorProvenanceExactAndFallbackRules(t *testing.T) {
	s := openTaintStore(t)
	credential := testCredential("a")
	principal := testCredential("b")
	rootPrincipal := testCredential("c")

	exact := testAppV30Signer("a")
	exact.PrincipalID = rootPrincipal
	exact.RootGeneration = 3
	require.NoError(t, s.SetAppV30Provenance("exact-memory", []AppV30ProvenanceSigner{exact}))
	provenance, err := s.AuthorProvenance("exact-memory")
	require.NoError(t, err)
	require.True(t, provenance.Exact)
	require.True(t, provenance.CoverageKnown)
	require.Equal(t, []AppV30ProvenanceSigner{exact}, provenance.Signers)

	require.NoError(t, s.SetMemoryAuthorPrincipal("legacy-memory", principal))
	require.NoError(t, s.SetMemoryAuthor("legacy-memory", credential))
	provenance, err = s.AuthorProvenance("legacy-memory")
	require.NoError(t, err)
	require.False(t, provenance.Exact)
	require.True(t, provenance.CoverageKnown)
	require.Equal(t, []AppV30ProvenanceSigner{{
		CredentialID: credential,
	}}, provenance.Signers)
	require.True(t, testCredentialScope(credential).Matches(provenance))
	require.False(t, (TaintDecisionScope{
		Kind:           tx.TaintScopeRootGeneration,
		PrincipalID:    rootPrincipal,
		GenerationFrom: 1,
		GenerationTo:   4,
	}).Matches(provenance))

	require.NoError(t, s.SetMemoryAuthorPrincipal("adopted-memory", principal))
	require.NoError(t, s.SetMemoryAuthor("adopted-memory", credential))
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(memoryLegacyAdoptionReceiptKey("adopted-memory"), make([]byte, sha256.Size))
	}))
	provenance, err = s.AuthorProvenance("adopted-memory")
	require.NoError(t, err)
	require.False(t, provenance.Exact)
	require.False(t, provenance.CoverageKnown)
	require.Empty(t, provenance.Signers)
	require.False(t, testCredentialScope(credential).Matches(provenance))

	require.NoError(t, s.SetMemoryAuthorPrincipal("migrated-memory", principal))
	require.NoError(t, s.SetMemoryAuthor("migrated-memory", credential))
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(appV30MigrationMarkerKey("migrated-memory"), []byte{1})
	}))
	provenance, err = s.AuthorProvenance("migrated-memory")
	require.NoError(t, err)
	require.False(t, provenance.Exact)
	require.False(t, provenance.CoverageKnown)

	require.NoError(t, s.SetAppV30Provenance("migrated-exact-memory", []AppV30ProvenanceSigner{exact}))
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(appV30MigrationMarkerKey("migrated-exact-memory"), []byte{1})
	}))
	provenance, err = s.AuthorProvenance("migrated-exact-memory")
	require.NoError(t, err)
	require.True(t, provenance.Exact)
	require.True(t, provenance.CoverageKnown)

	coauthorA, err := hex.DecodeString(testCredential("a"))
	require.NoError(t, err)
	coauthorB, err := hex.DecodeString(testCredential("b"))
	require.NoError(t, err)
	require.NoError(t, s.SetMemoryAuthorPrincipal("coauthor-memory", principal))
	require.NoError(t, s.SetCoCommitCoauthors("coauthor-memory", tx.EncodeCoauthorsCanonical(
		[]tx.CoCommitCoauthor{
			{PubKey: coauthorB, ChainID: "remote-b"},
			{PubKey: coauthorA, ChainID: "remote-a"},
		},
	)))
	provenance, err = s.AuthorProvenance("coauthor-memory")
	require.NoError(t, err)
	require.False(t, provenance.Exact)
	require.True(t, provenance.CoverageKnown)
	require.Equal(t, []AppV30ProvenanceSigner{
		{CredentialID: testCredential("a")},
		{CredentialID: testCredential("b")},
	}, provenance.Signers)
}

func TestClassifyMemoryTaintMatchesCredentialScope(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("credential-scope-target")
	credential := testCredential("d")
	decisionScope := testCredentialScope(credential)
	decisionID := testTaintDecisionID("credential-scope")
	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
		decisionScope,
	)
	require.NoError(t, err)
	require.True(t, created)
	_, err = s.ExpandTaintDecision("sage", decisionID, 64)
	require.NoError(t, err)
	complete, err := s.TaintServingCoverageComplete()
	require.NoError(t, err)
	require.False(t, complete, "a non-direct scope cannot be represented by the membership mirror")

	require.NoError(t, s.SetAppV30Provenance("signer-match", []AppV30ProvenanceSigner{{
		CredentialID: credential,
		PrincipalID:  testCredential("e"),
		Exact:        true,
	}}))
	verdict, err := s.ClassifyMemoryTaint("signer-match")
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, verdict.Outcome)
	require.Equal(t, []string{decisionID}, verdict.DecisionIDs)
	require.True(t, verdict.CoverageComplete)

	require.NoError(t, s.SetAppV30Provenance("signer-miss", []AppV30ProvenanceSigner{{
		CredentialID: testCredential("f"),
		PrincipalID:  testCredential("f"),
		Exact:        true,
	}}))
	verdict, err = s.ClassifyMemoryTaint("signer-miss")
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.True(t, verdict.CoverageComplete)
}

func TestClassifyMemoryTaintRootScopeRequiresExactRecordedGeneration(t *testing.T) {
	s := openTaintStore(t)
	rootPrincipal := testCredential("1")
	require.NoError(t, s.SetAppV30Provenance("root-generation-match", []AppV30ProvenanceSigner{{
		CredentialID:   testCredential("2"),
		PrincipalID:    rootPrincipal,
		RootGeneration: 3,
		Exact:          true,
	}}))
	require.NoError(t, s.SetMemoryAuthorPrincipal("ordinary-before-root", rootPrincipal))
	require.NoError(t, s.SetMemoryAuthor("ordinary-before-root", testCredential("2")))

	target := testTaintParent("root-scope-target")
	decisionScope := TaintDecisionScope{
		Kind:           tx.TaintScopeRootGeneration,
		PrincipalID:    rootPrincipal,
		GenerationFrom: 2,
		GenerationTo:   4,
	}
	decisionID := testTaintDecisionID("root-scope")
	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
		decisionScope,
	)
	require.NoError(t, err)
	require.True(t, created)

	verdict, err := s.ClassifyMemoryTaint("root-generation-match")
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, verdict.Outcome)
	require.Equal(t, []string{decisionID}, verdict.DecisionIDs)

	verdict, err = s.ClassifyMemoryTaint("ordinary-before-root")
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.False(t, verdict.CoverageComplete)
	require.True(t, verdict.LineageMissing)
}

func TestApplyTaintAdmissionAddsCredentialScopeWithoutParents(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("admission-scope-target")
	credential := testCredential("7")
	decisionScope := testCredentialScope(credential)
	decisionID := testTaintDecisionID("admission-scope")
	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
		decisionScope,
	)
	require.NoError(t, err)
	require.True(t, created)

	signers := []AppV30ProvenanceSigner{{
		CredentialID: credential,
		PrincipalID:  credential,
		Exact:        true,
	}}
	result, err := s.ValidateTaintAdmission("sage", "scope-admitted-child", nil, signers)
	require.NoError(t, err)
	require.Equal(t, 1, result.Decisions)

	result, err = s.ApplyTaintAdmission("sage", "scope-admitted-child", nil, signers)
	require.NoError(t, err)
	require.Equal(t, 1, result.Decisions)
	decisions, err := s.GetEffectiveTaintDecisions("scope-admitted-child")
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)

	missSigners := []AppV30ProvenanceSigner{{
		CredentialID: testCredential("8"),
		PrincipalID:  testCredential("8"),
		Exact:        true,
	}}
	result, err = s.ApplyTaintAdmission("sage", "scope-miss-child", nil, missSigners)
	require.NoError(t, err)
	require.Zero(t, result.Decisions)
	decisions, err = s.GetEffectiveTaintDecisions("scope-miss-child")
	require.NoError(t, err)
	require.Empty(t, decisions)
}

func TestTaintScopeServingUsesBoundedActiveDecisionIndex(t *testing.T) {
	s := openTaintStore(t)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		target := testTaintParent(fmt.Sprintf("retired-scope-target-%02d", i))
		decisionID := testTaintDecisionID(fmt.Sprintf("retired-scope-%02d", i))
		created, err := s.ApplyTaintDecision(
			"sage",
			decisionID,
			target,
			testTaintPayloadHash(decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
		changed, err := s.ClearTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, changed)
		for attempts := 0; attempts < 3; attempts++ {
			cleaned, err := s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
			require.NoError(t, err)
			if cleaned.Status == taint.StatusClean {
				break
			}
		}
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, taint.StatusClean, header.Status)
	}

	target := testTaintParent("active-after-history-target")
	decisionID := testTaintDecisionID("active-after-history")
	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
		testCredentialScope(testCredential("a")),
	)
	require.NoError(t, err)
	require.True(t, created)

	require.NoError(t, s.SetAppV30Provenance("bounded-history-memory", []AppV30ProvenanceSigner{{
		CredentialID: testCredential("b"),
		PrincipalID:  testCredential("b"),
		Exact:        true,
	}}))
	verdict, err := s.ClassifyMemoryTaint("bounded-history-memory")
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.True(t, verdict.CoverageComplete)
	require.Less(t, verdict.Units, 32)
}

func TestValidateTaintStateRejectsMissingOrMismatchedScope(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("scope-recovery-target")
	decisionID := testTaintDecisionID("scope-recovery")
	payloadHash := testTaintPayloadHash(decisionID)
	created, err := s.ApplyTaintDecisionWithScope(
		"sage",
		decisionID,
		target,
		payloadHash,
		testCredentialScope(testCredential("9")),
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, s.ValidateTaintState())

	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Delete(TaintDecisionScopeKey(decisionID))
	}))
	require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)

	encoded, err := encodeTaintDecisionScope(
		payloadHash,
		testCredentialScope(testCredential("9")),
	)
	require.NoError(t, err)
	encoded[1] ^= 0xff
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(TaintDecisionScopeKey(decisionID), encoded)
	}))
	require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)

	copy(encoded[1:], payloadHash[:])
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(TaintDecisionScopeKey(decisionID), encoded)
	}))
	require.NoError(t, s.ValidateTaintState())
	require.True(t, bytes.Equal(encoded[1:1+sha256.Size], payloadHash[:]))

	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Delete(taint.ActiveDecisionKey(decisionID))
	}))
	require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
		return txn.Set(taint.ActiveDecisionKey(decisionID), []byte{})
	}))
	require.NoError(t, s.ValidateTaintState())
}
