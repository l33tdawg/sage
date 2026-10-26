package outcomereceipt

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testCandidateID = "mem-candidate"
	testContextA    = "mem-context-a"
	testContextB    = "mem-context-b"
)

func TestCanonicalJSONNormalizesUnorderedFields(t *testing.T) {
	receipt := validReceipt(t)
	first, err := DigestHex(receipt)
	require.NoError(t, err)
	require.Equal(t, "84d860249ab99478850a1dd91248887ff9a9950882d029d0449b3308ad9f0d54", first)

	reordered := receipt
	reordered.TaskSet.Tasks = append([]TaskRef(nil), receipt.TaskSet.Tasks...)
	reordered.TaskSet.Tasks[0], reordered.TaskSet.Tasks[1] = reordered.TaskSet.Tasks[1], reordered.TaskSet.Tasks[0]
	reordered.Runs = append([]Run(nil), receipt.Runs...)
	reordered.Runs[0], reordered.Runs[len(reordered.Runs)-1] = reordered.Runs[len(reordered.Runs)-1], reordered.Runs[0]
	reordered.Evidence = append([]EvidenceRef(nil), receipt.Evidence...)

	second, err := DigestHex(reordered)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestContextOrderChangesDigest(t *testing.T) {
	receipt := validReceipt(t)
	first, err := DigestHex(receipt)
	require.NoError(t, err)

	changed := receipt
	for i := range changed.Runs {
		if changed.Runs[i].TaskMemoryID != receipt.Runs[0].TaskMemoryID || changed.Runs[i].Trial != receipt.Runs[0].Trial {
			continue
		}
		switch changed.Runs[i].Arm {
		case ArmControl:
			changed.Runs[i].ContextMemoryIDs = []string{testContextA, testContextB}
		case ArmTreatment:
			changed.Runs[i].ContextMemoryIDs = []string{testContextA, testContextB, testCandidateID}
		}
	}
	second, err := DigestHex(changed)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestValidateRejectsCounterfactualInvariantViolations(t *testing.T) {
	t.Run("candidate in control context", func(t *testing.T) {
		receipt := validReceipt(t)
		for i := range receipt.Runs {
			if receipt.Runs[i].Arm == ArmControl {
				receipt.Runs[i].ContextMemoryIDs = append(receipt.Runs[i].ContextMemoryIDs, testCandidateID)
				receipt.Runs[i].MaskedMemoryIDs = nil
				break
			}
		}
		require.ErrorContains(t, Validate(receipt), "control context must not include the candidate")
	})

	t.Run("missing treatment arm", func(t *testing.T) {
		receipt := validReceipt(t)
		for i, run := range receipt.Runs {
			if run.Arm == ArmTreatment {
				receipt.Runs = append(receipt.Runs[:i], receipt.Runs[i+1:]...)
				break
			}
		}
		require.ErrorContains(t, Validate(receipt), "must contain one control and one treatment")
	})

	t.Run("treatment changes more than the candidate", func(t *testing.T) {
		receipt := validReceipt(t)
		for i := range receipt.Runs {
			if receipt.Runs[i].Arm == ArmTreatment {
				receipt.Runs[i].ContextMemoryIDs = []string{testContextA, testContextB, "mem-added", testCandidateID}
				break
			}
		}
		require.ErrorContains(t, Validate(receipt), "must equal the control context plus the candidate")
	})

	t.Run("non-public candidate", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.Candidate.Classification = 1
		require.ErrorContains(t, Validate(receipt), "classification must be 0")
	})

	t.Run("task digest mismatch", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.TaskSet.Digest = strings.Repeat("0", 64)
		require.ErrorContains(t, Validate(receipt), "task_set.digest does not match")
	})

	t.Run("duplicate context", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.Runs[0].ContextMemoryIDs = append(receipt.Runs[0].ContextMemoryIDs, receipt.Runs[0].ContextMemoryIDs[0])
		require.ErrorContains(t, Validate(receipt), "contains duplicate value")
	})

	t.Run("uppercase digest", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.TaskSet.Tasks[0].ContentHash = strings.ToUpper(receipt.TaskSet.Tasks[0].ContentHash)
		digest, err := TaskSetDigest(receipt.TaskSet.Tasks)
		require.NoError(t, err)
		receipt.TaskSet.Digest = digest
		require.ErrorContains(t, Validate(receipt), "lowercase 64-character SHA-256")
	})

	t.Run("receipt created before evaluation completed", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.CreatedAt = receipt.Evaluation.CompletedAt.Add(-time.Second)
		require.ErrorContains(t, Validate(receipt), "created_at must not precede")
	})

	t.Run("non-UTC timestamp", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.CreatedAt = receipt.CreatedAt.In(time.FixedZone("local", 8*60*60))
		require.ErrorContains(t, Validate(receipt), "must be expressed in UTC")
	})

	t.Run("successful summary with failed run", func(t *testing.T) {
		receipt := validReceipt(t)
		receipt.Runs[0].Error = "evaluator failed"
		require.ErrorContains(t, Validate(receipt), "records an error")
	})
}

func TestSignedReceiptRoundTrip(t *testing.T) {
	receipt := validReceipt(t)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	signature, err := Sign(receipt, RoleEvaluator, "eval-harness-1", privateKey)
	require.NoError(t, err)
	envelope := SignedReceipt{
		Receipt:    receipt,
		Signatures: []Signature{signature},
	}
	require.NoError(t, VerifyEnvelope(envelope))

	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)
	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.NoError(t, VerifyEnvelope(decoded))

	tampered := decoded
	tampered.Receipt.Summary.Delta = "0.26"
	require.ErrorContains(t, VerifyEnvelope(tampered), "signature verification failed")
}

func TestVerifyEnvelopeRequiresEvaluatorAndUniqueSignatures(t *testing.T) {
	receipt := validReceipt(t)
	_, sageKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sageID := testPublicKeyHex(sageKey)
	sageSignature, err := Sign(receipt, RoleSAGEAgent, sageID, sageKey)
	require.NoError(t, err)

	require.ErrorContains(t, VerifyEnvelope(SignedReceipt{
		Receipt:    receipt,
		Signatures: []Signature{sageSignature},
	}), "no evaluator signature")

	_, evaluatorKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	evaluatorSignature, err := Sign(receipt, RoleEvaluator, "eval-harness-1", evaluatorKey)
	require.NoError(t, err)
	require.NoError(t, VerifyEnvelope(SignedReceipt{
		Receipt:    receipt,
		Signatures: []Signature{sageSignature, evaluatorSignature},
	}))

	require.ErrorContains(t, VerifyEnvelope(SignedReceipt{
		Receipt:    receipt,
		Signatures: []Signature{evaluatorSignature, evaluatorSignature},
	}), "duplicate signature")
}

func TestDecodeRejectsUnknownAndTrailingValues(t *testing.T) {
	_, err := Decode([]byte(`{"receipt":{},"signatures":[],"unexpected":true}`))
	require.ErrorContains(t, err, "unknown field")

	_, err = Decode([]byte(`{"receipt":{},"signatures":[]} {}`))
	require.ErrorContains(t, err, "trailing JSON value")

	_, err = Decode([]byte(`{"receipt":{},"receipt":{},"signatures":[]}`))
	require.ErrorContains(t, err, "duplicate key")
}

func TestSAGEAgentSignerIDMustMatchPublicKey(t *testing.T) {
	receipt := validReceipt(t)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = Sign(receipt, RoleSAGEAgent, "different-agent-id", key)
	require.ErrorContains(t, err, "does not match the signing public key")
}

func validReceipt(t *testing.T) Receipt {
	t.Helper()

	tasks := []TaskRef{
		{
			ChainID:        "sage-test",
			MemoryID:       "mem-task-a",
			ContentHash:    hashOf("task-a"),
			Domain:         "research.eval",
			AuthorAgentID:  hashOf("task-author-a"),
			Classification: 0,
		},
		{
			ChainID:        "sage-test",
			MemoryID:       "mem-task-b",
			ContentHash:    hashOf("task-b"),
			Domain:         "research.eval",
			AuthorAgentID:  hashOf("task-author-b"),
			Classification: 0,
		},
	}
	taskDigest, err := TaskSetDigest(tasks)
	require.NoError(t, err)

	started := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	receipt := Receipt{
		Version:   VersionV1,
		CreatedAt: started.Add(5 * time.Minute),
		Producer: Producer{
			Name:               "sage-noesis-pilot",
			Version:            "0.1.0",
			ImplementationHash: hashOf("producer"),
		},
		TaskSet: TaskSet{
			ID:      "pilot-task-set",
			Version: "1",
			Digest:  taskDigest,
			Tasks:   tasks,
		},
		Candidate: MemoryRef{
			ChainID:        "sage-test",
			MemoryID:       testCandidateID,
			ContentHash:    hashOf("candidate"),
			Domain:         "research.eval",
			AuthorAgentID:  hashOf("author"),
			Classification: 0,
		},
		Evaluation: Evaluation{
			Judge: JudgeSpec{
				ModelID:         "judge-model-v1",
				ModelHash:       hashOf("model"),
				RubricHash:      hashOf("rubric"),
				PromptHash:      hashOf("prompt"),
				EnvironmentHash: hashOf("environment"),
			},
			Retrieval: RetrievalPolicy{
				ID:      "sage-hybrid",
				Version: "1",
				TopK:    5,
			},
			Metric:         "task_pass_rate",
			Unit:           "ratio",
			HigherIsBetter: true,
			Repeats:        2,
			ForcedContext:  true,
			StartedAt:      started,
			CompletedAt:    started.Add(5 * time.Minute),
		},
		Summary: Summary{
			Status:        StatusPositive,
			Pairs:         4,
			ControlMean:   "0.5",
			TreatmentMean: "0.75",
			Delta:         "0.25",
			CI95Lower:     "0.1",
			CI95Upper:     "0.4",
		},
		Evidence: []EvidenceRef{
			{
				Kind:      "manifest",
				MediaType: "application/json",
				Digest:    hashOf("manifest"),
			},
		},
		Caveats: []string{"Synthetic fixture for the v1 contract."},
	}
	for _, task := range tasks {
		for trial := 1; trial <= receipt.Evaluation.Repeats; trial++ {
			queryHash := hashOf(fmt.Sprintf("%s/%d", task.MemoryID, trial))
			receipt.Runs = append(receipt.Runs,
				Run{
					TaskMemoryID:     task.MemoryID,
					Trial:            trial,
					Arm:              ArmControl,
					Seed:             uint64(100 + trial),
					QueryHash:        queryHash,
					ContextMemoryIDs: []string{testContextB, testContextA},
					MaskedMemoryIDs:  []string{testCandidateID},
					Score:            "0.5",
					ArtifactHashes:   []string{hashOf("control-" + task.MemoryID)},
				},
				Run{
					TaskMemoryID:     task.MemoryID,
					Trial:            trial,
					Arm:              ArmTreatment,
					Seed:             uint64(100 + trial),
					QueryHash:        queryHash,
					ContextMemoryIDs: []string{testContextB, testContextA, testCandidateID},
					Score:            "0.75",
					ArtifactHashes:   []string{hashOf("treatment-" + task.MemoryID)},
				},
			)
		}
	}
	require.NoError(t, Validate(receipt))
	return receipt
}

func hashOf(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func testPublicKeyHex(privateKey ed25519.PrivateKey) string {
	return hex.EncodeToString(privateKey.Public().(ed25519.PublicKey))
}
