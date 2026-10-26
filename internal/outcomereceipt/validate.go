package outcomereceipt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

var (
	hashPattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
)

const maxTextFieldBytes = 4096

// Decode strictly decodes one signed receipt envelope. Unknown fields fail
// closed so a producer cannot attach unsigned meanings to v1.
func Decode(data []byte) (SignedReceipt, error) {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return SignedReceipt{}, fmt.Errorf("decode signed outcome receipt: %w", err)
	}
	var envelope SignedReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return SignedReceipt{}, fmt.Errorf("decode signed outcome receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return SignedReceipt{}, fmt.Errorf("decode signed outcome receipt: trailing JSON value")
		}
		return SignedReceipt{}, fmt.Errorf("decode signed outcome receipt: trailing data: %w", err)
	}
	return envelope, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, "$", 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, path string, depth int) error {
	if depth > 64 {
		return fmt.Errorf("JSON nesting exceeds 64 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s contains a non-string object key", path)
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("%s contains duplicate key %q", path, key)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, path+"."+key, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return fmt.Errorf("%s has an unexpected object terminator", path)
		}
	case '[':
		index := 0
		for decoder.More() {
			if err := scanJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
			index++
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return fmt.Errorf("%s has an unexpected array terminator", path)
		}
	default:
		return fmt.Errorf("%s has an unexpected delimiter %q", path, delim)
	}
	return nil
}

// Validate enforces the v1 evidence contract. It does not verify signatures;
// use VerifyEnvelope for a portable signed receipt.
func Validate(receipt Receipt) error {
	if receipt.Version != VersionV1 {
		return fmt.Errorf("version must be %q", VersionV1)
	}
	if err := validateUTC("created_at", receipt.CreatedAt); err != nil {
		return err
	}
	if err := validateTextField("producer.name", receipt.Producer.Name); err != nil {
		return err
	}
	if err := validateTextField("producer.version", receipt.Producer.Version); err != nil {
		return err
	}
	if err := validateHash("producer.implementation_hash", receipt.Producer.ImplementationHash); err != nil {
		return err
	}

	if err := validateID("task_set.id", receipt.TaskSet.ID); err != nil {
		return err
	}
	if err := validateTextField("task_set.version", receipt.TaskSet.Version); err != nil {
		return err
	}
	if err := validateHash("task_set.digest", receipt.TaskSet.Digest); err != nil {
		return err
	}
	if len(receipt.TaskSet.Tasks) == 0 {
		return fmt.Errorf("task_set.tasks must contain at least one task")
	}
	expectedTaskDigest, err := TaskSetDigest(receipt.TaskSet.Tasks)
	if err != nil {
		return err
	}
	if receipt.TaskSet.Digest != expectedTaskDigest {
		return fmt.Errorf("task_set.digest does not match the canonical task list")
	}

	taskIDs := make(map[string]struct{}, len(receipt.TaskSet.Tasks))
	for i, task := range receipt.TaskSet.Tasks {
		prefix := fmt.Sprintf("task_set.tasks[%d]", i)
		if err := validateTaskRef(prefix, task); err != nil {
			return err
		}
		if _, exists := taskIDs[task.MemoryID]; exists {
			return fmt.Errorf("%s duplicates memory_id %q", prefix, task.MemoryID)
		}
		taskIDs[task.MemoryID] = struct{}{}
	}

	if err := validateCandidate(receipt.Candidate); err != nil {
		return err
	}
	if _, exists := taskIDs[receipt.Candidate.MemoryID]; exists {
		return fmt.Errorf("candidate memory must not also be a task memory")
	}

	if err := validateEvaluation(receipt.Evaluation); err != nil {
		return err
	}
	if receipt.CreatedAt.Before(receipt.Evaluation.CompletedAt) {
		return fmt.Errorf("created_at must not precede evaluation.completed_at")
	}
	if len(receipt.Runs) == 0 {
		return fmt.Errorf("runs must not be empty")
	}
	if err := validateRuns(receipt, taskIDs); err != nil {
		return err
	}
	expectedPairs := len(receipt.TaskSet.Tasks) * receipt.Evaluation.Repeats
	if err := validateSummary(receipt.Summary, expectedPairs); err != nil {
		return err
	}
	if receipt.Summary.Status != StatusFailed {
		for i, run := range receipt.Runs {
			if run.Error != "" {
				return fmt.Errorf("runs[%d] records an error but summary.status is %q", i, receipt.Summary.Status)
			}
		}
	}
	if len(receipt.Evidence) == 0 {
		return fmt.Errorf("evidence must contain at least one artifact")
	}
	evidenceDigests := make(map[string]struct{}, len(receipt.Evidence))
	for i, evidence := range receipt.Evidence {
		prefix := fmt.Sprintf("evidence[%d]", i)
		if err := validateID(prefix+".kind", evidence.Kind); err != nil {
			return err
		}
		if err := validateTextField(prefix+".media_type", evidence.MediaType); err != nil {
			return err
		}
		if err := validateHash(prefix+".digest", evidence.Digest); err != nil {
			return err
		}
		if evidence.URI != "" {
			if err := validateTextField(prefix+".uri", evidence.URI); err != nil {
				return err
			}
		}
		if _, exists := evidenceDigests[evidence.Digest]; exists {
			return fmt.Errorf("%s duplicates evidence digest %q", prefix, evidence.Digest)
		}
		evidenceDigests[evidence.Digest] = struct{}{}
	}
	for i, caveat := range receipt.Caveats {
		if err := validateTextField(fmt.Sprintf("caveats[%d]", i), caveat); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskRef(prefix string, task TaskRef) error {
	if err := validateID(prefix+".chain_id", task.ChainID); err != nil {
		return err
	}
	if err := validateID(prefix+".memory_id", task.MemoryID); err != nil {
		return err
	}
	if err := validateHash(prefix+".content_hash", task.ContentHash); err != nil {
		return err
	}
	if err := validateTextField(prefix+".domain", task.Domain); err != nil {
		return err
	}
	if err := validateHash(prefix+".author_agent_id", task.AuthorAgentID); err != nil {
		return fmt.Errorf("%s.author_agent_id must be a lowercase 32-byte Ed25519 public key", prefix)
	}
	if task.Classification != 0 {
		return fmt.Errorf("%s.classification must be 0 (PUBLIC) in v1", prefix)
	}
	return nil
}

func validateCandidate(candidate MemoryRef) error {
	if err := validateID("candidate.chain_id", candidate.ChainID); err != nil {
		return err
	}
	if err := validateID("candidate.memory_id", candidate.MemoryID); err != nil {
		return err
	}
	if err := validateHash("candidate.content_hash", candidate.ContentHash); err != nil {
		return err
	}
	if err := validateTextField("candidate.domain", candidate.Domain); err != nil {
		return err
	}
	if err := validateHash("candidate.author_agent_id", candidate.AuthorAgentID); err != nil {
		return fmt.Errorf("candidate.author_agent_id must be a lowercase 32-byte Ed25519 public key")
	}
	if candidate.Classification != 0 {
		return fmt.Errorf("candidate.classification must be 0 (PUBLIC) in v1")
	}
	return nil
}

func validateEvaluation(evaluation Evaluation) error {
	if err := validateTextField("evaluation.judge.model_id", evaluation.Judge.ModelID); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{"evaluation.judge.model_hash", evaluation.Judge.ModelHash},
		{"evaluation.judge.rubric_hash", evaluation.Judge.RubricHash},
		{"evaluation.judge.prompt_hash", evaluation.Judge.PromptHash},
		{"evaluation.judge.environment_hash", evaluation.Judge.EnvironmentHash},
	} {
		if err := validateHash(field.name, field.value); err != nil {
			return err
		}
	}
	if err := validateID("evaluation.retrieval.id", evaluation.Retrieval.ID); err != nil {
		return err
	}
	if err := validateTextField("evaluation.retrieval.version", evaluation.Retrieval.Version); err != nil {
		return err
	}
	if evaluation.Retrieval.TopK < 1 || evaluation.Retrieval.TopK > 1000 {
		return fmt.Errorf("evaluation.retrieval.top_k must be between 1 and 1000")
	}
	if err := validateTextField("evaluation.metric", evaluation.Metric); err != nil {
		return err
	}
	if err := validateTextField("evaluation.unit", evaluation.Unit); err != nil {
		return err
	}
	if evaluation.Repeats < 1 {
		return fmt.Errorf("evaluation.repeats must be at least 1")
	}
	if err := validateUTC("evaluation.started_at", evaluation.StartedAt); err != nil {
		return err
	}
	if err := validateUTC("evaluation.completed_at", evaluation.CompletedAt); err != nil {
		return err
	}
	if evaluation.CompletedAt.Before(evaluation.StartedAt) {
		return fmt.Errorf("evaluation.completed_at must not precede started_at")
	}
	return nil
}

func validateRuns(receipt Receipt, taskIDs map[string]struct{}) error {
	type pairKey struct {
		taskMemoryID string
		trial        int
	}
	type pairState struct {
		control   *Run
		treatment *Run
	}
	pairs := make(map[pairKey]*pairState)
	for i, run := range receipt.Runs {
		prefix := fmt.Sprintf("runs[%d]", i)
		if _, exists := taskIDs[run.TaskMemoryID]; !exists {
			return fmt.Errorf("%s.task_memory_id %q is not in task_set.tasks", prefix, run.TaskMemoryID)
		}
		if run.Trial < 1 || run.Trial > receipt.Evaluation.Repeats {
			return fmt.Errorf("%s.trial must be between 1 and evaluation.repeats", prefix)
		}
		if run.Arm != ArmControl && run.Arm != ArmTreatment {
			return fmt.Errorf("%s.arm must be %q or %q", prefix, ArmControl, ArmTreatment)
		}
		if err := validateHash(prefix+".query_hash", run.QueryHash); err != nil {
			return err
		}
		if err := validateIDSet(prefix+".context_memory_ids", run.ContextMemoryIDs); err != nil {
			return err
		}
		if err := validateIDSet(prefix+".masked_memory_ids", run.MaskedMemoryIDs); err != nil {
			return err
		}
		for _, id := range run.MaskedMemoryIDs {
			if contains(run.ContextMemoryIDs, id) {
				return fmt.Errorf("%s cannot both include and mask memory %q", prefix, id)
			}
		}
		candidatePresent := contains(run.ContextMemoryIDs, receipt.Candidate.MemoryID)
		candidateMasked := contains(run.MaskedMemoryIDs, receipt.Candidate.MemoryID)
		switch run.Arm {
		case ArmTreatment:
			if !candidatePresent {
				return fmt.Errorf("%s treatment context must include the candidate memory", prefix)
			}
			if candidateMasked {
				return fmt.Errorf("%s treatment must not mask the candidate memory", prefix)
			}
		case ArmControl:
			if candidatePresent {
				return fmt.Errorf("%s control context must not include the candidate memory", prefix)
			}
			if !candidateMasked {
				return fmt.Errorf("%s control must record the candidate in masked_memory_ids", prefix)
			}
		}
		if run.Score == "" && run.Passed == nil && run.Error == "" {
			return fmt.Errorf("%s must carry score, passed, or error", prefix)
		}
		if run.Score != "" {
			if err := validateDecimal(prefix+".score", run.Score); err != nil {
				return err
			}
		}
		if run.LatencyMicros < 0 {
			return fmt.Errorf("%s.latency_micros must not be negative", prefix)
		}
		for j, artifact := range run.ArtifactHashes {
			if err := validateHash(fmt.Sprintf("%s.artifact_hashes[%d]", prefix, j), artifact); err != nil {
				return err
			}
		}
		if hasDuplicates(run.ArtifactHashes) {
			return fmt.Errorf("%s.artifact_hashes contains duplicates", prefix)
		}

		key := pairKey{taskMemoryID: run.TaskMemoryID, trial: run.Trial}
		state := pairs[key]
		if state == nil {
			state = &pairState{}
			pairs[key] = state
		}
		if run.Arm == ArmControl {
			if state.control != nil {
				return fmt.Errorf("%s duplicates the control arm for task/trial", prefix)
			}
			runCopy := run
			state.control = &runCopy
		} else {
			if state.treatment != nil {
				return fmt.Errorf("%s duplicates the treatment arm for task/trial", prefix)
			}
			runCopy := run
			state.treatment = &runCopy
		}
	}

	expectedPairs := len(receipt.TaskSet.Tasks) * receipt.Evaluation.Repeats
	if len(pairs) != expectedPairs {
		return fmt.Errorf("runs contain %d task/trial pairs, want %d", len(pairs), expectedPairs)
	}
	for key, state := range pairs {
		if state.control == nil || state.treatment == nil {
			return fmt.Errorf("task %q trial %d must contain one control and one treatment run", key.taskMemoryID, key.trial)
		}
		if state.control.QueryHash != state.treatment.QueryHash {
			return fmt.Errorf("task %q trial %d arms must use the same query_hash", key.taskMemoryID, key.trial)
		}
		if state.control.Seed != state.treatment.Seed {
			return fmt.Errorf("task %q trial %d arms must use the same seed", key.taskMemoryID, key.trial)
		}
		treatmentWithoutCandidate, removed := removeString(state.treatment.ContextMemoryIDs, receipt.Candidate.MemoryID)
		if !removed || !slices.Equal(state.control.ContextMemoryIDs, treatmentWithoutCandidate) {
			return fmt.Errorf("task %q trial %d treatment must equal the control context plus the candidate", key.taskMemoryID, key.trial)
		}
		controlMasksWithoutCandidate, hadCandidate := removeString(state.control.MaskedMemoryIDs, receipt.Candidate.MemoryID)
		if !hadCandidate || !sameStringSet(controlMasksWithoutCandidate, state.treatment.MaskedMemoryIDs) {
			return fmt.Errorf("task %q trial %d masked memories must differ only by the candidate", key.taskMemoryID, key.trial)
		}
	}
	return nil
}

func validateSummary(summary Summary, expectedPairs int) error {
	switch summary.Status {
	case StatusPositive, StatusNeutral, StatusNegative, StatusInconclusive, StatusFailed:
	default:
		return fmt.Errorf("summary.status %q is not recognized", summary.Status)
	}
	if summary.Pairs < 0 {
		return fmt.Errorf("summary.pairs must not be negative")
	}
	if summary.Pairs != expectedPairs {
		return fmt.Errorf("summary.pairs is %d, want %d", summary.Pairs, expectedPairs)
	}
	values := []struct {
		name  string
		value string
	}{
		{"summary.control_mean", summary.ControlMean},
		{"summary.treatment_mean", summary.TreatmentMean},
		{"summary.delta", summary.Delta},
		{"summary.ci95_lower", summary.CI95Lower},
		{"summary.ci95_upper", summary.CI95Upper},
	}
	for _, field := range values {
		if field.value == "" {
			continue
		}
		if err := validateDecimal(field.name, field.value); err != nil {
			return err
		}
	}
	if summary.Status != StatusFailed {
		for _, field := range values {
			if field.value == "" {
				return fmt.Errorf("%s is required when summary.status is %q", field.name, summary.Status)
			}
		}
	}
	return nil
}

func validateUTC(field string, value time.Time) error {
	if value.IsZero() {
		return fmt.Errorf("%s is required", field)
	}
	if value.Location() != time.UTC {
		return fmt.Errorf("%s must be expressed in UTC", field)
	}
	return nil
}

func validateHash(field, value string) error {
	if !hashPattern.MatchString(value) {
		return fmt.Errorf("%s must be a lowercase 64-character SHA-256 hex digest", field)
	}
	return nil
}

func validateID(field, value string) error {
	if !idPattern.MatchString(value) {
		return fmt.Errorf("%s must be 1-128 ASCII identifier characters", field)
	}
	return nil
}

func validateTextField(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", field)
	}
	if len(value) > maxTextFieldBytes {
		return fmt.Errorf("%s exceeds %d bytes", field, maxTextFieldBytes)
	}
	if !norm.NFC.IsNormalString(value) {
		return fmt.Errorf("%s must be NFC-normalized", field)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s contains a forbidden control character", field)
		}
	}
	return nil
}

func validateIDSet(field string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for i, value := range values {
		if err := validateID(fmt.Sprintf("%s[%d]", field, i), value); err != nil {
			return err
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("%s contains duplicate value %q", field, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateDecimal(field, value string) error {
	if !decimalPattern.MatchString(value) {
		return fmt.Errorf("%s must be a canonical decimal string", field)
	}
	if value == "-0" || strings.HasPrefix(value, "-0.") {
		return fmt.Errorf("%s must not represent negative zero", field)
	}
	if dot := strings.IndexByte(value, '.'); dot >= 0 && strings.HasSuffix(value, "0") {
		return fmt.Errorf("%s must not contain trailing fractional zeroes", field)
	}
	return nil
}

func validateSignatureMetadata(role, signerID string) error {
	switch role {
	case RoleSAGEAgent, RoleEvaluator, RoleNoesis:
	default:
		return fmt.Errorf("signature role %q is not recognized", role)
	}
	return validateTextField("signature.signer_id", signerID)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func removeString(values []string, target string) ([]string, bool) {
	out := make([]string, 0, len(values))
	removed := false
	for _, value := range values {
		if value == target && !removed {
			removed = true
			continue
		}
		out = append(out, value)
	}
	return out, removed
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]string(nil), a...)
	right := append([]string(nil), b...)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

func hasDuplicates(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
