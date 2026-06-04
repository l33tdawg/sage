package contentvalidator

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// errStub is a ContentValidator that always rejects with a fixed error. Used to
// exercise the "registered validator returns an error => REJECT" path.
func errStub(msg string) ContentValidator {
	return func(_ *memory.MemoryRecord) error {
		return errors.New(msg)
	}
}

// passStub is a ContentValidator that always accepts. Used to exercise the
// "registered validator returns nil => pass-through" path.
func passStub() ContentValidator {
	return func(_ *memory.MemoryRecord) error {
		return nil
	}
}

// jsonStub is a ContentValidator that treats rec.Content as JSON and rejects any
// content that fails to unmarshal. It stands in for a real schema validator that
// hard-rejects malformed bodies.
func jsonStub() ContentValidator {
	return func(rec *memory.MemoryRecord) error {
		var v map[string]interface{}
		if err := json.Unmarshal([]byte(rec.Content), &v); err != nil {
			return errors.New("malformed content")
		}
		return nil
	}
}

func TestValidate(t *testing.T) {
	const (
		domain  = "sentinel"
		outcome = "action"
	)

	// registered builds a fresh registry with a single validator under
	// (domain, outcome) so each case is isolated from the others.
	registered := func(v ContentValidator) *ContentValidatorRegistry {
		r := NewContentValidatorRegistry()
		r.RegisterContentValidator(domain, outcome, v)
		return r
	}

	tests := []struct {
		name         string
		registry     *ContentValidatorRegistry
		domain       string
		outcomeClass string
		content      string
		wantRejected bool
		wantReason   string
	}{
		{
			// (a) nil receiver is a pass-through.
			name:         "nil registry passes through",
			registry:     nil,
			domain:       domain,
			outcomeClass: outcome,
			content:      "anything",
			wantRejected: false,
			wantReason:   "",
		},
		{
			// (b) empty registry (no validators registered) is a pass-through.
			name:         "empty registry passes through",
			registry:     NewContentValidatorRegistry(),
			domain:       domain,
			outcomeClass: outcome,
			content:      "anything",
			wantRejected: false,
			wantReason:   "",
		},
		{
			// (c) a key that was never registered is a pass-through, even when
			// other keys are registered (backward-compat for unknown domains).
			name:         "registered but unmatched key passes through",
			registry:     registered(errStub("should not run")),
			domain:       "other-domain",
			outcomeClass: "other-outcome",
			content:      "anything",
			wantRejected: false,
			wantReason:   "",
		},
		{
			// (d) a registered validator that returns nil accepts.
			name:         "registered validator returns nil accepts",
			registry:     registered(passStub()),
			domain:       domain,
			outcomeClass: outcome,
			content:      "anything",
			wantRejected: false,
			wantReason:   "",
		},
		{
			// (e) a registered validator that returns an error is a HARD REJECT,
			// and the error string is surfaced verbatim as the reason.
			name:         "registered validator returns error rejects",
			registry:     registered(errStub("schema violation: field x missing")),
			domain:       domain,
			outcomeClass: outcome,
			content:      "anything",
			wantRejected: true,
			wantReason:   "schema violation: field x missing",
		},
		{
			// (f) the JSON-shaped validator accepts well-formed content.
			name:         "json validator accepts valid json",
			registry:     registered(jsonStub()),
			domain:       domain,
			outcomeClass: outcome,
			content:      `{"schema_version":1,"outcome_class":"action"}`,
			wantRejected: false,
			wantReason:   "",
		},
		{
			// (f) the JSON-shaped validator hard-rejects malformed content.
			name:         "json validator rejects malformed content",
			registry:     registered(jsonStub()),
			domain:       domain,
			outcomeClass: outcome,
			content:      "this is not json",
			wantRejected: true,
			wantReason:   "malformed content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &memory.MemoryRecord{Content: tt.content}

			rejected, reason := tt.registry.Validate(tt.domain, tt.outcomeClass, rec)
			assert.Equal(t, tt.wantRejected, rejected)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

// TestValidateClosedDomain covers RegisterClosedDomain: on a closed domain that
// has at least one registered validator, an unregistered/empty outcome_class is a
// HARD REJECT (closing the fail-open hole) — while open domains and zero-validator
// closed domains keep the backward-compatible pass-through.
func TestValidateClosedDomain(t *testing.T) {
	const (
		domain  = "events"
		outcome = "action"
	)

	// closedWithValidator: domain marked closed AND one validator registered.
	closedWithValidator := func() *ContentValidatorRegistry {
		r := NewContentValidatorRegistry()
		r.RegisterContentValidator(domain, outcome, passStub())
		r.RegisterClosedDomain(domain)
		return r
	}

	rec := &memory.MemoryRecord{Content: "anything"}

	t.Run("closed domain rejects unregistered class", func(t *testing.T) {
		r := closedWithValidator()
		rejected, reason := r.Validate(domain, "other", rec)
		assert.True(t, rejected)
		assert.Equal(t, `unrecognized outcome_class "other" for closed domain "events"`, reason)
	})

	t.Run("closed domain rejects empty class", func(t *testing.T) {
		// The empty class is what the router yields for a malformed/cross-class
		// body — on a closed domain it must reject, not pass through.
		r := closedWithValidator()
		rejected, reason := r.Validate(domain, "", rec)
		assert.True(t, rejected)
		assert.Equal(t, `unrecognized outcome_class "" for closed domain "events"`, reason)
	})

	t.Run("closed domain still runs the registered class validator", func(t *testing.T) {
		r := NewContentValidatorRegistry()
		r.RegisterContentValidator(domain, outcome, errStub("schema violation"))
		r.RegisterClosedDomain(domain)
		// The registered class routes to its validator (hard reject), NOT to the
		// closed-domain reason — closing a domain doesn't shadow its validators.
		rejected, reason := r.Validate(domain, outcome, rec)
		assert.True(t, rejected)
		assert.Equal(t, "schema violation", reason)
	})

	t.Run("closed domain with zero validators is a no-op pass-through", func(t *testing.T) {
		// Closing a domain you have not yet registered any validator for must not
		// lock it out entirely — it stays pass-through until a validator exists.
		r := NewContentValidatorRegistry()
		r.RegisterClosedDomain(domain)
		// A different domain has a validator so the registry isn't empty.
		r.RegisterContentValidator("telemetry", "metric", passStub())
		rejected, reason := r.Validate(domain, "anything", rec)
		assert.False(t, rejected)
		assert.Equal(t, "", reason)
	})

	t.Run("open domain unregistered class still passes through", func(t *testing.T) {
		// Backward-compat: a domain that was never marked closed keeps the old
		// pass-through-on-unregistered-key semantics.
		r := NewContentValidatorRegistry()
		r.RegisterContentValidator(domain, outcome, passStub())
		rejected, reason := r.Validate(domain, "other", rec)
		assert.False(t, rejected)
		assert.Equal(t, "", reason)
	})

	t.Run("closed-domain reject is deterministic", func(t *testing.T) {
		r := closedWithValidator()
		rejected1, reason1 := r.Validate(domain, "other", rec)
		rejected2, reason2 := r.Validate(domain, "other", rec)
		assert.Equal(t, rejected1, rejected2)
		assert.Equal(t, reason1, reason2)
	})
}

// TestValidateDeterminism asserts (g): repeated Validate calls on identical input
// return bit-identical (bool, string). Consensus correctness depends on this.
func TestValidateDeterminism(t *testing.T) {
	r := NewContentValidatorRegistry()
	r.RegisterContentValidator("sentinel", "action", errStub("deterministic reject"))

	rec := &memory.MemoryRecord{Content: `{"schema_version":1,"outcome_class":"action"}`}

	rejected1, reason1 := r.Validate("sentinel", "action", rec)
	rejected2, reason2 := r.Validate("sentinel", "action", rec)

	require.True(t, rejected1)
	assert.Equal(t, rejected1, rejected2)
	assert.Equal(t, reason1, reason2)
	assert.Equal(t, "deterministic reject", reason1)
}

// TestValidateIndependentKeys asserts (h): two distinct (domain, outcomeClass)
// keys route to their own validators with no cross-talk.
func TestValidateIndependentKeys(t *testing.T) {
	r := NewContentValidatorRegistry()
	r.RegisterContentValidator("sentinel", "action", errStub("action rejected"))
	r.RegisterContentValidator("sentinel", "detection", passStub())

	rec := &memory.MemoryRecord{Content: "anything"}

	// The "action" key rejects with its own reason.
	rejected, reason := r.Validate("sentinel", "action", rec)
	assert.True(t, rejected)
	assert.Equal(t, "action rejected", reason)

	// The "detection" key, registered with a passing validator, accepts —
	// proving the action validator did not bleed into the detection route.
	rejected, reason = r.Validate("sentinel", "detection", rec)
	assert.False(t, rejected)
	assert.Equal(t, "", reason)

	// Same domain, an unregistered outcome class still passes through.
	rejected, reason = r.Validate("sentinel", "unknown", rec)
	assert.False(t, rejected)
	assert.Equal(t, "", reason)
}
