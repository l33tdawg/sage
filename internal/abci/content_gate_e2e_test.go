package abci

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/l33tdawg/sage/internal/contentvalidator"
	"github.com/l33tdawg/sage/internal/memory"
)

// TestContentGate_EndToEnd_RegisteredValidatorRejectsOnChain exercises the
// generic Layer-2 content gate end-to-end through processMemorySubmit using the
// same registration API any deployment uses — a trivial stub validator stands in
// for whatever schemas a deployment compiles into its own build (SAGE core ships
// none). It proves: the gate is dormant unless BOTH a registry is wired and the
// app-v7 fork is active (no separate enable flag); a registered (domain,
// outcome_class) whose validator errors becomes an on-chain Code 18 reject; an
// unregistered key passes through; and free-form prose (no JSON envelope, so
// outcome_class "") passes through.
func TestContentGate_EndToEnd_RegisteredValidatorRejectsOnChain(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	registerAgent(t, app, ak, "submitter", "member")

	const domain = "gate-test-domain"

	reg := contentvalidator.NewContentValidatorRegistry()
	reg.RegisterContentValidator(domain, "blocked", func(rec *memory.MemoryRecord) error {
		return errors.New("stub: blocked outcome_class is rejected")
	})

	blocked := `{"schema_version":1,"outcome_class":"blocked"}`
	allowed := `{"schema_version":1,"outcome_class":"allowed"}`

	// Dormant by default (nothing wired): the blocked body passes through and
	// auto-registers the domain to the submitter.
	d0 := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 10, time.Now())
	assert.Equal(t, uint32(0), d0.Code, "gate dormant (no registry/fork): blocked body must pass through")

	// Arm the gate: wire the registry + activate app-v7 at height 100. There is
	// no separate enable flag — a compiled-in registry past the fork is enough.
	app.SetContentValidators(reg)
	app.appV7AppliedHeight = 100 // postAppV7Fork(h) is true only for h > 100

	// At the activation height the fork is still pre-active (strict > semantic).
	preFork := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 100, time.Now())
	assert.Equal(t, uint32(0), preFork.Code, "at activation height: pre-fork, gate still dormant")

	// Post-fork: a registered (domain, "blocked") body is HARD-REJECTED on-chain.
	rej := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 101, time.Now())
	assert.Equal(t, uint32(18), rej.Code, "registered validator rejection must surface as Code 18")
	assert.Contains(t, rej.Log, "content schema rejected")

	// Post-fork: an UNREGISTERED (domain, "allowed") body passes through.
	pass := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, allowed), 102, time.Now())
	assert.Equal(t, uint32(0), pass.Code, "unregistered (domain, outcome_class) must pass through")

	// Post-fork: free-form prose (no JSON envelope => outcome_class "") passes through.
	prose := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, "just some prose, not json"), 103, time.Now())
	assert.Equal(t, uint32(0), prose.Code, "non-JSON prose (outcome_class \"\") must pass through")
}
