package contentvalidator

import (
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

// FunctionAuthorityRequest names one canonical local functional assignment.
// A write grant, role, profile or body claim alone is not that assignment.
type FunctionAuthorityRequest struct {
	Function string
	Domain   string
	Subject  string
}

// FunctionAuthorityResult repeats the exact scope evaluated by consensus.
type FunctionAuthorityResult struct {
	Function string
	Domain   string
	Subject  string
	Allowed  bool
}

type FunctionAuthorizer func(FunctionAuthorityRequest) (FunctionAuthorityResult, error)

// ValidationContext is created for each executing submission. Its authorizer
// must close over that invocation's scoped consensus store and canonical time,
// never the constructing app or a shared mutable registry.
type ValidationContext struct {
	Height            int64
	BlockTime         time.Time
	AuthorizeFunction FunctionAuthorizer
}

type ContextContentValidator func(*memory.MemoryRecord, ValidationContext) error

// RegisterContextContentValidator is boot-only, like RegisterContentValidator.
// A legacy validator must also be registered for replay before adoption.
func (r *ContentValidatorRegistry) RegisterContextContentValidator(domain, outcomeClass string, validator ContextContentValidator) {
	if r.contextual == nil {
		r.contextual = make(map[validatorKey]ContextContentValidator)
	}
	r.contextual[validatorKey{domain: domain, outcomeClass: outcomeClass}] = validator
}

// RegisterFunctionContentValidator binds a contextual schema to the canonical
// function whose future policy selects it. Existing validators remain the
// replay path before that policy's activation height.
func (r *ContentValidatorRegistry) RegisterFunctionContentValidator(domain, outcomeClass, function string, validator ContextContentValidator) {
	r.RegisterContextContentValidator(domain, outcomeClass, validator)
	if r.contextFunctions == nil {
		r.contextFunctions = make(map[validatorKey]string)
	}
	r.contextFunctions[validatorKey{domain: domain, outcomeClass: outcomeClass}] = function
}

func (r *ContentValidatorRegistry) FunctionFor(domain, outcomeClass string) string {
	if r == nil {
		return ""
	}
	return r.contextFunctions[validatorKey{domain: domain, outcomeClass: outcomeClass}]
}

// HasFunction reports whether this frozen binary can execute the named
// function on the exact domain. Metadata without a callable contextual
// validator and its legacy replay validator is not enforcement readiness.
func (r *ContentValidatorRegistry) HasFunction(domain, function string) bool {
	if r == nil || function == "" {
		return false
	}
	for key, registered := range r.contextFunctions {
		if key.domain == domain && registered == function && r.contextual[key] != nil && r.m[key] != nil {
			return true
		}
	}
	return false
}

// ValidateWithContext is called only at a governed execution boundary. Unknown
// keys keep the registry's existing open/closed-domain behavior; a registered
// contextual key with a missing implementation fails closed.
func (r *ContentValidatorRegistry) ValidateWithContext(domain, outcomeClass string, rec *memory.MemoryRecord, ctx ValidationContext) (bool, string) {
	if r == nil {
		return false, ""
	}
	if validator, exists := r.contextual[validatorKey{domain: domain, outcomeClass: outcomeClass}]; exists {
		if validator == nil || ctx.Height <= 0 || ctx.BlockTime.IsZero() || ctx.AuthorizeFunction == nil {
			return true, "content validator: missing functional authority execution context"
		}
		if err := validator(rec, ctx); err != nil {
			return true, err.Error()
		}
		return false, ""
	}
	return r.Validate(domain, outcomeClass, rec)
}
