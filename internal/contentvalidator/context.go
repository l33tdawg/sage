package contentvalidator

import (
	"sort"
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

// ClosedDomainFunctions returns the frozen domain's distinct registered
// functions in deterministic order. Once any policy is active, core must still
// route unknown classes through the domain's closed-schema validation.
func (r *ContentValidatorRegistry) ClosedDomainFunctions(domain string) []string {
	if r == nil {
		return nil
	}
	if _, closed := r.closed[domain]; !closed {
		return nil
	}
	if _, registered := r.domains[domain]; !registered {
		return nil
	}
	seen := make(map[string]struct{})
	for key, function := range r.contextFunctions {
		if key.domain == domain && function != "" {
			seen[function] = struct{}{}
		}
	}
	functions := make([]string, 0, len(seen))
	for function := range seen {
		functions = append(functions, function)
	}
	sort.Strings(functions)
	return functions
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
