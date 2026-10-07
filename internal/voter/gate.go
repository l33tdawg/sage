package voter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/memory"
)

// Gate is the optional memory gate: a semantic check, asked of one or more
// operator-configured judges, of whether a proposed memory is lasting
// knowledge or a remark about the session it came from — and, for a memory
// submitted with evidence, whether that evidence supports it.
//
// It exists because the built-in checks trust the author's self-declared
// confidence: an agent that stores "the attachment was lost; the user must
// re-send the numbers" as a 0.9 fact gets it committed, and later sessions
// recall it as if it were true.
//
// Design constraints:
//
//   - Judging never runs in the voter loop. The loop only READS a stored
//     semantic verdict; a memory without one is handed to a bounded background
//     evaluator and skipped this tick, so a slow or hung judge cannot delay
//     other votes, upgrade voting or backlog telemetry.
//   - The built-in checks are always evaluated fresh. The gate can only narrow
//     their outcome (reject, or hold for review) — never turn a built-in
//     rejection into an accept.
//   - The semantic verdict is cached per judge version (model set, policy and
//     check wording). A version change re-judges pending memories. An
//     operator's review decision resolves only the semantic question, and is
//     final for that memory across versions: a human decision outranks a later
//     judge.
//   - A judge failure holds the memory for review. It never bypasses a
//     configured judge or turns unavailable evidence into absent evidence.
//   - Everything is a per-node opinion. Judges are not deterministic across
//     nodes, which is why the gate lives in the voter and never in the state
//     machine.
type Gate struct {
	// Judges are asked concurrently; Policy says how their answers combine.
	Judges []LastingJudge
	// SupportJudges, when set, are also asked — concurrently with Judges —
	// whether a memory is supported by the evidence submitted with it. Only
	// memories that HAVE evidence (see EvidenceStore) are asked; a memory
	// without evidence is judged exactly as before. Their answers always
	// combine as PolicyAll: every judge must reach ActAt for the memory to
	// pass, whatever Policy says (see docs/reference/write-gate.md).
	SupportJudges []SupportJudge
	// Policy: PolicyLead (default) — the FIRST judge leads; a memory passes
	// when the lead is at or above ActAt and no other judge is below
	// RejectBelow, and fails only when every judge is below RejectBelow.
	// PolicyAll — every judge must reach ActAt to pass.
	Policy string
	// ActAt is the pass threshold (default 0.9); RejectBelow the fail
	// threshold (default 0.5). Between them the gate holds the memory for
	// operator review.
	ActAt       float64
	RejectBelow float64
	// IncludeDomainPrefixes, when set, limits the gate — and therefore what
	// memory content is sent to the judges — to these domains. Empty means
	// every domain not exempted.
	IncludeDomainPrefixes []string
	// ExemptDomainPrefixes are never judged (e.g. program-written catalogs);
	// the built-in checks apply as before and no content is sent.
	ExemptDomainPrefixes []string
	// Version identifies the judge setup; verdicts are cached under it.
	Version string
	// Timeout bounds the judge calls for one memory (default 60s).
	Timeout time.Duration
	// Workers is the number of concurrent evaluations (default 2) and
	// QueueSize the bound on memories waiting for one (default 64).
	Workers   int
	QueueSize int
	// RetryAfterFailure bounds repeated failed evaluations when a hold could
	// not be persisted (default 10m). The memory remains held during backoff.
	RetryAfterFailure time.Duration

	once     sync.Once
	queue    chan string
	mu       sync.Mutex
	inflight map[string]bool
	failed   map[string]time.Time
}

// LastingJudge is a provider-neutral judge: the probability that content
// states lasting knowledge (a fact about the world, or a standing rule or
// method) rather than a remark about the conversation it came from.
type LastingJudge interface {
	LastingProbability(ctx context.Context, content string) (float64, error)
}

// SupportJudge is a provider-neutral evidence judge: the probability that
// evidence supports what content asserts, as stated (time frame and certainty
// included).
type SupportJudge interface {
	SupportedProbability(ctx context.Context, content, evidence string) (float64, error)
}

// EvidenceStore is optionally implemented by a GateStore that keeps the
// evidence submitted with memories. Without it the evidence check never runs.
type EvidenceStore interface {
	// JudgeableEvidence returns the evidence submitted with a memory in
	// plaintext, and ok=false when none was submitted. Like JudgeableContent
	// it must FAIL — never return ok=false or ciphertext — when evidence exists
	// but cannot be decrypted, and it returns memory.ErrEvidenceExpired —
	// never ok=false — when evidence was submitted but has since been deleted.
	JudgeableEvidence(ctx context.Context, memoryID string) (evidence string, ok bool, err error)
}

// evidencePruner is optionally implemented by an EvidenceStore; the gate runs
// it periodically so evidence of decided memories does not accumulate.
type evidencePruner interface {
	PruneMemoryEvidence(ctx context.Context, now time.Time) error
}

// evidencePruneInterval is how often the gate prunes evidence.
const evidencePruneInterval = 10 * time.Minute

// GateStore is what the gate needs from the node's store beyond Store.
type GateStore interface {
	// JudgeableContent returns a memory's plaintext for the judges. It must
	// FAIL — never return stored ciphertext — when the content cannot be
	// decrypted (e.g. the vault is locked), so nothing unreadable ever leaves
	// the node.
	JudgeableContent(ctx context.Context, memoryID string) (string, error)
	// SemanticVerdict returns the verdict recorded for memoryID under version.
	SemanticVerdict(ctx context.Context, memoryID, version string) (memory.SemanticVerdict, bool, error)
	RecordSemanticVerdict(ctx context.Context, memoryID, version string, v memory.SemanticVerdict) error
	// ReviewDecision returns an operator's accept/reject for a held memory.
	ReviewDecision(ctx context.Context, memoryID string) (decision string, ok bool, err error)
}

// Judge-combination policies (see Gate.Policy).
const (
	PolicyLead = "lead"
	PolicyAll  = "all"
)

func (g *Gate) init() {
	g.once.Do(func() {
		if g.ActAt <= 0 || g.ActAt > 1 {
			g.ActAt = 0.9
		}
		if g.RejectBelow <= 0 || g.RejectBelow >= g.ActAt {
			g.RejectBelow = 0.5
		}
		if g.Timeout <= 0 {
			g.Timeout = 60 * time.Second
		}
		if g.Policy != PolicyAll {
			g.Policy = PolicyLead
		}
		if g.Workers <= 0 {
			g.Workers = 2
		}
		if g.QueueSize <= 0 {
			g.QueueSize = 64
		}
		if g.RetryAfterFailure <= 0 {
			g.RetryAfterFailure = 10 * time.Minute
		}
		g.queue = make(chan string, g.QueueSize)
		g.inflight = map[string]bool{}
		g.failed = map[string]time.Time{}
	})
}

// InScope reports whether memories in domain are judged (and so whether their
// content is sent to the judges).
func (g *Gate) InScope(domain string) bool {
	for _, p := range g.ExemptDomainPrefixes {
		if p != "" && strings.HasPrefix(domain, p) {
			return false
		}
	}
	if len(g.IncludeDomainPrefixes) == 0 {
		return true
	}
	for _, p := range g.IncludeDomainPrefixes {
		if p != "" && strings.HasPrefix(domain, p) {
			return true
		}
	}
	return false
}

// Start runs the background evaluator until ctx ends. The voter calls it once.
func (g *Gate) Start(ctx context.Context, gs GateStore, logger zerolog.Logger) {
	g.init()
	if p, ok := gs.(evidencePruner); ok {
		go func() {
			t := time.NewTicker(evidencePruneInterval)
			defer t.Stop()
			for {
				if err := p.PruneMemoryEvidence(ctx, time.Now()); err != nil && ctx.Err() == nil {
					logger.Warn().Err(err).Msg("memory gate could not prune submission evidence")
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	for i := 0; i < g.Workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case id := <-g.queue:
					g.evaluate(ctx, gs, id, logger)
				}
			}
		}()
	}
}

// enqueue asks for an evaluation without blocking. A full queue is not an
// error: the memory is offered again on a later tick.
func (g *Gate) enqueue(memoryID string) {
	g.mu.Lock()
	if g.inflight[memoryID] {
		g.mu.Unlock()
		return
	}
	g.inflight[memoryID] = true
	g.mu.Unlock()
	select {
	case g.queue <- memoryID:
	default:
		g.mu.Lock()
		delete(g.inflight, memoryID)
		g.mu.Unlock()
	}
}

func (g *Gate) recentlyFailed(memoryID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	at, ok := g.failed[memoryID]
	if !ok {
		return false
	}
	if time.Since(at) > g.RetryAfterFailure {
		delete(g.failed, memoryID)
		return false
	}
	return true
}

// evaluate judges one memory and stores a verdict or an unavailable hold.
func (g *Gate) evaluate(ctx context.Context, gs GateStore, memoryID string, logger zerolog.Logger) {
	defer func() {
		g.mu.Lock()
		delete(g.inflight, memoryID)
		g.mu.Unlock()
	}()
	fail := func(err error) {
		g.mu.Lock()
		g.failed[memoryID] = time.Now()
		g.mu.Unlock()
		logger.Warn().Err(err).Str("memory_id", memoryID).
			Msg("memory gate could not judge this memory — held for review")
		if ctx.Err() == nil {
			v := memory.SemanticVerdict{Verdict: memory.VerdictAbstain, Reason: "held for review: judge unavailable"}
			if rerr := gs.RecordSemanticVerdict(ctx, memoryID, g.Version, v); rerr != nil {
				logger.Warn().Err(rerr).Str("memory_id", memoryID).Msg("memory gate could not persist unavailable hold")
			}
		}
	}
	content, err := gs.JudgeableContent(ctx, memoryID)
	if err != nil {
		fail(err)
		return
	}
	var evidence string
	hasEvidence := false
	if es, ok := gs.(EvidenceStore); ok && len(g.SupportJudges) > 0 {
		evidence, hasEvidence, err = es.JudgeableEvidence(ctx, memoryID)
		if errors.Is(err, memory.ErrEvidenceExpired) {
			// Keep an explicit expired-evidence hold rather than taking the
			// no-evidence path: a human must decide this memory.
			v := memory.SemanticVerdict{Verdict: memory.VerdictAbstain,
				Reason: "held for review: " + memory.ErrEvidenceExpired.Error() + ", so its support cannot be judged"}
			if rerr := gs.RecordSemanticVerdict(ctx, memoryID, g.Version, v); rerr != nil {
				fail(rerr)
			}
			return
		}
		if err != nil {
			fail(err)
			return
		}
	}
	jctx, cancel := context.WithTimeout(ctx, g.Timeout)
	defer cancel()
	ps := make([]float64, len(g.Judges))
	errs := make([]error, len(g.Judges))
	var sps []float64
	var serrs []error
	if hasEvidence {
		sps = make([]float64, len(g.SupportJudges))
		serrs = make([]error, len(g.SupportJudges))
	}
	var wg sync.WaitGroup
	for i, j := range g.Judges {
		wg.Add(1)
		go func(i int, j LastingJudge) {
			defer wg.Done()
			ps[i], errs[i] = j.LastingProbability(jctx, content)
		}(i, j)
	}
	if hasEvidence {
		for i, j := range g.SupportJudges {
			wg.Add(1)
			go func(i int, j SupportJudge) {
				defer wg.Done()
				sps[i], serrs[i] = j.SupportedProbability(jctx, content, evidence)
			}(i, j)
		}
	}
	wg.Wait()
	for _, e := range append(errs, serrs...) {
		if e != nil {
			fail(e)
			return
		}
	}
	v := g.combine(ps)
	if hasEvidence {
		v = mergeVerdicts(v, g.combineSupport(sps))
	}
	if err := gs.RecordSemanticVerdict(ctx, memoryID, g.Version, v); err != nil {
		fail(err)
	}
}

// combine applies the policy to the judges' probabilities.
func (g *Gate) combine(ps []float64) memory.SemanticVerdict {
	lead, lo, hi := ps[0], 1.0, 0.0
	for _, p := range ps {
		lo, hi = min(lo, p), max(hi, p)
	}
	pass := lead >= g.ActAt && lo >= g.RejectBelow
	p := lead
	if g.Policy == PolicyAll {
		pass, p = lo >= g.ActAt, lo
	}
	switch {
	case pass:
		return memory.SemanticVerdict{Verdict: memory.VerdictPass, P: p, Reason: fmt.Sprintf("lasting p=%.2f", p)}
	case hi < g.RejectBelow:
		return memory.SemanticVerdict{Verdict: memory.VerdictReject, P: p,
			Reason: fmt.Sprintf("a remark about its own session, not lasting memory (p=%.2f)", p)}
	default:
		return memory.SemanticVerdict{Verdict: memory.VerdictAbstain, P: p,
			Reason: fmt.Sprintf("held for review: lasting-memory uncertain (p=%.2f)", p)}
	}
}

// combineSupport combines the evidence judges' probabilities. It always
// requires every judge: on held-out tests the lead-with-veto policy let
// claims about an earlier time or claims only reported by someone pass as
// supported, and requiring agreement removed them.
func (g *Gate) combineSupport(ps []float64) memory.SemanticVerdict {
	lo, hi := 1.0, 0.0
	for _, p := range ps {
		lo, hi = min(lo, p), max(hi, p)
	}
	switch {
	case lo >= g.ActAt:
		return memory.SemanticVerdict{Verdict: memory.VerdictPass, P: lo, Reason: fmt.Sprintf("supported by its evidence p=%.2f", lo)}
	case hi < g.RejectBelow:
		return memory.SemanticVerdict{Verdict: memory.VerdictReject, P: hi,
			Reason: fmt.Sprintf("not supported by the evidence submitted with it (p=%.2f)", hi)}
	default:
		return memory.SemanticVerdict{Verdict: memory.VerdictAbstain, P: lo,
			Reason: fmt.Sprintf("held for review: support by its evidence uncertain (p=%.2f)", lo)}
	}
}

// mergeVerdicts combines the lasting and evidence verdicts for one memory: it
// passes only when both pass, is rejected when either is, and is otherwise
// held for review.
func mergeVerdicts(lasting, support memory.SemanticVerdict) memory.SemanticVerdict {
	switch {
	case lasting.Verdict == memory.VerdictReject:
		return lasting
	case support.Verdict == memory.VerdictReject:
		return support
	case lasting.Verdict == memory.VerdictPass && support.Verdict == memory.VerdictPass:
		return memory.SemanticVerdict{Verdict: memory.VerdictPass, P: min(lasting.P, support.P),
			Reason: lasting.Reason + "; " + support.Reason}
	case lasting.Verdict == memory.VerdictAbstain:
		return lasting
	default:
		return support
	}
}

// gateOutcome is what the voter does with one memory this tick.
type gateOutcome int

const (
	gateUseBaseline gateOutcome = iota // vote the built-in decision
	gateOverride                       // vote the returned decision
	gateHold                           // do not vote this tick
)

// Apply combines the FRESH built-in decision with the stored semantic verdict.
// It never blocks on a judge: a memory with no verdict is queued for the
// background evaluator and held.
func (g *Gate) Apply(ctx context.Context, gs GateStore, mem *memory.MemoryRecord, baseline Decision, logger zerolog.Logger) (gateOutcome, Decision) {
	g.init()
	if !baseline.Accept || !g.InScope(mem.DomainTag) {
		return gateUseBaseline, baseline
	}
	if decision, ok, err := gs.ReviewDecision(ctx, mem.MemoryID); err != nil {
		logger.Warn().Err(err).Str("memory_id", mem.MemoryID).Msg("memory gate review lookup failed — held for review")
		return gateHold, Decision{}
	} else if ok {
		if decision == memory.VerdictAccept {
			return gateOverride, Decision{Accept: true, Reason: baseline.Reason + "; semantic check resolved by operator review"}
		}
		return gateOverride, Decision{Accept: false, Reason: "rejected by operator review"}
	}
	v, ok, err := gs.SemanticVerdict(ctx, mem.MemoryID, g.Version)
	if err != nil {
		logger.Warn().Err(err).Str("memory_id", mem.MemoryID).Msg("memory gate verdict lookup failed — held for review")
		return gateHold, Decision{}
	}
	if !ok {
		if g.recentlyFailed(mem.MemoryID) {
			return gateHold, Decision{}
		}
		g.enqueue(mem.MemoryID)
		return gateHold, Decision{}
	}
	switch v.Verdict {
	case memory.VerdictPass:
		return gateOverride, Decision{Accept: true, Reason: baseline.Reason + "; " + v.Reason}
	case memory.VerdictReject:
		return gateOverride, Decision{Accept: false, Reason: v.Reason}
	default:
		return gateHold, Decision{}
	}
}
