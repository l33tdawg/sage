package voter

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/tx"
)

// maxVotedTracked caps the in-memory set of memory IDs already voted on this
// session, so a long-running voter's footprint stays flat. Resetting only causes
// at most one idempotent re-vote per still-proposed memory.
const maxVotedTracked = 100_000

const (
	memoryVotePageSize            = 128
	memoryVoteScanBudget          = 1024
	memoryVotesPerTick            = 20
	memoryVoteRetryAfter          = 10 * time.Second
	memoryVoteAdmissionRetryAfter = 30 * time.Second
)

// Config configures a per-node memory auto-voter.
type Config struct {
	// Key is the node's consensus signing key (priv_validator_key.json). The voter
	// signs MemoryVote / GovVote txs with it; the derived signer ID (hex of the
	// public key) must be a member of the on-chain validator set for votes to
	// count — which it is, because the genesis validator set is keyed by exactly
	// this identity.
	Key ed25519.PrivateKey
	// CometRPC is the CometBFT RPC endpoint used to broadcast signed vote txs.
	CometRPC string
	// PollInterval is how often pending memories are scanned (default 2s).
	PollInterval time.Duration
	// Health, when non-nil, receives a VoterStatus snapshot every poll tick so
	// the node's /ready endpoint can surface voter liveness and the proposed
	// backlog (sage-gui wires this). Optional and nil-safe: amid has no local
	// health server and leaves it nil — the Prometheus gauges publish either way.
	Health *metrics.HealthChecker
}

// App is the slice of *abci.SageApp the voter needs for the upgrade-proposal arm.
// Declared as an interface so the voter package never imports internal/abci (no
// import cycle) and can be faked in tests.
type App interface {
	// ActiveUpgradeVote reports an in-flight app-version upgrade proposal this node
	// should weigh in on: its ID, target app version, whether THIS binary supports
	// that version, and whether a proposal is active at all.
	ActiveUpgradeVote() (proposalID string, targetVersion uint64, supported, ok bool)
	// UpgradeProposalHasVote reports whether voterID already has an on-chain vote
	// recorded for the proposal (so the voter doesn't re-broadcast).
	UpgradeProposalHasVote(proposalID, voterID string) bool
	// MemoryVoteTargetState binds app-v25 voting to canonical memory state.
	// eligible is true only when this ID may currently receive a vote; recorded
	// is true only after this validator's vote is visible in committed state.
	MemoryVoteTargetState(memoryID, voterID string) (eligible, recorded bool)
}

// PendingSource yields proposed memories awaiting votes.
type PendingSource interface {
	GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error)
}

type PendingPageSource interface {
	GetPendingByDomainPage(
		ctx context.Context,
		domainTag string,
		limit, offset int,
	) ([]*memory.MemoryRecord, error)
}

type VotablePendingPageSource interface {
	GetVotablePendingByDomainPage(
		ctx context.Context,
		domainTag string,
		limit, offset int,
	) ([]*memory.MemoryRecord, error)
}

// BacklogSource exposes the proposed-backlog watermark behind the stuck-memory
// telemetry (sage_proposed_oldest_age_seconds / sage_proposed_pending_count).
// Both real stores (SQLite/Postgres) implement it via store.MemoryStore.
type BacklogSource interface {
	// OldestProposedCreatedAt returns the created_at of the oldest memory still
	// in status='proposed' (ok=false when nothing is pending).
	OldestProposedCreatedAt(ctx context.Context) (time.Time, bool, error)
	// ProposedPendingCount returns how many memories are in status='proposed'.
	ProposedPendingCount(ctx context.Context) (int, error)
}

// Store is the memory store the voter reads from: pending work + dedup lookups
// + backlog telemetry.
type Store interface {
	PendingSource
	DupChecker
	BacklogSource
}

// Run is the voter loop. It blocks until ctx is cancelled. Every tick it:
//  1. votes on each newly-seen proposed memory (one vote, signed with the node's
//     consensus key), and
//  2. auto-votes ACCEPT on an active, supported app-version upgrade proposal.
//
// Determinism note: per-node votes need NOT agree across nodes — nodes may
// legitimately disagree, and checkAndApplyQuorum resolves the outcome
// deterministically from committed on-chain state. The voter writes NO consensus
// state directly; its only effect is the broadcast vote tx, which flows through
// normal consensus.
func Run(ctx context.Context, app App, store Store, cfg Config, logger zerolog.Logger) {
	if len(cfg.Key) != ed25519.PrivateKeySize {
		logger.Error().Msg("memory auto-voter not started: invalid consensus key")
		return
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	selfID := hex.EncodeToString(cfg.Key.Public().(ed25519.PublicKey))

	// Liveness signal: sage_voter_running=1 for the lifetime of this loop, 0 on
	// every exit path (and 0 forever on nodes where Run never got this far).
	// The health block mirrors it so /ready flips running=false on shutdown too.
	metrics.SetVoterRunning(true)
	defer metrics.SetVoterRunning(false)
	if cfg.Health != nil {
		cfg.Health.SetVoterStatus(metrics.VoterStatus{Running: true, ValidatorID: selfID})
		defer cfg.Health.SetVoterStatus(metrics.VoterStatus{Running: false, ValidatorID: selfID})
	}

	logger.Info().
		Str("validator", selfID[:16]).
		Dur("interval", cfg.PollInterval).
		Msg("memory auto-voter started — one node, one vote (signing with node consensus key)")

	// Memories already voted on this session — avoids re-broadcasting every tick.
	// Entries are added only after committed canonical state confirms the vote.
	voted := make(map[string]bool)
	// A CheckTx-accepted broadcast is merely in flight. Retry it after a bounded
	// interval unless committed state confirms it; never confuse admission with
	// a durable vote.
	inflight := make(map[string]time.Time)
	// The raw SQL backlog can contain legacy rows which app-v25 correctly refuses
	// to vote. Carry a stable page offset across ticks so a large ghost prefix
	// cannot starve newer canonical submissions or make one tick unbounded.
	voteScanOffset := 0
	// Upgrade proposals we've already warned are unsupported, so the tick doesn't
	// re-log the same warning. (Supported proposals are NOT suppressed here — the
	// on-chain UpgradeProposalHasVote check self-heals a dropped broadcast.)
	warnedProposals := make(map[string]bool)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// lastVote is when this session last broadcast a memory vote tx (zero =
	// never). Surfaced via /ready's voter block as last_vote_unix.
	var lastVote time.Time
	var voteRetryAt time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Bound the dedup set: once we've tracked a lot of memories this
			// session, drop it. A re-vote on a still-proposed memory is idempotent
			// (the engine rejects duplicate votes), so resetting is safe and keeps
			// memory flat over a long-running node.
			if len(voted) > maxVotedTracked {
				voted = make(map[string]bool)
			}
			if len(inflight) > maxVotedTracked {
				inflight = make(map[string]time.Time)
			}
			if time.Now().Before(voteRetryAt) {
				publishBacklogTelemetry(ctx, store, cfg.Health, selfID, lastVote)
				continue
			}
			cast, unavailable := voteOnPendingMemoriesResult(
				ctx, app, store, cfg, voted, inflight, &voteScanOffset, logger,
			)
			if cast > 0 {
				lastVote = time.Now()
			}
			if !unavailable {
				unavailable = voteOnUpgradeProposalResult(
					ctx, app, cfg, selfID, warnedProposals, logger,
				)
			}
			if unavailable {
				voteRetryAt = time.Now().Add(memoryVoteAdmissionRetryAfter)
				logger.Warn().
					Dur("retry_after", memoryVoteAdmissionRetryAfter).
					Msg("memory auto-voter paused because consensus admission is unavailable")
			}
			publishBacklogTelemetry(ctx, store, cfg.Health, selfID, lastVote)
		}
	}
}

// publishBacklogTelemetry refreshes the stuck-memory alarm pair
// (sage_proposed_oldest_age_seconds / sage_proposed_pending_count) and, when a
// health checker is wired (sage-gui; amid leaves it nil), mirrors the same
// snapshot into /ready's "voter" block. NODE-LOCAL: both numbers come from
// THIS node's off-chain store, so on a multi-node chain every node reports its
// own view of the shared backlog. Observability only — no consensus state, no
// tx, no AppHash impact.
func publishBacklogTelemetry(ctx context.Context, store Store, health *metrics.HealthChecker, selfID string, lastVote time.Time) {
	oldest, ok, err := store.OldestProposedCreatedAt(ctx)
	if err != nil {
		return // transient store error — keep the previous gauge values
	}
	pending, err := store.ProposedPendingCount(ctx)
	if err != nil {
		return
	}
	var age float64
	if ok {
		if age = time.Since(oldest).Seconds(); age < 0 {
			age = 0 // clock skew guard — never publish a negative age
		}
	}
	metrics.SetProposedBacklog(age, pending)
	if health != nil {
		var lastVoteUnix int64
		if !lastVote.IsZero() {
			lastVoteUnix = lastVote.Unix()
		}
		health.SetVoterStatus(metrics.VoterStatus{
			Running:                  true,
			ValidatorID:              selfID,
			LastVoteUnix:             lastVoteUnix,
			OldestProposedAgeSeconds: age,
			PendingProposed:          pending,
		})
	}
}

// voteOnPendingMemories scans proposed memories and casts one signed vote per
// newly-seen memory. Returns how many vote txs were broadcast this tick (feeds
// the /ready voter block's last_vote_unix).
func voteOnPendingMemories(
	ctx context.Context,
	app App,
	store Store,
	cfg Config,
	voted map[string]bool,
	inflight map[string]time.Time,
	scanOffset *int,
	logger zerolog.Logger,
) int {
	cast, _ := voteOnPendingMemoriesResult(
		ctx, app, store, cfg, voted, inflight, scanOffset, logger,
	)
	return cast
}

func voteOnPendingMemoriesResult(
	ctx context.Context,
	app App,
	store Store,
	cfg Config,
	voted map[string]bool,
	inflight map[string]time.Time,
	scanOffset *int,
	logger zerolog.Logger,
) (int, bool) {
	if scanOffset == nil {
		return 0, false
	}
	selfID := hex.EncodeToString(cfg.Key.Public().(ed25519.PublicKey))
	cast := 0
	scanned := 0
	offset := *scanOffset
	wrapped := false
	now := time.Now()
	pager, paged := store.(PendingPageSource)
	votablePager, recoveryAware := store.(VotablePendingPageSource)
	for scanned < memoryVoteScanBudget && cast < memoryVotesPerTick {
		pageLimit := memoryVotePageSize
		if remaining := memoryVoteScanBudget - scanned; remaining < pageLimit {
			pageLimit = remaining
		}
		var (
			pending []*memory.MemoryRecord
			err     error
		)
		if recoveryAware {
			pending, err = votablePager.GetVotablePendingByDomainPage(
				ctx, "%", pageLimit, offset,
			)
		} else if paged {
			pending, err = pager.GetPendingByDomainPage(ctx, "%", pageLimit, offset)
		} else {
			// Compatibility lane for third-party stores implementing the original
			// voter interface. Built-in SQLite/Postgres always take the paged
			// branch, which is required to pass legacy ghost prefixes.
			if offset > 0 {
				offset = 0
				break
			}
			pending, err = store.GetPendingByDomain(ctx, "%", pageLimit)
		}
		if err != nil {
			break
		}
		if len(pending) == 0 {
			if offset > 0 && !wrapped {
				offset = 0
				wrapped = true
				continue
			}
			break
		}
		for _, mem := range pending {
			offset++
			scanned++
			eligible, recorded := app.MemoryVoteTargetState(mem.MemoryID, selfID)
			if recorded {
				voted[mem.MemoryID] = true
				delete(inflight, mem.MemoryID)
				continue
			}
			if !eligible {
				delete(inflight, mem.MemoryID)
				continue
			}
			// Committed state is authoritative. Adoption deliberately clears
			// stale pre-envelope vote receipts; discard the matching process
			// cache so the newly canonical proposed memory can receive a vote.
			delete(voted, mem.MemoryID)
			if sentAt, ok := inflight[mem.MemoryID]; ok &&
				now.Sub(sentAt) < memoryVoteRetryAfter {
				continue
			}
			contentHash := hex.EncodeToString(mem.ContentHash)
			decision := Decide(ctx, store, MemoryInput{
				Content:     mem.Content,
				ContentHash: contentHash,
				Domain:      mem.DomainTag,
				MemType:     string(mem.MemoryType),
				Confidence:  mem.ConfidenceScore,
			})
			decStr := "reject"
			if decision.Accept {
				decStr = "accept"
			}

			var result voteBroadcastResult
			err = tx.WithNonceLease(ctx, cfg.Key, func(nonce uint64) error {
				voteTx := &tx.ParsedTx{
					Type:      tx.TxTypeMemoryVote,
					Nonce:     nonce,
					Timestamp: time.Now(),
					MemoryVote: &tx.MemoryVote{
						MemoryID:  mem.MemoryID,
						Decision:  voteDecisionFromString(decStr),
						Rationale: decision.Reason,
					},
				}
				if signErr := tx.SignTx(voteTx, cfg.Key); signErr != nil {
					return signErr
				}
				encoded, encodeErr := tx.EncodeTx(voteTx)
				if encodeErr != nil {
					return encodeErr
				}
				result = broadcastVoteTx(ctx, cfg.CometRPC, encoded, logger)
				return nil
			})
			if err != nil {
				logger.Debug().Err(err).Msg("failed to build or broadcast vote tx under nonce lease")
				if ctx.Err() != nil {
					*scanOffset = 0
					return cast, true
				}
				continue
			}
			if result.unavailable {
				*scanOffset = 0
				return cast, true
			}
			if result.accepted {
				inflight[mem.MemoryID] = now
				cast++
			}
			if cast == memoryVotesPerTick {
				break
			}
		}
		if len(pending) < pageLimit {
			offset = 0
			break
		}
	}
	*scanOffset = offset
	return cast, false
}

// voteOnUpgradeProposal auto-votes ACCEPT on an active app-version upgrade
// proposal, but only if THIS binary supports the target (the readiness gate that
// keeps an unsupported upgrade from drawing the node toward a quorum it cannot
// execute). Under the per-node model the node IS the validator and self-votes only
// when ready — strictly safer than the old 4-archetype abstention scheme — and the
// multi-node 2/3 governance quorum still binds the outcome.
func voteOnUpgradeProposal(ctx context.Context, app App, cfg Config, selfID string, warnedProposals map[string]bool, logger zerolog.Logger) {
	_ = voteOnUpgradeProposalResult(ctx, app, cfg, selfID, warnedProposals, logger)
}

func voteOnUpgradeProposalResult(ctx context.Context, app App, cfg Config, selfID string, warnedProposals map[string]bool, logger zerolog.Logger) bool {
	proposalID, target, supported, ok := app.ActiveUpgradeVote()
	if !ok {
		return false
	}
	if !supported {
		if !warnedProposals[proposalID] {
			logger.Warn().
				Str("proposal_id", proposalID).
				Uint64("target_app_version", target).
				Msg("active upgrade proposal targets an app version this binary does not support — NOT auto-voting; upgrade this binary to participate")
			warnedProposals[proposalID] = true
		}
		return false
	}
	if app.UpgradeProposalHasVote(proposalID, selfID) {
		return false // already recorded on-chain — don't re-broadcast
	}

	logger.Info().
		Str("proposal_id", proposalID).
		Uint64("target_app_version", target).
		Msg("auto-voting ACCEPT on supported upgrade proposal")
	var result voteBroadcastResult
	err := tx.WithNonceLease(ctx, cfg.Key, func(nonce uint64) error {
		voteTx := &tx.ParsedTx{
			Type:      tx.TxTypeGovVote,
			Nonce:     nonce,
			Timestamp: time.Now(),
			GovVote: &tx.GovVote{
				ProposalID: proposalID,
				Decision:   tx.VoteDecisionAccept,
			},
		}
		if signErr := tx.SignTx(voteTx, cfg.Key); signErr != nil {
			return signErr
		}
		encoded, encodeErr := tx.EncodeTx(voteTx)
		if encodeErr != nil {
			return encodeErr
		}
		result = broadcastVoteTx(ctx, cfg.CometRPC, encoded, logger)
		return nil
	})
	if err != nil {
		logger.Debug().Err(err).Msg("failed to build or broadcast gov vote tx under nonce lease")
		return ctx.Err() != nil
	}
	return result.unavailable
}
