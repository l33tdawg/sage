package voter

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// maxVotedTracked caps the in-memory set of memory IDs already voted on this
// session, so a long-running voter's footprint stays flat. Resetting only causes
// at most one idempotent re-vote per still-proposed memory.
const maxVotedTracked = 100_000

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
}

// PendingSource yields proposed memories awaiting votes.
type PendingSource interface {
	GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error)
}

// Store is the memory store the voter reads from: pending work + dedup lookups.
type Store interface {
	PendingSource
	DupChecker
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

	logger.Info().
		Str("validator", selfID[:16]).
		Dur("interval", cfg.PollInterval).
		Msg("memory auto-voter started — one node, one vote (signing with node consensus key)")

	// Memories already voted on this session — avoids re-broadcasting every tick.
	// A stuck memory (e.g. a 2-2 tie on a multi-node chain) is left alone rather
	// than reflooded.
	voted := make(map[string]bool)
	// Upgrade proposals we've already warned are unsupported, so the tick doesn't
	// re-log the same warning. (Supported proposals are NOT suppressed here — the
	// on-chain UpgradeProposalHasVote check self-heals a dropped broadcast.)
	warnedProposals := make(map[string]bool)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

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
			voteOnPendingMemories(ctx, store, cfg, voted, logger)
			voteOnUpgradeProposal(ctx, app, cfg, selfID, warnedProposals, logger)
		}
	}
}

// voteOnPendingMemories scans proposed memories and casts one signed vote per
// newly-seen memory.
func voteOnPendingMemories(ctx context.Context, store Store, cfg Config, voted map[string]bool, logger zerolog.Logger) {
	pending, err := store.GetPendingByDomain(ctx, "%", 20)
	if err != nil {
		return
	}
	for _, mem := range pending {
		if voted[mem.MemoryID] {
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

		voteTx := &tx.ParsedTx{
			Type:      tx.TxTypeMemoryVote,
			Nonce:     tx.MonotonicNonce(cfg.Key), // strictly increasing per key (app-v9 nonce gate)
			Timestamp: time.Now(),
			MemoryVote: &tx.MemoryVote{
				MemoryID:  mem.MemoryID,
				Decision:  voteDecisionFromString(decStr),
				Rationale: decision.Reason,
			},
		}
		if err := tx.SignTx(voteTx, cfg.Key); err != nil {
			logger.Debug().Err(err).Msg("failed to sign vote tx")
			continue
		}
		encoded, err := tx.EncodeTx(voteTx)
		if err != nil {
			logger.Debug().Err(err).Msg("failed to encode vote tx")
			continue
		}
		broadcastVoteTx(ctx, cfg.CometRPC, encoded, logger)
		voted[mem.MemoryID] = true
	}
}

// voteOnUpgradeProposal auto-votes ACCEPT on an active app-version upgrade
// proposal, but only if THIS binary supports the target (the readiness gate that
// keeps an unsupported upgrade from drawing the node toward a quorum it cannot
// execute). Under the per-node model the node IS the validator and self-votes only
// when ready — strictly safer than the old 4-archetype abstention scheme — and the
// multi-node 2/3 governance quorum still binds the outcome.
func voteOnUpgradeProposal(ctx context.Context, app App, cfg Config, selfID string, warnedProposals map[string]bool, logger zerolog.Logger) {
	proposalID, target, supported, ok := app.ActiveUpgradeVote()
	if !ok {
		return
	}
	if !supported {
		if !warnedProposals[proposalID] {
			logger.Warn().
				Str("proposal_id", proposalID).
				Uint64("target_app_version", target).
				Msg("active upgrade proposal targets an app version this binary does not support — NOT auto-voting; upgrade this binary to participate")
			warnedProposals[proposalID] = true
		}
		return
	}
	if app.UpgradeProposalHasVote(proposalID, selfID) {
		return // already recorded on-chain — don't re-broadcast
	}

	voteTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovVote,
		Nonce:     tx.MonotonicNonce(cfg.Key),
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   tx.VoteDecisionAccept,
		},
	}
	if err := tx.SignTx(voteTx, cfg.Key); err != nil {
		logger.Debug().Err(err).Msg("failed to sign gov vote tx")
		return
	}
	encoded, err := tx.EncodeTx(voteTx)
	if err != nil {
		logger.Debug().Err(err).Msg("failed to encode gov vote tx")
		return
	}
	logger.Info().
		Str("proposal_id", proposalID).
		Uint64("target_app_version", target).
		Msg("auto-voting ACCEPT on supported upgrade proposal")
	broadcastVoteTx(ctx, cfg.CometRPC, encoded, logger)
}
