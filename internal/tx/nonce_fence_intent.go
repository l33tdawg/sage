package tx

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// The fence is in-process state, and that is its one documented hole: a
// restart, a crash or a SIGKILL discards it, the allocator re-seeds each key
// from the highest COMMITTED on-chain nonce — which is below the abandoned one
// by definition, because "unresolved" is what unresolved means — and the next
// action signs into that gap. The abandoned transaction is then refused Code 4
// when it finally lands, and the loss is untraceable: an operator sees an
// unrelated later action fail as a replay.
//
// Durable intent closes that hole from the safe side. The record written here
// says only "this exact transaction, for this key, with this nonce, was handed
// to the transport and its fate is unproven". It does NOT carry a verdict, and
// it is deleted only when a fate is proven: on a committed submit, on a
// hash-bound definitive rejection, or on a fence lift. A node that stops
// answering with one of these on disk raises the fence again at startup instead
// of re-seeding past it.
//
// WHY THE SIGNED BYTES ARE NOT STORED. Storing them would make a restored fence
// resolvable by re-submission, not just by an operator proof. It would also copy
// the full transaction into a local table on every submission, and those bytes
// routinely carry memory content that SAGE is required to keep out of plaintext
// stores (`require_encrypted_storage`, vault-required private media). Trading a
// content-leak regression for fence liveness is not a trade this file may make
// on its own, so the durable record carries identity and not payload, and the
// notes on restore state exactly what that costs: the fence survives, the
// payload may not.

// FenceIntent is the durable shadow of one submission whose fate is unproven.
type FenceIntent struct {
	// SignerPubKeyHex is hex(ed25519 public key) — the agent id used everywhere
	// else in SAGE, and the key FencedSigners reports.
	SignerPubKeyHex string
	// TxHash is the CometBFT hash of the exact bytes, uppercase hex. Empty only
	// when the transaction would not encode, which is itself unprovable.
	TxHash string
	// Nonce is the allocation the key is stuck on; HasNonce is false when the
	// bytes would not decode. This is what makes the record actionable: it can be
	// compared against what the chain has committed for this signer.
	Nonce    uint64
	HasNonce bool
	// CreatedAt is when the bytes were handed to the transport.
	CreatedAt time.Time
}

// FenceIntentStore is the node-local durability hook. It is injected because
// internal/tx owns no storage, and it is deliberately small: one row per signer
// with unproven intent, no history.
//
// A store that is wired but failing does NOT block a broadcast. Refusing to
// send because a bookkeeping write failed would turn a disk hiccup into a
// signing outage, which is a worse failure than the one this closes; the write
// failure is emitted as an event instead, so the degraded guarantee is visible
// rather than silent.
type FenceIntentStore interface {
	SaveFenceIntent(ctx context.Context, intent FenceIntent) error
	DeleteFenceIntent(ctx context.Context, signerPubKeyHex string) error
	ListFenceIntents(ctx context.Context) ([]FenceIntent, error)
}

var fenceIntentStore struct {
	mu    sync.RWMutex
	store FenceIntentStore
}

// SetFenceIntentStore installs the durable-intent store. Passing nil disables
// durability and returns the fence to in-process-only behavior.
func SetFenceIntentStore(store FenceIntentStore) {
	fenceIntentStore.mu.Lock()
	fenceIntentStore.store = store
	fenceIntentStore.mu.Unlock()
}

func currentFenceIntentStore() FenceIntentStore {
	fenceIntentStore.mu.RLock()
	defer fenceIntentStore.mu.RUnlock()
	return fenceIntentStore.store
}

// recordFenceIntent persists the intent for bytes that are being handed to the
// transport. It is called from RegisterSubmittedTx, which every adopter already
// calls at exactly that boundary.
func recordFenceIntent(sk ed25519.PrivateKey, encoded []byte) {
	store := currentFenceIntentStore()
	if store == nil {
		return
	}
	pub, ok := sk.Public().(ed25519.PublicKey)
	if !ok {
		return
	}
	hash := CometTxHash(encoded)
	nonce, hasNonce := fencedTxNonce(encoded)
	intent := FenceIntent{
		// Canonical case matches FencedSigners: lowercase signer hex, uppercase
		// transaction hash. One spelling per identity, so an operator command and
		// a status row can be matched by eye and by string comparison.
		SignerPubKeyHex: hex.EncodeToString(pub),
		TxHash:          strings.ToUpper(hex.EncodeToString(hash[:])),
		Nonce:           nonce,
		HasNonce:        hasNonce,
		CreatedAt:       time.Now().UTC(),
	}
	if err := store.SaveFenceIntent(context.Background(), intent); err != nil {
		emitFenceEvent("fence_intent_write_failed",
			fenceKV("signer", signerPrefix(string(pub))),
			fenceKV("note", "the durable fence intent could not be recorded, so a restart from this moment "+
				"would NOT re-raise this fence; the broadcast still proceeds"))
	}
}

// discardFenceIntent drops the durable shadow for a key whose fate is proven.
func discardFenceIntent(key string) {
	store := currentFenceIntentStore()
	if store == nil {
		return
	}
	pub := ed25519.PublicKey(key)
	if len(pub) != ed25519.PublicKeySize {
		return
	}
	if err := store.DeleteFenceIntent(context.Background(), hex.EncodeToString(pub)); err != nil {
		emitFenceEvent("fence_intent_clear_failed",
			fenceKV("signer", signerPrefix(key)),
			fenceKV("note", "a proven fate could not retire the durable fence intent; the next restart will "+
				"re-raise this fence and refuse to sign until it is cleared"))
	}
}

// Restore fencing from durable intent at startup. It returns the number of
// fences re-raised.
//
// This is the whole point of the file: a node that was killed mid-submission
// comes back refusing to sign that key again, rather than re-seeding the
// allocator past an abandoned nonce and losing the transaction to a Code 4 that
// looks like an unrelated replay failure.
func RestoreFencesFromIntents(ctx context.Context) (int, error) {
	store := currentFenceIntentStore()
	if store == nil {
		return 0, nil
	}
	intents, err := store.ListFenceIntents(ctx)
	if err != nil {
		return 0, fmt.Errorf("list durable fence intents: %w", err)
	}
	restored := 0
	for _, intent := range intents {
		raw, decodeErr := hex.DecodeString(strings.TrimSpace(intent.SignerPubKeyHex))
		if decodeErr != nil || len(raw) != ed25519.PublicKeySize {
			emitFenceEvent("fence_restore_refused",
				fenceKV("note", "a durable fence intent names a signer that is not a valid ed25519 public key; "+
					"it cannot be turned into a fence and is left in place for an operator to inspect"))
			continue
		}
		key := string(raw)
		fenceSubmission(key, &indeterminateSubmit{
			err:              errFenceRestoredFromIntent,
			cause:            fenceCauseRestored,
			recordedTxHash:   intent.TxHash,
			recordedNonce:    intent.Nonce,
			hasRecordedNonce: intent.HasNonce,
		})
		emitFenceEvent("fence_restored",
			fenceKV("signer", signerPrefix(key)),
			fenceKV("tx_hash", intent.TxHash),
			fenceNonceField(intent.Nonce, intent.HasNonce),
			fenceKV("note", "restored from durable intent after a restart: the signed bytes did not survive "+
				"this process, so reconciliation cannot re-submit them; this fence resolves on a PROVEN fate "+
				"read from the chain (the recorded hash in a committed block, or a committed nonce at or above "+
				"the fenced allocation), or through the operator recovery routes if no proof can exist"))
		restored++
	}
	return restored, nil
}

var errFenceRestoredFromIntent = errors.New(
	"submission was in flight when the previous process ended; its fate is still unproven")

// FenceLiftProof is the evidence an operator recovery must present. It is
// deliberately a value rather than a bare command flag: the proof is recorded
// with the lift, so a recovered fence reads as a decision on evidence rather
// than as a fence that disappeared.
type FenceLiftProof struct {
	// Kind is "committed", "rejected" or "superseded". The first two mean the
	// exact transaction was found in a committed block; "superseded" means a
	// HIGHER nonce for the same signer has committed, which makes the fenced
	// nonce permanently uncommittable under the consensus nonce rule.
	Kind string
	// SignerPubKeyHex is the fence's signer, uppercase hex.
	SignerPubKeyHex string
	// Nonce is the fenced allocation the proof is about.
	Nonce    uint64
	HasNonce bool
	// TxHash is the fence's recorded transaction hash, when it had one.
	TxHash string
	// CommittedNonce is the higher committed nonce, when Kind is "superseded".
	CommittedNonce uint64
	// Detail is operator-facing context kept with the lift.
	Detail string
}

// FenceLiftUnprovenError is returned when the evidence does not prove a fate.
// It is a distinct type so an operator command can print the reason without
// pattern-matching on text.
type FenceLiftUnprovenError struct {
	Signer string
	Reason string
}

func (e *FenceLiftUnprovenError) Error() string {
	return fmt.Sprintf("fence for signer %s cannot be lifted: %s", e.Signer, e.Reason)
}

// ProveFenceLiftFromChain reads the proofs this package will accept: the exact
// transaction found in a block, or the signer's committed nonce having reached
// the fenced allocation (spent when it equals it, superseded when it is above).
//
// THE EQUALITY CASE IS A REAL PROOF, NOT A RELAXATION. Consensus refuses a
// transaction whose nonce is <= the signer's committed nonce, so once the
// committed floor has reached the fenced allocation those exact bytes can never
// commit again and no later allocation can overtake anything. That is the same
// property the re-submission path lifts on when CheckTx answers code 4 — the
// floor is checked directly here because a fence restored from durable intent
// has no bytes to re-submit. It is labelled "spent" rather than "superseded"
// because the floor alone cannot say whether these bytes were the transaction
// that committed or were overtaken; the transaction index is what distinguishes
// them, and a node whose indexer is disabled cannot answer at all.
//
// A missing transaction is NOT a proof and never becomes one here. CometBFT
// indexes a transaction only once it is in a block, so a tx sitting unindexed
// in a mempool answers exactly as a tx does one second before it commits; that
// asymmetry is the reason the fence has no timeout, and this prover does not
// reintroduce it as a lookup-miss override.
func ProveFenceLiftFromChain(
	ctx context.Context,
	cometRPC string,
	nonceFloor func(ed25519.PublicKey) (uint64, bool),
	fence FencedSigner,
) (FenceLiftProof, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(fence.SignerPubKeyHex))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return FenceLiftProof{}, &FenceLiftUnprovenError{
			Signer: fence.SignerPubKeyPrefix,
			Reason: "the fence does not name a valid ed25519 signer, so no proof can be read for it",
		}
	}
	pub := ed25519.PublicKey(raw)
	proof := FenceLiftProof{
		SignerPubKeyHex: hex.EncodeToString(pub),
		Nonce:           fence.Nonce,
		HasNonce:        fence.HasNonce,
		TxHash:          fence.TxHash,
	}

	// The committed nonce floor first: it needs no RPC and it is the proof that
	// survives a restart, because a restored fence has no bytes to look up by
	// content.
	if nonceFloor != nil && fence.HasNonce {
		committed, ok := nonceFloor(pub)
		if ok && committed > fence.Nonce {
			proof.Kind = "superseded"
			proof.CommittedNonce = committed
			proof.Detail = fmt.Sprintf(
				"signer has committed nonce %d, above the fenced %d; the fenced transaction can never commit "+
					"(consensus refuses a stale nonce), so the allocation is dead and its payload is lost",
				committed, fence.Nonce)
			return proof, nil
		}
		if ok && committed == fence.Nonce {
			proof.Kind = "spent"
			proof.CommittedNonce = committed
			proof.Detail = fmt.Sprintf(
				"signer's committed nonce has reached the fenced allocation %d: consensus refuses a nonce "+
					"that is not strictly above it, so these bytes can never commit again. Whether they were "+
					"the transaction that committed, or were overtaken by a different allocation of the same "+
					"nonce, is not distinguishable from the nonce floor alone",
				fence.Nonce)
			return proof, nil
		}
	}

	endpoint := strings.TrimRight(strings.TrimSpace(cometRPC), "/")
	hashText := strings.TrimSpace(fence.TxHash)
	if endpoint != "" && hashText != "" {
		var hash [32]byte
		decoded, decodeErr := hex.DecodeString(strings.TrimPrefix(strings.ToUpper(hashText), "0X"))
		if decodeErr == nil && len(decoded) == len(hash) {
			copy(hash[:], decoded)
			outcome, lookupErr := cometIndexedOutcome(ctx, endpoint, nil, hash)
			if lookupErr != nil {
				return FenceLiftProof{}, lookupErr
			}
			switch outcome.Verdict {
			case TxVerdictCommitted:
				proof.Kind = "committed"
				proof.Detail = outcome.Detail
				return proof, nil
			case TxVerdictRejected:
				proof.Kind = "rejected"
				proof.Detail = outcome.Detail
				return proof, nil
			}
		}
	}

	return FenceLiftProof{}, &FenceLiftUnprovenError{
		Signer: fence.SignerPubKeyPrefix,
		Reason: "neither proof holds yet: the transaction is not in a committed block, and no higher nonce " +
			"has committed for this signer. A missing lookup is not proof of anything — CometBFT indexes a " +
			"transaction only once it is in a block, so a mempool-resident transaction answers the same way " +
			"one second before it commits. Wait, or restore the node's own reconciliation by rebroadcasting " +
			"the identical bytes where the adopter still holds them.",
	}
}

// LiftFenceWithProof retires a fence on evidence an operator proved outside the
// reconciler. It re-validates the proof against the live fence rather than
// trusting the caller, and it is the ONLY entry point that can lift a fence the
// reconciler could not resolve — a restored fence, whose bytes did not survive
// the restart.
func LiftFenceWithProof(ctx context.Context, signerPubKeyHex string, proof FenceLiftProof) error {
	raw, err := hex.DecodeString(strings.TrimSpace(signerPubKeyHex))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return fmt.Errorf("fence lift: %q is not an ed25519 signer", signerPubKeyHex)
	}
	key := string(raw)

	fenceMu.Lock()
	fence := fences[key]
	fenceMu.Unlock()
	if fence == nil {
		return fmt.Errorf("fence lift: signer %s holds no fence", signerPrefix(key))
	}

	verdict, detail, validateErr := validateFenceLiftProof(signerPrefix(key), fence, proof)
	if validateErr != nil {
		return validateErr
	}
	// This package cannot tell whether the proof was read by the node's own
	// reader or by a caller, so the operator path labels it: a lift recorded as
	// a plain proof may have been resolved automatically, and only this route
	// knows a human asked.
	detail = "operator lift: " + detail

	// A restored fence has no reconciler to retire its pending count, so the
	// operator's verdict retires it. Done under the lock that liftFence also
	// takes, before the call, so the decrement inside liftFence observes zero.
	fenceMu.Lock()
	if fence.pending > 0 {
		fence.pending = 1
	}
	fenceMu.Unlock()

	liftFence(key, fence, verdict, detail)
	return nil
}

func validateFenceLiftProof(signer string, fence *keyFence, proof FenceLiftProof) (TxVerdict, string, error) {
	switch proof.Kind {
	case "superseded":
		if !fence.hasNonce {
			return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
				Signer: signer,
				Reason: "the fence recorded no nonce, so supersession cannot be evaluated for it",
			}
		}
		if proof.CommittedNonce <= fence.nonce {
			return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
				Signer: signer,
				Reason: fmt.Sprintf(
					"the proof claims committed nonce %d, which is not above the fenced nonce %d",
					proof.CommittedNonce, fence.nonce),
			}
		}
		return TxVerdictSuperseded, fmt.Sprintf(
			"operator proof: signer committed nonce %d above the fenced %d, so the fenced transaction can "+
				"never commit and its payload is permanently lost (%s)",
			proof.CommittedNonce, fence.nonce, proof.Detail), nil
	case "committed", "rejected":
		if fence.txHash != "" && proof.TxHash != "" &&
			!strings.EqualFold(fence.txHash, proof.TxHash) {
			return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
				Signer: signer,
				Reason: "the proof is about a different transaction than the one this fence holds",
			}
		}
		verdict := TxVerdictCommitted
		if proof.Kind == "rejected" {
			verdict = TxVerdictRejected
		}
		return verdict, fmt.Sprintf("proof: %s", proof.Detail), nil
	case "spent":
		if !fence.hasNonce {
			return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
				Signer: signer,
				Reason: "the fence recorded no nonce, so a spent allocation cannot be evaluated for it",
			}
		}
		if proof.CommittedNonce < fence.nonce {
			return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
				Signer: signer,
				Reason: fmt.Sprintf(
					"the proof claims committed nonce %d, which is below the fenced nonce %d",
					proof.CommittedNonce, fence.nonce),
			}
		}
		return TxVerdictSpent, fmt.Sprintf(
			"proof: signer's committed nonce has reached the fenced %d, so the allocation is spent and "+
				"these bytes can never commit again (%s)", fence.nonce, proof.Detail), nil
	default:
		return TxVerdictUnresolved, "", &FenceLiftUnprovenError{
			Signer: signer,
			Reason: fmt.Sprintf("unknown proof kind %q", proof.Kind),
		}
	}
}
