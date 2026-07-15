package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/scope"
)

var (
	ErrScopeBallotConflict = errors.New("scope ballot conflict")
	ErrScopeVoteExists     = errors.New("scoped vote already exists")
)

func scopeBallotKey(memoryID string) []byte   { return []byte("state:scope-proposal:" + memoryID) }
func scopedContentKey(memoryID string) []byte { return []byte("state:scope-content:" + memoryID) }
func scopedVoteHeightKey(memoryID, validatorID string) []byte {
	return []byte("state:scope-vote-height:" + memoryID + ":" + validatorID)
}

// SetScopedMemorySubmission atomically creates the ordinary memory state and
// its canonical v11.9 ballot/content mirrors. Every field is validated before
// the transaction begins; an error leaves no partially scoped memory behind.
func (s *BadgerStore) SetScopedMemorySubmission(ballot scope.Ballot, content scope.Content) error {
	ballotBytes, err := scope.EncodeBallot(ballot)
	if err != nil {
		return fmt.Errorf("encode scope ballot: %w", err)
	}
	contentBytes, err := scope.EncodeContent(content)
	if err != nil {
		return fmt.Errorf("encode scoped content: %w", err)
	}
	if ballot.State != scope.BallotPending || ballot.MemoryID != content.MemoryID || ballot.ScopeID != content.ScopeID ||
		ballot.ScopeRevision != content.ScopeRevision || ballot.SubmittedHeight != content.SubmittedHeight {
		return errors.New("scope ballot and content identity do not match")
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := requireExactOrMissing(txn, scopeBallotKey(ballot.MemoryID), ballotBytes, ErrScopeBallotConflict); err != nil {
			return err
		}
		if err := requireExactOrMissing(txn, scopedContentKey(content.MemoryID), contentBytes, ErrScopeBallotConflict); err != nil {
			return err
		}

		// A still-proposed legacy record may be enrolled by a later app-v20
		// re-submit only when its canonical hash is identical. Never overwrite a
		// terminal verdict or change the content behind an existing memory ID.
		if item, getErr := txn.Get(memoryKey(content.MemoryID)); getErr == nil {
			if err := item.Value(func(value []byte) error {
				hash, status, err := decodeMemoryEntry(value)
				if err != nil {
					return err
				}
				if status != "proposed" || !bytes.Equal(hash, content.ContentHash) {
					return fmt.Errorf("%w: existing memory hash or status differs", ErrScopeBallotConflict)
				}
				return nil
			}); err != nil {
				return err
			}
		} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}

		if err := requireExactOrMissing(txn, memoryDomainKey(content.MemoryID), []byte(content.Domain), ErrScopeBallotConflict); err != nil {
			return err
		}
		if err := requireExactOrMissing(txn, memoryAuthorKey(content.MemoryID), []byte(content.SubmittingAgentID), ErrScopeBallotConflict); err != nil {
			return err
		}
		if err := requireExactOrMissing(txn, memClassKey(content.MemoryID), []byte{content.Classification}, ErrScopeBallotConflict); err != nil {
			return err
		}

		writes := []struct {
			key   []byte
			value []byte
		}{
			{memoryKey(content.MemoryID), encodeMemoryHashEntry(content.ContentHash, "proposed")},
			{memoryDomainKey(content.MemoryID), []byte(content.Domain)},
			{memoryAuthorKey(content.MemoryID), []byte(content.SubmittingAgentID)},
			{memClassKey(content.MemoryID), []byte{content.Classification}},
			{scopeBallotKey(ballot.MemoryID), ballotBytes},
			{scopedContentKey(content.MemoryID), contentBytes},
		}
		for _, write := range writes {
			if err := txn.Set(write.key, write.value); err != nil {
				return err
			}
		}
		return nil
	})
}

func requireExactOrMissing(txn *badger.Txn, key, want []byte, conflict error) error {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return item.Value(func(value []byte) error {
		if bytes.Equal(value, want) {
			return nil
		}
		return conflict
	})
}

func decodeMemoryEntry(value []byte) ([]byte, string, error) {
	if len(value) < 4 {
		return nil, "", errors.New("invalid memory hash entry")
	}
	hashLen := binary.BigEndian.Uint32(value[:4])
	if uint64(hashLen)+4 > uint64(len(value)) {
		return nil, "", errors.New("invalid memory hash entry")
	}
	hash := append([]byte(nil), value[4:4+int(hashLen)]...)
	return hash, string(value[4+int(hashLen):]), nil
}

func (s *BadgerStore) GetScopeBallot(memoryID string) (*scope.Ballot, error) {
	var ballot *scope.Ballot
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(scopeBallotKey(memoryID))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			decoded, err := scope.DecodeBallot(value)
			if err != nil {
				return err
			}
			ballot = &decoded
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scope ballot %q: %w", memoryID, err)
	}
	return ballot, nil
}

func (s *BadgerStore) GetScopedContent(memoryID string) (*scope.Content, error) {
	var content *scope.Content
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(scopedContentKey(memoryID))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			decoded, err := scope.DecodeContent(value)
			if err != nil {
				return err
			}
			content = &decoded
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scoped content %q: %w", memoryID, err)
	}
	return content, nil
}

// ListScopedContents returns every canonical scoped envelope in bytewise
// memory-ID order. Recovery uses this after snapshot/state sync to rebuild a
// discarded local projection without consulting federation peers.
func (s *BadgerStore) ListScopedContents() ([]scope.Content, error) {
	prefix := []byte("state:scope-content:")
	contents := make([]scope.Content, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			memoryID := string(item.Key()[len(prefix):])
			if err := item.Value(func(value []byte) error {
				content, err := scope.DecodeContent(value)
				if err != nil {
					return err
				}
				if content.MemoryID != memoryID {
					return fmt.Errorf("scoped content key %q disagrees with envelope %q", memoryID, content.MemoryID)
				}
				contents = append(contents, content)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list scoped contents: %w", err)
	}
	return contents, nil
}

// SetScopedVote records one terminal member decision. Scoped votes are
// immutable: retrying the exact decision at the same FinalizeBlock height is
// idempotent, while changing either is rejected. Membership is checked against
// the pinned ballot, not a live scope.
func (s *BadgerStore) SetScopedVote(memoryID, validatorID, decision string, height int64) (bool, error) {
	if decision != "accept" && decision != "reject" && decision != "abstain" {
		return false, fmt.Errorf("invalid scoped vote decision %q", decision)
	}
	if height <= 0 {
		return false, errors.New("scoped vote height must be positive")
	}
	inserted := false
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(scopeBallotKey(memoryID))
		if err != nil {
			return err
		}
		var ballot scope.Ballot
		if err := item.Value(func(value []byte) error {
			var decodeErr error
			ballot, decodeErr = scope.DecodeBallot(value)
			return decodeErr
		}); err != nil {
			return err
		}
		member := false
		for _, candidate := range ballot.Members {
			if candidate.ValidatorID == validatorID {
				member = true
				break
			}
		}
		if !member {
			return fmt.Errorf("validator %q is not a pinned scope member", validatorID)
		}
		voteKey := []byte("state:vote:" + memoryID + ":" + validatorID)
		if existing, getErr := txn.Get(voteKey); getErr == nil {
			return existing.Value(func(value []byte) error {
				if string(value) != decision {
					return ErrScopeVoteExists
				}
				heightItem, heightErr := txn.Get(scopedVoteHeightKey(memoryID, validatorID))
				if heightErr != nil {
					return ErrScopeVoteExists
				}
				return heightItem.Value(func(heightBytes []byte) error {
					if len(heightBytes) != 8 || int64(binary.BigEndian.Uint64(heightBytes)) != height { // #nosec G115 -- consensus heights are positive int64 values
						return ErrScopeVoteExists
					}
					return nil
				})
			})
		} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}
		if ballot.State != scope.BallotPending {
			return fmt.Errorf("scoped ballot is terminal: %d", ballot.State)
		}
		if err := txn.Set(voteKey, []byte(decision)); err != nil {
			return err
		}
		heightBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(heightBytes, uint64(height)) // #nosec G115 -- height is validated positive
		if err := txn.Set(scopedVoteHeightKey(memoryID, validatorID), heightBytes); err != nil {
			return err
		}
		inserted = true
		return nil
	})
	return inserted, err
}

// GetScopedVote returns a scoped member's immutable decision and the exact
// FinalizeBlock height that first recorded it. The height binding lets crash
// recovery distinguish replay of the same uncommitted block from a later
// attempt to reuse an already-consumed nonce or vote.
func (s *BadgerStore) GetScopedVote(memoryID, validatorID string) (decision string, height int64, ok bool, err error) {
	voteKey := []byte("state:vote:" + memoryID + ":" + validatorID)
	err = s.db.View(func(txn *badger.Txn) error {
		voteItem, getErr := txn.Get(voteKey)
		if getErr != nil {
			return getErr
		}
		if valueErr := voteItem.Value(func(value []byte) error {
			decision = string(value)
			return nil
		}); valueErr != nil {
			return valueErr
		}
		heightItem, getErr := txn.Get(scopedVoteHeightKey(memoryID, validatorID))
		if getErr != nil {
			return getErr
		}
		return heightItem.Value(func(value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid scoped vote height")
			}
			height = int64(binary.BigEndian.Uint64(value)) // #nosec G115 -- stored consensus height originated as int64
			if height <= 0 {
				return errors.New("invalid scoped vote height")
			}
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("get scoped vote %q/%q: %w", memoryID, validatorID, err)
	}
	return decision, height, true, nil
}

// SetScopedMemoryVerdict atomically moves both the pinned ballot and ordinary
// memory record to the same terminal state while preserving the content hash.
func (s *BadgerStore) SetScopedMemoryVerdict(memoryID string, verdict scope.BallotState) error {
	var status string
	switch verdict {
	case scope.BallotCommitted:
		status = "committed"
	case scope.BallotDeprecated:
		status = "deprecated"
	default:
		return errors.New("scoped verdict must be committed or deprecated")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		ballotItem, err := txn.Get(scopeBallotKey(memoryID))
		if err != nil {
			return err
		}
		var ballot scope.Ballot
		if ballotValueErr := ballotItem.Value(func(value []byte) error {
			var decodeErr error
			ballot, decodeErr = scope.DecodeBallot(value)
			return decodeErr
		}); ballotValueErr != nil {
			return ballotValueErr
		}
		if ballot.State == verdict {
			return nil
		}
		if ballot.State != scope.BallotPending {
			return fmt.Errorf("scoped ballot already reached state %d", ballot.State)
		}
		contentItem, err := txn.Get(scopedContentKey(memoryID))
		if err != nil {
			return err
		}
		var content scope.Content
		if contentValueErr := contentItem.Value(func(value []byte) error {
			var decodeErr error
			content, decodeErr = scope.DecodeContent(value)
			return decodeErr
		}); contentValueErr != nil {
			return contentValueErr
		}
		memoryItem, err := txn.Get(memoryKey(memoryID))
		if err != nil {
			return err
		}
		if memoryValueErr := memoryItem.Value(func(value []byte) error {
			hash, current, decodeErr := decodeMemoryEntry(value)
			if decodeErr != nil {
				return decodeErr
			}
			if current != "proposed" || !bytes.Equal(hash, content.ContentHash) {
				return errors.New("scoped content and ordinary memory state disagree")
			}
			return nil
		}); memoryValueErr != nil {
			return memoryValueErr
		}
		ballot.State = verdict
		encodedBallot, err := scope.EncodeBallot(ballot)
		if err != nil {
			return err
		}
		if err := txn.Set(scopeBallotKey(memoryID), encodedBallot); err != nil {
			return err
		}
		return txn.Set(memoryKey(memoryID), encodeMemoryHashEntry(content.ContentHash, status))
	})
}
