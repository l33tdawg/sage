package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"

	"github.com/l33tdawg/sage/internal/vault"
)

const MaxPrivateJPEGBytes = 2 * 1024 * 1024
const privateJPEGSchema = "sage.private-jpeg.v1\x00"
const privateJPEGHeader = len(privateJPEGSchema) + 64 + 36 + 8 + 4 + sha256.Size
const maxPrivateJPEGSealed = privateJPEGHeader + MaxPrivateJPEGBytes + 28

var (
	ErrPrivateMediaUnavailable = errors.New("private_media_unavailable")
	ErrPrivateMediaInvalid     = errors.New("private_media_invalid")
	ErrPrivateMediaNotFound    = errors.New("private_media_not_found")
	ErrPrivateMediaConflict    = errors.New("private_media_conflict")
	ErrPrivateMediaCorrupt     = errors.New("private_media_corrupt")
	ErrPrivateMediaQuota       = errors.New("private_media_quota")
	ErrPrivateMediaDiskBudget  = errors.New("private_media_disk_budget")
)

type PrivateMediaQuota struct {
	Enabled      bool
	ActorBytes   int64
	NodeBytes    int64
	ActorObjects int64
	NodeObjects  int64
	MinFreeBytes int64
}

type PrivateMediaSpaceProbe func(context.Context) (int64, error)

type PrivateMediaRecord struct {
	AgentID  string
	ObjectID string
	Revision int64
	Length   int64
	Digest   string
	JPEG     []byte
}

type PrivateMediaStore struct {
	store *SQLiteStore
	quota PrivateMediaQuota
	space PrivateMediaSpaceProbe
}

func NewPrivateMediaStore(sqlite *SQLiteStore, quota PrivateMediaQuota, space PrivateMediaSpaceProbe) (*PrivateMediaStore, error) {
	if sqlite == nil || sqlite.db == nil {
		return nil, ErrPrivateMediaUnavailable
	}
	if quota.Enabled && (space == nil || quota.ActorBytes <= 0 || quota.NodeBytes < quota.ActorBytes ||
		quota.ActorObjects <= 0 || quota.NodeObjects < quota.ActorObjects || quota.MinFreeBytes < 0 ||
		quota.MinFreeBytes > math.MaxInt64-4*int64(maxPrivateJPEGSealed)-65536) {
		return nil, ErrPrivateMediaInvalid
	}
	return &PrivateMediaStore{store: sqlite, quota: quota, space: space}, nil
}

func (s *SQLiteStore) migratePrivateMedia(ctx context.Context) error {
	_, err := s.writeExecContext(ctx, `CREATE TABLE IF NOT EXISTS private_jpeg_objects (
		agent_id TEXT NOT NULL,
		object_id TEXT NOT NULL,
		revision INTEGER NOT NULL CHECK (revision = 1),
		sealed_record BLOB NOT NULL,
		PRIMARY KEY (agent_id, object_id)
	)`)
	return err
}

func validPrivateJPEG(image []byte) bool {
	if len(image) < 4 || len(image) > MaxPrivateJPEGBytes || !bytes.HasPrefix(image, []byte{255, 216}) || !bytes.HasSuffix(image, []byte{255, 217}) {
		return false
	}
	offset, components := 2, 0
	for offset < len(image)-2 {
		if image[offset] != 255 {
			return false
		}
		for offset < len(image) && image[offset] == 255 {
			offset++
		}
		if offset >= len(image) {
			return false
		}
		marker := image[offset]
		offset++
		if marker == 0 || marker == 1 || marker == 216 || marker == 217 || (marker >= 208 && marker <= 215) || offset+2 > len(image)-2 {
			return false
		}
		length := int(binary.BigEndian.Uint16(image[offset : offset+2]))
		if length < 2 || offset+length > len(image)-2 {
			return false
		}
		segment := image[offset+2 : offset+length]
		offset += length
		if marker == 192 || marker == 194 {
			if components != 0 || len(segment) < 6 {
				return false
			}
			height, width := int(binary.BigEndian.Uint16(segment[1:3])), int(binary.BigEndian.Uint16(segment[3:5]))
			components = int(segment[5])
			if segment[0] != 8 || (components != 1 && components != 3) || len(segment) != 6+3*components ||
				width < 1 || width > 1920 || height < 1 || height > 1080 {
				return false
			}
		} else if marker >= 192 && marker <= 207 && marker != 196 && marker != 200 && marker != 204 {
			return false
		}
		if marker == 218 {
			return components != 0 && len(segment) > 0 && segment[0] >= 1 && int(segment[0]) <= components &&
				len(segment) == 4+2*int(segment[0]) && offset < len(image)-2
		}
	}
	return false
}

func privateJPEGPlaintext(actor, identifier string, image []byte) []byte {
	digest := sha256.Sum256(image)
	plain := make([]byte, 0, privateJPEGHeader+len(image))
	plain = append(plain, privateJPEGSchema...)
	plain = append(plain, actor...)
	plain = append(plain, identifier...)
	plain = binary.BigEndian.AppendUint64(plain, 1)
	plain = binary.BigEndian.AppendUint32(plain, uint32(len(image)))
	plain = append(plain, digest[:]...)
	return append(plain, image...)
}

func readPrivateJPEG(ctx context.Context, connection sqlQuerier, active *vault.Vault, actor, identifier string) (*PrivateMediaRecord, error) {
	var revision, length int64
	var sealed []byte
	err := connection.QueryRowContext(ctx, `SELECT revision, length(sealed_record),
		CASE WHEN length(sealed_record) <= ? THEN sealed_record ELSE NULL END
		FROM private_jpeg_objects WHERE agent_id = ? AND object_id = ?`, maxPrivateJPEGSealed, actor, identifier).Scan(&revision, &length, &sealed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPrivateMediaNotFound
	}
	if err != nil {
		return nil, ErrPrivateMediaUnavailable
	}
	if revision != 1 || length < int64(privateJPEGHeader)+28 || length > int64(maxPrivateJPEGSealed) {
		return nil, ErrPrivateMediaCorrupt
	}
	plain, err := active.Decrypt(sealed)
	if err != nil || len(plain) < privateJPEGHeader || len(plain) > privateJPEGHeader+MaxPrivateJPEGBytes {
		return nil, ErrPrivateMediaCorrupt
	}
	prefix := privateJPEGSchema + actor + identifier
	if !bytes.HasPrefix(plain, []byte(prefix)) {
		return nil, ErrPrivateMediaCorrupt
	}
	offset := len(prefix)
	if binary.BigEndian.Uint64(plain[offset:offset+8]) != uint64(revision) {
		return nil, ErrPrivateMediaCorrupt
	}
	offset += 8
	imageLength := binary.BigEndian.Uint32(plain[offset : offset+4])
	offset += 4
	image := plain[privateJPEGHeader:]
	digest := sha256.Sum256(image)
	if int(imageLength) != len(image) || subtle.ConstantTimeCompare(plain[offset:offset+sha256.Size], digest[:]) != 1 || !validPrivateJPEG(image) {
		return nil, ErrPrivateMediaCorrupt
	}
	return &PrivateMediaRecord{AgentID: actor, ObjectID: identifier, Revision: revision, Length: int64(len(image)), Digest: hex.EncodeToString(digest[:]), JPEG: image}, nil
}

func (media *PrivateMediaStore) Get(ctx context.Context, actor, identifier string) (*PrivateMediaRecord, error) {
	if media == nil || media.store == nil || media.store.db == nil || !media.quota.Enabled {
		return nil, ErrPrivateMediaUnavailable
	}
	if !validWorkflowJournalIdentity(actor, identifier) {
		return nil, ErrPrivateMediaInvalid
	}
	sqlite := media.store
	sqlite.vaultPublicationMu.RLock()
	defer sqlite.vaultPublicationMu.RUnlock()
	active := sqlite.vault.Load()
	if !sqlite.vaultExpected.Load() || active == nil {
		return nil, ErrPrivateMediaUnavailable
	}
	return readPrivateJPEG(ctx, sqlite.conn, active, actor, identifier)
}

func (media *PrivateMediaStore) Put(ctx context.Context, actor, identifier string, expectedRevision int64, image []byte) (*PrivateMediaRecord, error) {
	if media == nil || media.store == nil || media.store.db == nil || !media.quota.Enabled {
		return nil, ErrPrivateMediaUnavailable
	}
	if !validWorkflowJournalIdentity(actor, identifier) || expectedRevision != 0 || len(image) > MaxPrivateJPEGBytes {
		return nil, ErrPrivateMediaInvalid
	}
	image = append([]byte(nil), image...)
	if !validPrivateJPEG(image) {
		return nil, ErrPrivateMediaInvalid
	}
	freeBytes, spaceErr := media.space(ctx)
	sqlite := media.store
	unlock, err := sqlite.lockVaultWrite(ctx)
	if err != nil {
		return nil, ErrPrivateMediaUnavailable
	}
	defer unlock()
	active := sqlite.vault.Load()
	if !sqlite.vaultExpected.Load() || active == nil {
		return nil, ErrPrivateMediaUnavailable
	}
	transaction, err := sqlite.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, ErrPrivateMediaUnavailable
	}
	defer transaction.Rollback()
	current, err := readPrivateJPEG(ctx, transaction, active, actor, identifier)
	if err == nil {
		if bytes.Equal(current.JPEG, image) {
			return current, nil
		}
		return nil, ErrPrivateMediaConflict
	}
	if !errors.Is(err, ErrPrivateMediaNotFound) {
		return nil, err
	}
	sealed, err := active.Encrypt(privateJPEGPlaintext(actor, identifier, image))
	if err != nil || len(sealed) > maxPrivateJPEGSealed {
		return nil, ErrPrivateMediaUnavailable
	}
	var actorCount, nodeCount, actorBytes, nodeBytes int64
	err = transaction.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(length(sealed_record)), 0),
		COALESCE(SUM(CASE WHEN agent_id = ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN agent_id = ? THEN length(sealed_record) ELSE 0 END), 0) FROM private_jpeg_objects`, actor, actor).
		Scan(&nodeCount, &nodeBytes, &actorCount, &actorBytes)
	if err != nil || actorBytes < 0 || nodeBytes < 0 {
		return nil, ErrPrivateMediaUnavailable
	}
	added := int64(len(sealed))
	if actorCount >= media.quota.ActorObjects || nodeCount >= media.quota.NodeObjects ||
		actorBytes > media.quota.ActorBytes-added || nodeBytes > media.quota.NodeBytes-added {
		return nil, ErrPrivateMediaQuota
	}
	if spaceErr != nil || freeBytes <= 0 || freeBytes < media.quota.MinFreeBytes+4*added+65536 {
		return nil, ErrPrivateMediaDiskBudget
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO private_jpeg_objects(agent_id, object_id, revision, sealed_record) VALUES(?,?,1,?)`, actor, identifier, sealed); err != nil {
		return nil, ErrPrivateMediaUnavailable
	}
	if err := transaction.Commit(); err != nil {
		return nil, ErrPrivateMediaUnavailable
	}
	digest := sha256.Sum256(image)
	return &PrivateMediaRecord{AgentID: actor, ObjectID: identifier, Revision: 1, Length: int64(len(image)), Digest: hex.EncodeToString(digest[:]), JPEG: image}, nil
}
