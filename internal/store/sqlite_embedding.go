package store

// Embedding encoding for the SQLite projection.
//
// Embeddings used to be stored as JSON text ("[0.0123,-0.0456,...]"): about
// 19 KB for a 1,536-dim vector, and QuerySimilar re-parsed every row's JSON
// on every vector recall. They are now stored as a 4-byte marker followed by
// packed little-endian float32 (4 bytes per dimension, about 6 KB for 1,536
// dims), which decodes with no parsing.
//
// The embedding column is an off-chain, per-node search index: it is not part
// of consensus, the AppHash, or the embedding_hash (which hashes the float
// values, not these bytes), so changing its encoding is a local change.
// Vault encryption wraps the encoded bytes exactly as before: the marker sits
// inside the ciphertext.
//
// decodeEmbedding reads both formats, so legacy JSON rows keep working until
// MigrateEmbeddingEncoding rewrites them.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
)

// embeddingPackedMagic prefixes a packed float32 embedding. A JSON array can
// never start with a NUL byte, so the two formats cannot be confused.
var embeddingPackedMagic = []byte{0x00, 'f', '3', '2'}

// embeddingMigrationPage bounds how many rows one migration page reads and
// rewrites, so a large store never holds the write lock for long.
const embeddingMigrationPage = 256

// encodeEmbedding returns the packed form of emb, or nil for an empty vector.
// A vector holding NaN or ±Inf encodes to nil, as the JSON encoder did (it
// refused such values), so such a row stores no embedding.
func encodeEmbedding(emb []float32) []byte {
	if len(emb) == 0 {
		return nil
	}
	out := make([]byte, len(embeddingPackedMagic)+4*len(emb))
	copy(out, embeddingPackedMagic)
	body := out[len(embeddingPackedMagic):]
	for i, v := range emb {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil
		}
		binary.LittleEndian.PutUint32(body[4*i:], math.Float32bits(v))
	}
	return out
}

// decodeEmbedding reads a packed or legacy JSON embedding. It returns nil for
// empty, malformed, or undecryptable bytes.
func decodeEmbedding(data []byte) []float32 {
	if len(data) == 0 {
		return nil
	}
	if bytes.HasPrefix(data, embeddingPackedMagic) {
		return decodePackedEmbedding(data[len(embeddingPackedMagic):])
	}
	return decodeLegacyEmbedding(data)
}

func decodePackedEmbedding(body []byte) []float32 {
	if len(body) == 0 || len(body)%4 != 0 {
		return nil
	}
	out := make([]float32, len(body)/4)
	for i := range out {
		v := math.Float32frombits(binary.LittleEndian.Uint32(body[4*i:]))
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil
		}
		out[i] = v
	}
	return out
}

func decodeLegacyEmbedding(data []byte) []float32 {
	var emb []float32
	if err := json.Unmarshal(data, &emb); err != nil || len(emb) == 0 {
		return nil
	}
	return emb
}

// MigrateEmbeddingEncoding rewrites legacy JSON embeddings in the packed
// format and returns how many rows it converted. It is idempotent and safe to
// run while the node serves:
//
//   - each row is rewritten with a compare-and-set on its old bytes, so a
//     concurrent insert or re-embed always wins;
//   - rows it cannot read (for example encrypted under a vault that is not
//     unlocked, or under another key) are left alone and keep decoding as
//     before;
//   - with encryption expected but the vault locked it does nothing, because
//     the encrypted rows cannot be read or rewritten until unlock.
//
// Without a vault only legacy rows are read (the marker is checked in SQL),
// so once a store is converted a run costs one indexed range scan.
func (s *SQLiteStore) MigrateEmbeddingEncoding(ctx context.Context) (int, error) {
	vaultOn := s.vault.Load() != nil
	if !vaultOn && s.vaultExpected.Load() {
		return 0, nil
	}
	query := `SELECT memory_id, embedding FROM memories
		WHERE embedding IS NOT NULL AND memory_id > ?`
	if !vaultOn {
		query += ` AND substr(embedding, 1, 4) IS NOT ?`
	}
	query += ` ORDER BY memory_id LIMIT ?`

	type row struct {
		id  string
		raw []byte
	}
	converted := 0
	lastID := ""
	for {
		if err := ctx.Err(); err != nil {
			return converted, err
		}
		args := []any{lastID}
		if !vaultOn {
			args = append(args, embeddingPackedMagic)
		}
		args = append(args, embeddingMigrationPage)
		rows, err := s.conn.QueryContext(ctx, query, args...)
		if err != nil {
			return converted, fmt.Errorf("migrate embeddings: read: %w", err)
		}
		var page []row
		for rows.Next() {
			var r row
			if scanErr := rows.Scan(&r.id, &r.raw); scanErr != nil {
				_ = rows.Close()
				return converted, fmt.Errorf("migrate embeddings: scan: %w", scanErr)
			}
			page = append(page, r)
		}
		iterErr := rows.Err()
		_ = rows.Close()
		if iterErr != nil {
			return converted, fmt.Errorf("migrate embeddings: read: %w", iterErr)
		}
		if len(page) == 0 {
			return converted, nil
		}
		for _, r := range page {
			n, err := s.migrateOneEmbedding(ctx, r.id, r.raw)
			if err != nil {
				return converted, err
			}
			converted += n
		}
		lastID = page[len(page)-1].id
	}
}

func (s *SQLiteStore) migrateOneEmbedding(ctx context.Context, memoryID string, raw []byte) (int, error) {
	plain, _ := s.decryptEmbedding(raw)
	if bytes.HasPrefix(plain, embeddingPackedMagic) {
		return 0, nil
	}
	emb := decodeLegacyEmbedding(plain)
	if emb == nil {
		return 0, nil // unreadable here: leave it for a later run
	}
	packed := encodeEmbedding(emb)
	if packed == nil {
		return 0, nil
	}
	stored, err := s.encryptEmbedding(packed)
	if err != nil {
		return 0, fmt.Errorf("migrate embeddings: encrypt: %w", err)
	}
	res, err := s.writeExecContext(ctx,
		`UPDATE memories SET embedding = ? WHERE memory_id = ? AND CAST(embedding AS BLOB) = ?`,
		stored, memoryID, raw)
	if err != nil {
		return 0, fmt.Errorf("migrate embeddings: update: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
