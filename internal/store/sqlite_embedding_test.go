package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
)

func legacyJSONEmbedding(t testing.TB, emb []float32) []byte {
	t.Helper()
	b, err := json.Marshal(emb)
	require.NoError(t, err)
	return b
}

func rawEmbeddingColumn(t *testing.T, s *SQLiteStore, id string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, s.conn.QueryRowContext(context.Background(),
		`SELECT embedding FROM memories WHERE memory_id = ?`, id).Scan(&raw))
	return raw
}

func TestEmbeddingEncoding_PackedRoundTrip(t *testing.T) {
	emb := []float32{0.5, -1.25, 3.0e-7, 0, -0, math.MaxFloat32, -math.SmallestNonzeroFloat32}
	enc := encodeEmbedding(emb)
	require.True(t, bytes.HasPrefix(enc, embeddingPackedMagic))
	assert.Len(t, enc, len(embeddingPackedMagic)+4*len(emb))
	assert.Equal(t, emb, decodeEmbedding(enc))
}

func TestEmbeddingEncoding_LegacyJSONStillDecodes(t *testing.T) {
	emb := []float32{0.1, 0.2, -0.3}
	assert.Equal(t, emb, decodeEmbedding(legacyJSONEmbedding(t, emb)))
}

func TestEmbeddingEncoding_RejectsBadInput(t *testing.T) {
	assert.Nil(t, encodeEmbedding(nil))
	assert.Nil(t, encodeEmbedding([]float32{}))
	assert.Nil(t, encodeEmbedding([]float32{1, float32(math.NaN())}), "NaN must not be stored")
	assert.Nil(t, encodeEmbedding([]float32{float32(math.Inf(1))}), "Inf must not be stored")

	assert.Nil(t, decodeEmbedding(nil))
	assert.Nil(t, decodeEmbedding(embeddingPackedMagic), "marker without a body")
	assert.Nil(t, decodeEmbedding(append(append([]byte{}, embeddingPackedMagic...), 1, 2, 3)), "body not a multiple of 4")
	nan := append(append([]byte{}, embeddingPackedMagic...), 0x00, 0x00, 0xc0, 0x7f)
	assert.Nil(t, decodeEmbedding(nan), "NaN in a packed body")
	assert.Nil(t, decodeEmbedding([]byte("not json")))
	assert.Nil(t, decodeEmbedding([]byte("[]")))
}

func TestInsertMemory_StoresPackedEmbedding(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	rec := testMemory("m1", "agentA", "packed on insert", "general")
	rec.Embedding = []float32{0.1, 0.2, 0.3}
	require.NoError(t, s.InsertMemory(ctx, rec))
	raw := rawEmbeddingColumn(t, s, "m1")
	assert.True(t, bytes.HasPrefix(raw, embeddingPackedMagic))
	assert.Len(t, raw, 4+12)

	require.NoError(t, s.UpdateMemoryEmbedding(ctx, "m1", []float32{0.4, 0.5, 0.6}, "test"))
	assert.Equal(t, []float32{0.4, 0.5, 0.6}, decodeEmbedding(rawEmbeddingColumn(t, s, "m1")))
}

func TestMigrateEmbeddingEncoding_Plaintext(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	embs := map[string][]float32{
		"a": {1, 0, 0},
		"b": {0, 1, 0},
		"c": {0.9, 0.1, 0},
	}
	for id, emb := range embs {
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agentA", "content "+id, "general")))
		_, err := s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`, legacyJSONEmbedding(t, emb), id)
		require.NoError(t, err)
	}
	// An unreadable legacy row is left alone, not dropped.
	require.NoError(t, s.InsertMemory(ctx, testMemory("junk", "agentA", "content junk", "general")))
	_, err := s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`, []byte("garbage"), "junk")
	require.NoError(t, err)

	before, err := s.QuerySimilar(ctx, []float32{1, 0, 0}, QueryOptions{TopK: 10})
	require.NoError(t, err)

	n, err := s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	for id, emb := range embs {
		raw := rawEmbeddingColumn(t, s, id)
		assert.True(t, bytes.HasPrefix(raw, embeddingPackedMagic), id)
		assert.Equal(t, emb, decodeEmbedding(raw), id)
	}
	assert.Equal(t, []byte("garbage"), rawEmbeddingColumn(t, s, "junk"))

	after, err := s.QuerySimilar(ctx, []float32{1, 0, 0}, QueryOptions{TopK: 10})
	require.NoError(t, err)
	assert.Equal(t, recordIDs(before), recordIDs(after), "ranking unchanged by the rewrite")

	n, err = s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "second run is a no-op")
}

func TestMigrateEmbeddingEncoding_PagesThroughLargeStores(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	total := embeddingMigrationPage*2 + 7
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("m%04d", i)
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agentA", "content "+id, "general")))
		_, err := s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`,
			legacyJSONEmbedding(t, []float32{float32(i), 1}), id)
		require.NoError(t, err)
	}
	n, err := s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Equal(t, total, n)
}

func attachEmbeddingVault(t *testing.T, s *SQLiteStore) *vault.Vault {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "embedding-passphrase"))
	v, err := vault.Open(keyPath, "embedding-passphrase")
	require.NoError(t, err)
	s.ActivateVault(v)
	return v
}

func TestMigrateEmbeddingEncoding_VaultEncrypted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	v := attachEmbeddingVault(t, s)

	emb := []float32{0.25, -0.5, 0.75}
	require.NoError(t, s.InsertMemory(ctx, testMemory("enc", "agentA", "encrypted legacy", "general")))
	legacyCipher, err := v.Encrypt(legacyJSONEmbedding(t, emb))
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`, legacyCipher, "enc")
	require.NoError(t, err)

	// Locked: encryption expected, no vault. Nothing is read or rewritten.
	s.SetVault(nil)
	n, err := s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Equal(t, legacyCipher, rawEmbeddingColumn(t, s, "enc"))

	s.ActivateVault(v)
	n, err = s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	raw := rawEmbeddingColumn(t, s, "enc")
	assert.False(t, bytes.HasPrefix(raw, embeddingPackedMagic), "the marker stays inside the ciphertext")
	plain, err := v.Decrypt(raw)
	require.NoError(t, err)
	assert.True(t, bytes.HasPrefix(plain, embeddingPackedMagic))
	assert.Equal(t, emb, decodeEmbedding(plain))

	got, err := s.QuerySimilar(ctx, emb, QueryOptions{TopK: 1})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, emb, got[0].Embedding)

	n, err = s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestMigrateEmbeddingEncoding_ForeignKeyCiphertextLeftAlone(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	other := filepath.Join(t.TempDir(), "other.key")
	require.NoError(t, vault.Init(other, "other-passphrase"))
	ov, err := vault.Open(other, "other-passphrase")
	require.NoError(t, err)
	cipher, err := ov.Encrypt(legacyJSONEmbedding(t, []float32{1, 2}))
	require.NoError(t, err)

	attachEmbeddingVault(t, s)
	require.NoError(t, s.InsertMemory(ctx, testMemory("x", "agentA", "other key", "general")))
	_, err = s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`, cipher, "x")
	require.NoError(t, err)

	n, err := s.MigrateEmbeddingEncoding(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	assert.Equal(t, cipher, rawEmbeddingColumn(t, s, "x"))
}

// --- benchmarks ---

const benchEmbeddingDims = 1536

func benchVector(r *rand.Rand) []float32 {
	v := make([]float32, benchEmbeddingDims)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

func BenchmarkDecodeEmbedding(b *testing.B) {
	v := benchVector(rand.New(rand.NewSource(1))) // #nosec G404 -- benchmark data
	legacy := legacyJSONEmbedding(b, v)
	packed := encodeEmbedding(v)
	b.Run("legacy_json", func(b *testing.B) {
		b.SetBytes(int64(len(legacy)))
		for i := 0; i < b.N; i++ {
			if decodeEmbedding(legacy) == nil {
				b.Fatal("decode failed")
			}
		}
	})
	b.Run("packed_f32", func(b *testing.B) {
		b.SetBytes(int64(len(packed)))
		for i := 0; i < b.N; i++ {
			if decodeEmbedding(packed) == nil {
				b.Fatal("decode failed")
			}
		}
	})
}

// BenchmarkQuerySimilar scans 2,000 x 1,536-dim rows, about the size of a
// real store that measured p50 531 ms per recall with JSON embeddings.
func BenchmarkQuerySimilar(b *testing.B) {
	const rows = 2000
	for _, format := range []string{"legacy_json", "packed_f32"} {
		b.Run(format, func(b *testing.B) {
			ctx := context.Background()
			s, err := NewSQLiteStore(ctx, filepath.Join(b.TempDir(), "bench.db"))
			require.NoError(b, err)
			defer func() { _ = s.Close() }()
			r := rand.New(rand.NewSource(2)) // #nosec G404 -- benchmark data
			for i := 0; i < rows; i++ {
				id := fmt.Sprintf("m%05d", i)
				rec := testMemory(id, "agentA", "content "+id, "general")
				rec.Status = memory.StatusCommitted
				rec.Embedding = benchVector(r)
				require.NoError(b, s.InsertMemory(ctx, rec))
				if format == "legacy_json" {
					_, err := s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`,
						legacyJSONEmbedding(b, rec.Embedding), id)
					require.NoError(b, err)
				}
			}
			q := benchVector(r)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := s.QuerySimilar(ctx, q, QueryOptions{TopK: 10, StatusFilter: "committed"})
				if err != nil || len(got) != 10 {
					b.Fatalf("query: %v (%d results)", err, len(got))
				}
			}
		})
	}
}
