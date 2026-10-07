package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

const privateMediaActor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const privateMediaOther = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func privateMediaJPEG(width, height int, padding string) []byte {
	image := []byte{255, 216, 255, 192, 0, 11, 8, byte(height >> 8), byte(height), byte(width >> 8), byte(width), 1, 1, 17, 0,
		255, 218, 0, 8, 1, 1, 0, 0, 63, 0}
	image = append(image, padding...)
	return append(image, 255, 217)
}

func privateMediaFixture(t *testing.T) (*SQLiteStore, *PrivateMediaStore, *vault.Vault, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "media.db")
	sqlite, err := NewSQLiteStore(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlite.Close() })
	keyPath := filepath.Join(t.TempDir(), "synthetic-vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-media-passphrase"))
	active, err := vault.Open(keyPath, "synthetic-media-passphrase")
	require.NoError(t, err)
	sqlite.SetVaultExpected(true)
	sqlite.SetVault(active)
	quota := PrivateMediaQuota{Enabled: true, ActorBytes: 8 << 20, NodeBytes: 16 << 20, ActorObjects: 10, NodeObjects: 20, MinFreeBytes: 512 << 20}
	media, err := NewPrivateMediaStore(sqlite, quota, func(context.Context) (int64, error) { return 1 << 30, nil })
	require.NoError(t, err)
	return sqlite, media, active, path
}

func TestPrivateMediaImmutableReplayAndReopen(t *testing.T) {
	sqlite, media, active, path := privateMediaFixture(t)
	identifier := uuid.NewString()
	image := privateMediaJPEG(1920, 1080, "synthetic-original-full-frame")
	record, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	digest := sha256.Sum256(image)
	require.Equal(t, hex.EncodeToString(digest[:]), record.Digest)
	require.Equal(t, int64(len(image)), record.Length)
	require.Equal(t, int64(1), record.Revision)
	replayed, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	require.Equal(t, record, replayed)
	_, err = media.Put(t.Context(), privateMediaActor, identifier, 0, privateMediaJPEG(1280, 720, "different"))
	require.ErrorIs(t, err, ErrPrivateMediaConflict)
	_, err = media.Put(t.Context(), privateMediaActor, identifier, 1, image)
	require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	image[0] = 0
	record.JPEG[0] = 0
	got, err := media.Get(t.Context(), privateMediaActor, identifier)
	require.NoError(t, err)
	require.Equal(t, byte(255), got.JPEG[0])
	require.NoError(t, sqlite.Close())
	reopened, err := NewSQLiteStore(t.Context(), path)
	require.NoError(t, err)
	defer reopened.Close()
	reopened.SetVaultExpected(true)
	reopened.SetVault(active)
	other, err := NewPrivateMediaStore(reopened, media.quota, media.space)
	require.NoError(t, err)
	gotAgain, err := other.Get(t.Context(), privateMediaActor, identifier)
	require.NoError(t, err)
	require.Equal(t, got, gotAgain)
}

func TestPrivateMediaInactiveAndVaultGates(t *testing.T) {
	sqlite, media, active, _ := privateMediaFixture(t)
	identifier := uuid.NewString()
	image := privateMediaJPEG(1280, 720, "synthetic")
	inactive, err := NewPrivateMediaStore(sqlite, PrivateMediaQuota{}, nil)
	require.NoError(t, err)
	_, err = inactive.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
	_, err = inactive.Get(t.Context(), privateMediaActor, identifier)
	require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
	for _, expected := range []bool{false, true} {
		for _, attached := range []bool{false, true} {
			if expected && attached {
				continue
			}
			sqlite.SetVaultExpected(expected)
			sqlite.SetVault(nil)
			if attached {
				sqlite.SetVault(active)
			}
			_, err = media.Put(t.Context(), privateMediaActor, identifier, 0, image)
			require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
			_, err = media.Get(t.Context(), privateMediaActor, identifier)
			require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
		}
	}
}

func TestPrivateMediaJPEGAndIdentityBounds(t *testing.T) {
	_, media, _, _ := privateMediaFixture(t)
	identifier := uuid.NewString()
	valid := privateMediaJPEG(1280, 720, "synthetic")
	for _, image := range [][]byte{nil, []byte("not-jpeg"), valid[:len(valid)-1],
		privateMediaJPEG(1921, 1080, "x"), privateMediaJPEG(1920, 1081, "x"), privateMediaJPEG(0, 720, "x"),
		privateMediaJPEG(1, 0, "x"), {255, 216, 255, 217}} {
		_, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
		require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	}
	for _, actor := range []string{"", "../actor", "A" + privateMediaActor[1:]} {
		_, err := media.Put(t.Context(), actor, identifier, 0, valid)
		require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	}
	for _, identifier := range []string{"../photo", "", "00000000-0000-0000-0000-000000000000"} {
		_, err := media.Get(t.Context(), privateMediaActor, identifier)
		require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	}
	_, err := media.Get(t.Context(), privateMediaActor, identifier)
	require.ErrorIs(t, err, ErrPrivateMediaNotFound)
	maximum := privateMediaJPEG(1920, 1080, "x")
	maximum = append(maximum[:len(maximum)-2], bytes.Repeat([]byte{'x'}, MaxPrivateJPEGBytes-len(maximum))...)
	maximum = append(maximum, 255, 217)
	require.Len(t, maximum, MaxPrivateJPEGBytes)
	_, err = media.Put(t.Context(), privateMediaActor, identifier, 0, maximum)
	require.NoError(t, err)
	oversized := append(append([]byte(nil), maximum[:len(maximum)-2]...), 'x', 255, 217)
	_, err = media.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, oversized)
	require.ErrorIs(t, err, ErrPrivateMediaInvalid)
}

func TestPrivateMediaCiphertextOnlyDatabaseAndWAL(t *testing.T) {
	sqlite, media, _, path := privateMediaFixture(t)
	canary := "synthetic-original-photo-canary-1641825649"
	image := privateMediaJPEG(1920, 1080, canary)
	identifier := uuid.NewString()
	_, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	var sealed []byte
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT sealed_record FROM private_jpeg_objects`).Scan(&sealed))
	require.NotContains(t, string(sealed), canary)
	digest := sha256.Sum256(image)
	require.False(t, bytes.Contains(sealed, digest[:]))
	for _, filename := range []string{path, path + "-wal"} {
		data, err := os.ReadFile(filename)
		require.NoError(t, err)
		require.NotContains(t, string(data), canary)
		require.False(t, bytes.Contains(data, image))
		require.False(t, bytes.Contains(data, digest[:]))
	}
	var memories, pipelines int
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM memories`).Scan(&memories))
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM pipeline_messages`).Scan(&pipelines))
	require.Zero(t, memories)
	require.Zero(t, pipelines)
}

func TestPrivateMediaRowSwapsAndTamperingFailClosed(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	identifier, otherID := uuid.NewString(), uuid.NewString()
	image := privateMediaJPEG(1280, 720, "first-canary")
	_, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	_, err = media.Get(t.Context(), privateMediaOther, identifier)
	require.ErrorIs(t, err, ErrPrivateMediaNotFound)
	_, err = media.Put(t.Context(), privateMediaOther, otherID, 0, privateMediaJPEG(1280, 720, "second-canary"))
	require.NoError(t, err)
	var original, second []byte
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT sealed_record FROM private_jpeg_objects WHERE agent_id=?`, privateMediaActor).Scan(&original))
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT sealed_record FROM private_jpeg_objects WHERE agent_id=?`, privateMediaOther).Scan(&second))
	flipped := append([]byte(nil), original...)
	flipped[len(flipped)-1] ^= 1
	for _, bad := range [][]byte{second, flipped, original[:len(original)-1], bytes.Repeat([]byte{0}, maxPrivateJPEGSealed+1)} {
		_, err := sqlite.writeExecContext(t.Context(), `UPDATE private_jpeg_objects SET sealed_record=? WHERE agent_id=?`, bad, privateMediaActor)
		require.NoError(t, err)
		_, err = media.Get(t.Context(), privateMediaActor, identifier)
		require.ErrorIs(t, err, ErrPrivateMediaCorrupt)
		_, err = media.Put(t.Context(), privateMediaActor, identifier, 0, image)
		require.ErrorIs(t, err, ErrPrivateMediaCorrupt)
	}
}

func TestPrivateMediaAuthenticatedMetadataAndWrongKeyRejected(t *testing.T) {
	sqlite, media, active, _ := privateMediaFixture(t)
	identifier := uuid.NewString()
	image := privateMediaJPEG(1280, 720, "metadata-canary")
	_, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	for _, offset := range []int{0, len(privateJPEGSchema), len(privateJPEGSchema) + 64,
		len(privateJPEGSchema) + 100, privateJPEGHeader - sha256.Size - 1, privateJPEGHeader - 1} {
		plain := privateJPEGPlaintext(privateMediaActor, identifier, image)
		plain[offset] ^= 1
		sealed, err := active.Encrypt(plain)
		require.NoError(t, err)
		_, err = sqlite.writeExecContext(t.Context(), `UPDATE private_jpeg_objects SET sealed_record=?`, sealed)
		require.NoError(t, err)
		_, err = media.Get(t.Context(), privateMediaActor, identifier)
		require.ErrorIs(t, err, ErrPrivateMediaCorrupt)
	}
	keyPath := filepath.Join(t.TempDir(), "other-synthetic.key")
	require.NoError(t, vault.Init(keyPath, "other-passphrase"))
	wrong, err := vault.Open(keyPath, "other-passphrase")
	require.NoError(t, err)
	valid, err := active.Encrypt(privateJPEGPlaintext(privateMediaActor, identifier, image))
	require.NoError(t, err)
	_, err = sqlite.writeExecContext(t.Context(), `UPDATE private_jpeg_objects SET sealed_record=?`, valid)
	require.NoError(t, err)
	sqlite.SetVault(wrong)
	_, err = media.Get(t.Context(), privateMediaActor, identifier)
	require.ErrorIs(t, err, ErrPrivateMediaCorrupt)
	_, err = media.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.ErrorIs(t, err, ErrPrivateMediaCorrupt)
}

func TestPrivateMediaQuotasCountCiphertextAndDoNotConsumeWorkflowQuota(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	image := privateMediaJPEG(1280, 720, "quota-canary")
	cipherBytes := int64(privateJPEGHeader + len(image) + 28)
	quota := media.quota
	quota.ActorBytes, quota.NodeBytes = cipherBytes, 2*cipherBytes
	limited, err := NewPrivateMediaStore(sqlite, quota, media.space)
	require.NoError(t, err)
	identifier := uuid.NewString()
	_, err = limited.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	_, err = limited.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, image)
	require.ErrorIs(t, err, ErrPrivateMediaQuota)
	_, err = limited.Put(t.Context(), privateMediaActor, identifier, 0, image)
	require.NoError(t, err)
	_, err = limited.Put(t.Context(), privateMediaOther, uuid.NewString(), 0, image)
	require.NoError(t, err)
	_, err = limited.Put(t.Context(), "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", uuid.NewString(), 0, image)
	require.ErrorIs(t, err, ErrPrivateMediaQuota)
	_, err = media.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, image)
	require.NoError(t, err)
	quota.ActorBytes, quota.NodeBytes = 8<<20, 16<<20
	quota.ActorObjects, quota.NodeObjects = 2, 3
	limited, err = NewPrivateMediaStore(sqlite, quota, media.space)
	require.NoError(t, err)
	_, err = limited.Put(t.Context(), privateMediaOther, uuid.NewString(), 0, image)
	require.ErrorIs(t, err, ErrPrivateMediaQuota)
	var count int
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM workflow_journal`).Scan(&count))
	require.Zero(t, count)
}

func TestPrivateMediaDiskFloorAndProbeFailuresLeaveNoRows(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	image := privateMediaJPEG(1280, 720, "free-floor")
	need := media.quota.MinFreeBytes + 4*int64(privateJPEGHeader+len(image)+28) + 65536
	for _, probe := range []PrivateMediaSpaceProbe{
		func(context.Context) (int64, error) { return 0, nil },
		func(context.Context) (int64, error) { return -1, nil },
		func(context.Context) (int64, error) { return need - 1, nil },
		func(context.Context) (int64, error) { return 1 << 30, errors.New("synthetic-private-path") },
	} {
		limited, err := NewPrivateMediaStore(sqlite, media.quota, probe)
		require.NoError(t, err)
		_, err = limited.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, image)
		require.ErrorIs(t, err, ErrPrivateMediaDiskBudget)
		require.NotContains(t, err.Error(), "synthetic-private-path")
	}
	var count int
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM private_jpeg_objects`).Scan(&count))
	require.Zero(t, count)
	limited, err := NewPrivateMediaStore(sqlite, media.quota, func(context.Context) (int64, error) { return need, nil })
	require.NoError(t, err)
	_, err = limited.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, image)
	require.NoError(t, err)
}

func TestPrivateMediaConcurrentQuotaAndImmutableID(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	quota := media.quota
	quota.ActorObjects, quota.NodeObjects = 1, 1
	limited, err := NewPrivateMediaStore(sqlite, quota, media.space)
	require.NoError(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := limited.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, privateMediaJPEG(1280, 720, "race"))
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	success, rejected := 0, 0
	for result := range results {
		if result == nil {
			success++
		} else {
			require.ErrorIs(t, result, ErrPrivateMediaQuota)
			rejected++
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, rejected)
}

func TestPrivateMediaCancelledWriteAndInvalidConfiguration(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := media.Put(ctx, privateMediaActor, uuid.NewString(), 0, privateMediaJPEG(1280, 720, "cancel"))
	require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
	_, err = NewPrivateMediaStore(sqlite, media.quota, nil)
	require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	quota := media.quota
	quota.ActorBytes = quota.NodeBytes + 1
	_, err = NewPrivateMediaStore(sqlite, quota, media.space)
	require.ErrorIs(t, err, ErrPrivateMediaInvalid)
	plain := privateJPEGPlaintext(privateMediaActor, uuid.NewString(), privateMediaJPEG(1, 1, "x"))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(plain[len(privateJPEGSchema)+100:]))
}

func TestPrivateMediaQuotaAcrossDatabaseHandles(t *testing.T) {
	sqlite, media, active, path := privateMediaFixture(t)
	other, err := NewSQLiteStore(t.Context(), path)
	require.NoError(t, err)
	defer other.Close()
	other.SetVaultExpected(true)
	other.SetVault(active)
	quota := media.quota
	quota.ActorObjects, quota.NodeObjects = 1, 1
	first, err := NewPrivateMediaStore(sqlite, quota, media.space)
	require.NoError(t, err)
	second, err := NewPrivateMediaStore(other, quota, media.space)
	require.NoError(t, err)
	start, results := make(chan struct{}), make(chan error, 2)
	for _, current := range []*PrivateMediaStore{first, second} {
		go func(current *PrivateMediaStore) {
			<-start
			_, err := current.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, privateMediaJPEG(1280, 720, "cross-handle"))
			results <- err
		}(current)
	}
	close(start)
	succeeded := 0
	for range 2 {
		select {
		case result := <-results:
			if result == nil {
				succeeded++
			} else {
				require.True(t, errors.Is(result, ErrPrivateMediaQuota) || errors.Is(result, ErrPrivateMediaUnavailable))
			}
		case <-time.After(5 * time.Second):
			t.Fatal("private media quota contention did not finish")
		}
	}
	require.Equal(t, 1, succeeded)
	var count int
	require.NoError(t, sqlite.conn.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM private_jpeg_objects`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestPrivateMediaProbeDoesNotHoldVaultGate(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	entered, release, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	guarded, err := NewPrivateMediaStore(sqlite, media.quota, func(ctx context.Context) (int64, error) {
		close(entered)
		select {
		case <-release:
			return 1 << 30, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := guarded.Put(t.Context(), privateMediaActor, uuid.NewString(), 0, privateMediaJPEG(1280, 720, "vault-gate"))
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not reach space probe")
	}
	go func() { sqlite.SetVault(nil); close(published) }()
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("space probe blocked vault publication")
	}
	once.Do(func() { close(release) })
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
	case <-time.After(2 * time.Second):
		t.Fatal("write did not finish")
	}
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("vault publication did not resume")
	}
}

func TestPrivateMediaFailureAfterProbeRollsBackWithoutPlaintext(t *testing.T) {
	sqlite, media, _, _ := privateMediaFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	guarded, err := NewPrivateMediaStore(sqlite, media.quota, func(context.Context) (int64, error) {
		cancel()
		return 1 << 30, nil
	})
	require.NoError(t, err)
	identifier := uuid.NewString()
	_, err = guarded.Put(ctx, privateMediaActor, identifier, 0, privateMediaJPEG(1280, 720, "cancel-after-probe-canary"))
	require.ErrorIs(t, err, ErrPrivateMediaUnavailable)
	_, err = media.Get(t.Context(), privateMediaActor, identifier)
	require.ErrorIs(t, err, ErrPrivateMediaNotFound)
}

func TestPrivateMediaConcurrentConflictingSameIDNeverOverwrites(t *testing.T) {
	_, media, _, _ := privateMediaFixture(t)
	identifier := uuid.NewString()
	images := [][]byte{privateMediaJPEG(1280, 720, "first-original"), privateMediaJPEG(1280, 720, "second-original")}
	start, results := make(chan struct{}), make(chan error, 2)
	for _, image := range images {
		go func(image []byte) {
			<-start
			_, err := media.Put(t.Context(), privateMediaActor, identifier, 0, image)
			results <- err
		}(image)
	}
	close(start)
	success := 0
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				success++
			} else {
				require.ErrorIs(t, err, ErrPrivateMediaConflict)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("same-ID write contention did not finish")
		}
	}
	require.Equal(t, 1, success)
	record, err := media.Get(t.Context(), privateMediaActor, identifier)
	require.NoError(t, err)
	require.True(t, bytes.Equal(record.JPEG, images[0]) || bytes.Equal(record.JPEG, images[1]))
}
