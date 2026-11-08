package snapshot

// sweep.go owns the snapshot inventory and housekeeping primitives:
//
//   SweepStaging — called once at boot to reap .staging-* dirs left
//                  by crashes mid-Take. Idempotent. No I/O on success.
//   KeepLast(K)  — retention policy. Keeps the K newest snapshots by
//                  capture time plus a bounded set of recent
//                  BinaryVersion anchors. Heights restart at 1 on a
//                  chain redeploy, so they are never a recency clock.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SweepStaging removes every dataDir/snapshots/.staging-* directory.
// Returns the count removed and the first error encountered (if any);
// removal continues past per-directory errors so a single permission
// issue doesn't strand the rest.
func SweepStaging(dataDir string) (int, error) {
	root := filepath.Join(dataDir, snapshotsDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("sweep: read snapshots dir: %w", err)
	}
	removed := 0
	var firstErr error
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), stagingPrefix) {
			continue
		}
		p := filepath.Join(root, e.Name())
		if rmErr := os.RemoveAll(p); rmErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove %s: %w", e.Name(), rmErr)
			}
			continue
		}
		removed++
	}
	return removed, firstErr
}

// snapshotEntry is an in-memory descriptor used by KeepLast.
type snapshotEntry struct {
	height        int64
	dir           string
	binaryVersion string
	chainID       string
	capturedAt    time.Time
}

// SnapshotInfo is a read-only inventory record for an on-disk snapshot.
type SnapshotInfo struct {
	Height        int64
	ChainID       string
	BinaryVersion string
	CapturedAt    time.Time
	Dir           string
}

// KeepLast deletes snapshots beyond the K newest capture times for the
// current chain, plus at most max(K, 2) recent BinaryVersion anchors.
// Capture time, chain identity, and a bounded anchor count keep retention
// finite across both chain redeploys and release upgrades.
//
// Legacy manifests without ChainID remain eligible. Manifest capture time is
// authoritative; directory mtime is only a compatibility fallback for older
// manifests that predate TakenAt.
//
// Snapshots without a valid OK sentinel are ignored entirely — they
// don't count toward K and they aren't removed (SweepStaging handles
// .staging-* dirs; anything else without OK is suspicious and left
// for human inspection).
//
// Returns the count removed and the first error.
func KeepLast(dataDir string, k int) (int, error) {
	if k < 0 {
		return 0, errors.New("KeepLast: k must be >= 0")
	}
	snaps, err := listSnapshots(dataDir)
	if err != nil {
		return 0, err
	}

	sortSnapshotsNewestFirst(snaps)

	// A chain redeploy restarts heights at 1. Keep the newest captures from
	// the current lineage, accepting legacy snapshots whose manifest omitted
	// ChainID. If the current chain has no snapshots yet, fall back to the
	// complete inventory rather than deleting every recovery point.
	eligible := snaps
	if currentChainID := readGenesisChainID(dataDir); currentChainID != "" {
		current := make([]snapshotEntry, 0, len(snaps))
		for _, s := range snaps {
			if s.chainID == "" || s.chainID == currentChainID {
				current = append(current, s)
			}
		}
		if len(current) > 0 {
			eligible = current
		}
	}

	// Mark the K newest captures as kept.
	kept := make(map[string]struct{})
	for i, s := range eligible {
		if i < k {
			kept[s.dir] = struct{}{}
		}
	}

	// Pick at most max(K, 2) recent version anchors so a downgrade target is
	// available without allowing one permanent snapshot per historical
	// release to grow without bound.
	anchorLimit := k
	if anchorLimit < 2 {
		anchorLimit = 2
	}
	anchorVersions := make(map[string]struct{}, anchorLimit)
	for _, s := range eligible {
		if s.binaryVersion == "" {
			continue
		}
		if _, ok := anchorVersions[s.binaryVersion]; ok {
			continue
		}
		if len(anchorVersions) >= anchorLimit {
			break
		}
		anchorVersions[s.binaryVersion] = struct{}{}
		kept[s.dir] = struct{}{}
	}

	removed := 0
	var firstErr error
	for _, s := range snaps {
		if _, ok := kept[s.dir]; ok {
			continue
		}
		if rmErr := os.RemoveAll(s.dir); rmErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove %s: %w", s.dir, rmErr)
			}
			continue
		}
		removed++
	}
	return removed, firstErr
}

// listSnapshots returns every OK-sentineled snapshot under
// dataDir/snapshots/. Heights are parsed from the directory name; preserved
// same-height anchors fall back to the manifest height.
func listSnapshots(dataDir string) ([]snapshotEntry, error) {
	root := filepath.Join(dataDir, snapshotsDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	out := make([]snapshotEntry, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), stagingPrefix) {
			continue
		}
		dir := filepath.Join(root, e.Name())
		if _, err := os.Stat(filepath.Join(dir, OKSentinel)); err != nil {
			continue
		}
		manifestBytes, readErr := os.ReadFile(filepath.Join(dir, chunkManifest))
		var version string
		var chainID string
		var manifestHeight int64
		var takenAt time.Time
		if readErr == nil {
			var m Manifest
			if jsonErr := json.Unmarshal(manifestBytes, &m); jsonErr == nil {
				version = m.BinaryVersion
				chainID = m.ChainID
				manifestHeight = m.Height
				takenAt = m.TakenAt
			}
		}
		h, parseErr := strconv.ParseInt(e.Name(), 10, 64)
		if parseErr != nil {
			if !strings.HasPrefix(e.Name(), "anchor-") || manifestHeight <= 0 {
				continue
			}
			h = manifestHeight
		}
		if takenAt.IsZero() {
			if info, infoErr := e.Info(); infoErr == nil {
				takenAt = info.ModTime()
			}
		}
		out = append(out, snapshotEntry{
			height: h, dir: dir, binaryVersion: version, chainID: chainID, capturedAt: takenAt.UTC(),
		})
	}
	return out, nil
}

func sortSnapshotsNewestFirst(snaps []snapshotEntry) {
	sort.SliceStable(snaps, func(i, j int) bool {
		if !snaps[i].capturedAt.Equal(snaps[j].capturedAt) {
			return snaps[i].capturedAt.After(snaps[j].capturedAt)
		}
		if snaps[i].height != snaps[j].height {
			return snaps[i].height > snaps[j].height
		}
		return snaps[i].dir > snaps[j].dir
	})
}

// ListSnapshotInfo returns the complete snapshot inventory newest-capture
// first. It includes chain and version metadata so callers can distinguish
// equal heights from different lineages or binary versions.
func ListSnapshotInfo(dataDir string) ([]SnapshotInfo, error) {
	snaps, err := listSnapshots(dataDir)
	if err != nil {
		return nil, err
	}
	sortSnapshotsNewestFirst(snaps)
	out := make([]SnapshotInfo, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, SnapshotInfo{
			Height: s.height, ChainID: s.chainID, BinaryVersion: s.binaryVersion,
			CapturedAt: s.capturedAt, Dir: s.dir,
		})
	}
	return out, nil
}

// ListSnapshots returns the heights of every OK-sentineled snapshot
// in dataDir, newest-capture first. Exposed for callers that want the
// snapshot inventory without parsing the directory themselves.
func ListSnapshots(dataDir string) ([]int64, error) {
	snaps, err := listSnapshots(dataDir)
	if err != nil {
		return nil, err
	}
	sortSnapshotsNewestFirst(snaps)
	heights := make([]int64, 0, len(snaps))
	for _, s := range snaps {
		heights = append(heights, s.height)
	}
	return heights, nil
}
