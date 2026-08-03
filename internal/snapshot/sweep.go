package snapshot

// sweep.go owns the housekeeping primitives:
//
//   SweepStaging — called once at boot to reap .staging-* dirs left
//                  by crashes mid-Take. Idempotent. No I/O on success.
//   KeepLast(K)  — retention policy. Keeps the K newest snapshots by
//                  height plus one "anchor" per distinct BinaryVersion
//                  (drives downgrade). Older non-anchor snapshots are
//                  removed.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
}

// KeepLast deletes snapshots beyond the K newest, EXCEPT for one
// anchor snapshot per distinct BinaryVersion (which is pinned and
// never removed regardless of K).
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

	// Newest-first by height.
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].height > snaps[j].height })

	// Pick anchors: one per distinct BinaryVersion. Prefer newer
	// snapshots as anchors so the anchor for v7.1.0 is the latest
	// v7.1.0 snapshot we have.
	anchors := make(map[string]string) // version → dir
	for _, s := range snaps {
		if s.binaryVersion == "" {
			continue
		}
		if _, ok := anchors[s.binaryVersion]; !ok {
			anchors[s.binaryVersion] = s.dir
		}
	}

	// Mark the K newest as kept.
	kept := make(map[string]struct{})
	for i, s := range snaps {
		if i < k {
			kept[s.dir] = struct{}{}
		}
	}
	for _, dir := range anchors {
		kept[dir] = struct{}{}
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
// dataDir/snapshots/. Heights are parsed from the directory name; a
// directory whose name doesn't parse as an integer is skipped.
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
		var manifestHeight int64
		if readErr == nil {
			var m Manifest
			if jsonErr := json.Unmarshal(manifestBytes, &m); jsonErr == nil {
				version = m.BinaryVersion
				manifestHeight = m.Height
			}
		}
		h, parseErr := strconv.ParseInt(e.Name(), 10, 64)
		if parseErr != nil {
			if !strings.HasPrefix(e.Name(), "anchor-") || manifestHeight <= 0 {
				continue
			}
			h = manifestHeight
		}
		out = append(out, snapshotEntry{height: h, dir: dir, binaryVersion: version})
	}
	return out, nil
}

// ListSnapshots returns the heights of every OK-sentineled snapshot
// in dataDir, newest-first. Exposed for callers that want to inspect
// the snapshot inventory without parsing the directory themselves.
func ListSnapshots(dataDir string) ([]int64, error) {
	snaps, err := listSnapshots(dataDir)
	if err != nil {
		return nil, err
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].height > snaps[j].height })
	heights := make([]int64, 0, len(snaps))
	for _, s := range snaps {
		heights = append(heights, s.height)
	}
	return heights, nil
}
