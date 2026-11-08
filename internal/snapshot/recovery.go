package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	recoveryDirName      = "recovery"
	recoveryBinaryPrefix = "sage-gui-"
	recoveryTempPrefix   = ".sage-gui-recovery-"
	recoveryTempMaxAge   = time.Hour
)

// DefaultRecoveryKeep retains the running executable plus one prior fallback.
const DefaultRecoveryKeep = 2

// PruneRecoveryBinaries bounds the executable fallback cache under
// dataDir/recovery. protectedPath, when non-empty, is retained even if its
// mtime is older than other entries; up to keep files are retained overall.
// Crashed-copy temp files older than an hour are reaped; newer temp files may
// belong to an in-flight copy. Unknown files are left in place for operator
// inspection.
func PruneRecoveryBinaries(dataDir, protectedPath string, keep int) (int, error) {
	if keep < 1 {
		return 0, errors.New("recovery retention requires keep >= 1")
	}
	root := filepath.Join(dataDir, recoveryDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read recovery directory: %w", err)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return 0, fmt.Errorf("resolve recovery directory: %w", err)
	}
	protected := ""
	if protectedPath != "" {
		abs, absErr := filepath.Abs(protectedPath)
		if absErr != nil {
			return 0, fmt.Errorf("resolve protected recovery executable: %w", absErr)
		}
		if filepath.Dir(abs) == rootAbs {
			protected = abs
		}
	}

	type candidate struct {
		path    string
		modTime int64
	}
	candidates := make([]candidate, 0, len(entries))
	removed := 0
	var firstErr error
	remove := func(path string) {
		if rmErr := os.Remove(path); rmErr != nil {
			if os.IsNotExist(rmErr) {
				return
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("remove %s: %w", filepath.Base(path), rmErr)
			}
			return
		}
		removed++
	}

	for _, entry := range entries {
		path := filepath.Join(rootAbs, entry.Name())
		info, infoErr := entry.Info()
		if infoErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("inspect %s: %w", entry.Name(), infoErr)
			}
			continue
		}
		if strings.HasPrefix(entry.Name(), recoveryTempPrefix) {
			if !info.IsDir() && time.Since(info.ModTime()) > recoveryTempMaxAge {
				remove(path)
			}
			continue
		}
		if !info.Mode().IsRegular() || !strings.HasPrefix(entry.Name(), recoveryBinaryPrefix) {
			continue
		}
		candidates = append(candidates, candidate{path: path, modTime: info.ModTime().UnixNano()})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime != candidates[j].modTime {
			return candidates[i].modTime > candidates[j].modTime
		}
		return candidates[i].path > candidates[j].path
	})

	kept := 0
	if protected != "" {
		kept = 1
	}
	for _, candidate := range candidates {
		if candidate.path == protected {
			continue
		}
		if kept < keep {
			kept++
			continue
		}
		remove(candidate.path)
	}

	if removed > 0 {
		dir, openErr := os.Open(root)
		if openErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("open recovery directory for sync: %w", openErr)
			}
		} else {
			syncErr := dir.Sync()
			closeErr := dir.Close()
			if firstErr == nil && (syncErr != nil || closeErr != nil) {
				firstErr = errors.Join(syncErr, closeErr)
			}
		}
	}
	return removed, firstErr
}
