//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/l33tdawg/sage/internal/store"
	"golang.org/x/sys/unix"
)

func newPrivateMediaSpaceProbe(path string) (store.PrivateMediaSpaceProbe, func(), error) {
	if !filepath.IsAbs(path) {
		return nil, nil, errors.New("private media requires an absolute SQLite path")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errors.New("private media SQLite filesystem unavailable")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, nil, errors.New("private media requires a regular SQLite file")
	}
	var mutex sync.RWMutex
	closed := false
	probe := func(ctx context.Context) (int64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		mutex.RLock()
		defer mutex.RUnlock()
		if closed {
			return 0, errors.New("private media filesystem probe closed")
		}
		var stats unix.Statfs_t
		if err := unix.Fstatfs(int(file.Fd()), &stats); err != nil {
			return 0, errors.New("private media filesystem observation failed")
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if stats.Bsize <= 0 || stats.Bavail <= 0 || uint64(stats.Bavail) > uint64(math.MaxInt64)/uint64(stats.Bsize) {
			return 0, errors.New("invalid private media filesystem capacity")
		}
		return int64(uint64(stats.Bavail) * uint64(stats.Bsize)), nil
	}
	return probe, func() {
		mutex.Lock()
		defer mutex.Unlock()
		closed = true
		_ = file.Close()
	}, nil
}
