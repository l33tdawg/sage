//go:build !linux && !darwin

package main

import (
	"errors"

	"github.com/l33tdawg/sage/internal/store"
)

func newPrivateMediaSpaceProbe(string) (store.PrivateMediaSpaceProbe, func(), error) {
	return nil, nil, errors.New("private media filesystem probe unsupported on this platform")
}
