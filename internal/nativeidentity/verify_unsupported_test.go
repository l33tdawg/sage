//go:build !darwin || !cgo

package nativeidentity

import (
	"errors"
	"testing"
)

func TestUnsupportedBuildFailsClosed(t *testing.T) {
	if Supported() {
		t.Fatal("unsupported build advertised peer verification")
	}
	identity, err := Verify(nil, "always")
	if !errors.Is(err, ErrUnsupported) || identity != (Identity{}) {
		t.Fatalf("unsupported build returned identity: %+v / %v", identity, err)
	}
}
