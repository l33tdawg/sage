//go:build !darwin

package web

import (
	"context"
	"fmt"
)

func platformPendingUpdateMarker(execPath string) string {
	return execPath + pendingUpdateSuffix
}

func installDarwinAppUpdate(context.Context, string, string) (string, error) {
	return "", fmt.Errorf("macOS app updates are unavailable on this platform")
}

func rollbackPendingAppBundle(string) (bool, bool, error) {
	return false, false, nil
}

func confirmPendingAppBundle(string) (bool, error) {
	return false, nil
}
