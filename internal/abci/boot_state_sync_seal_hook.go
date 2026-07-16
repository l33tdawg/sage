//go:build !v119testfixture

package abci

import "context"

func runBootStateSyncPrePublishHook(context.Context, int64, []byte, uint64) error { return nil }
