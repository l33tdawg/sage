package web

import (
	"context"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

type appV30TaskReadScopeKey struct{}

type appV30TaskReadScope struct {
	taint *store.TaintServingReadScope
	at    time.Time
}

// Keep the existing cold-header allowance across task pages and final rechecks.
// This does not impose the separate aggregate serving-unit limit.
func withAppV30TaskReadScope(ctx context.Context) context.Context {
	if scope, ok := ctx.Value(appV30TaskReadScopeKey{}).(*appV30TaskReadScope); ok && scope != nil {
		return ctx
	}
	return context.WithValue(ctx, appV30TaskReadScopeKey{}, appV30TaskReadScopeFromContext(ctx))
}

func appV30TaskReadScopeFromContext(ctx context.Context) *appV30TaskReadScope {
	if scope, ok := ctx.Value(appV30TaskReadScopeKey{}).(*appV30TaskReadScope); ok && scope != nil {
		return scope
	}
	return &appV30TaskReadScope{taint: store.NewTaintServingReadScope(), at: time.Now()}
}
