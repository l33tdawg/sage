package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestFederationListenerWriteTimeoutCoversConsensusCommitBudget(t *testing.T) {
	// Authenticated receipt and ceremony operations may wait through the
	// ordinary 60-second broadcast_tx_commit budget. The peer Write preview is
	// fail-closed, but the listener still needs headroom for these live routes.
	const consensusCommitBudget = 60 * time.Second
	if federationListenerWriteTimeout <= consensusCommitBudget {
		t.Fatalf("federation listener write timeout %s must exceed consensus commit budget %s", federationListenerWriteTimeout, consensusCommitBudget)
	}
}

func TestFederationPreviewGrantCleanupPrecedesAPIListenersAndManager(t *testing.T) {
	source, err := os.ReadFile("node.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	cleanup := strings.Index(text, "dashboard.ReconcileFederationManagedGrants(ctx)")
	listenerBind := strings.Index(text, "restListener, restListenErr :=")
	manager := strings.Index(text, "var fedMgr *federation.Manager")
	if cleanup < 0 || listenerBind < 0 || manager < 0 {
		t.Fatalf("serving-order markers missing: cleanup=%d listener=%d manager=%d", cleanup, listenerBind, manager)
	}
	if cleanup > listenerBind || cleanup > manager {
		t.Fatalf("unsafe federation grant cleanup must run before listener bind and independently of Manager: cleanup=%d listener=%d manager=%d", cleanup, listenerBind, manager)
	}
	if strings.Contains(text, "SetLocalRESTHandler") {
		t.Fatal("federation Manager must not receive a reusable local submit router")
	}
}
