package main

import (
	"os"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
)

func TestFederationListenerWriteTimeoutsCoverJoinBudgets(t *testing.T) {
	peerBudget := federation.JoinConfirmationPeerTimeout()
	if federationListenerWriteTimeout <= peerBudget {
		t.Fatalf("federation listener write timeout %s must exceed peer confirmation budget %s",
			federationListenerWriteTimeout, peerBudget)
	}
	operationBudget := federation.JoinConfirmationOperationTimeout()
	if restListenerWriteTimeout <= operationBudget {
		t.Fatalf("REST listener write timeout %s must exceed full two-commit confirmation budget %s",
			restListenerWriteTimeout, operationBudget)
	}
}

func TestEffectiveFederationListenAddr(t *testing.T) {
	for _, tc := range []struct {
		configured string
		want       string
	}{
		{"", defaultFederationListenAddr},
		{"   ", defaultFederationListenAddr},
		{"0.0.0.0:18444", "0.0.0.0:18444"},
		{" 127.0.0.1:19444 ", "127.0.0.1:19444"},
	} {
		if got := effectiveFederationListenAddr(tc.configured); got != tc.want {
			t.Errorf("effectiveFederationListenAddr(%q) = %q, want %q", tc.configured, got, tc.want)
		}
	}
}

func TestValidatedFederationListenAddrRejectsEphemeralAndInvalidPorts(t *testing.T) {
	for _, configured := range []string{"0.0.0.0:0", "127.0.0.1:0", "0.0.0.0:65536", "missing-port"} {
		if _, err := validatedFederationListenAddr(configured); err == nil {
			t.Errorf("validatedFederationListenAddr(%q) accepted an unadvertisable address", configured)
		}
	}
	for _, configured := range []string{"0.0.0.0:18444", "127.0.0.1:19444", "[::]:8444"} {
		if got, err := validatedFederationListenAddr(configured); err != nil || got != configured {
			t.Errorf("validatedFederationListenAddr(%q) = %q, %v", configured, got, err)
		}
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
