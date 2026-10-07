package main

import (
	"strings"
	"testing"
)

// TestParseFenceAbandonArgsKeepsThePeerAcknowledgement pins the flag that makes
// this command reachable at all on a node that keeps a peer.
//
// The daemon's abandon route accepts connected peers behind a second
// acknowledgement (peer_redelivery_acknowledged) since v11.23.4, and
// validateAbandonEvidence enforces the same gate for every caller — including
// this one. Without a flag to carry that acknowledgement the CLI could only ever
// be refused on such a node, telling the operator to set a field the command had
// no way to set: the same "recovery route exists but is unreachable" defect the
// route was widened to fix.
func TestParseFenceAbandonArgsKeepsThePeerAcknowledgement(t *testing.T) {
	flags, err := parseFenceAbandonArgs([]string{
		"--signer", "5c9ea6448b3caf58",
		"--reason=unprovable since the June restart",
		"--acknowledge-payload-loss",
		"--peer-redelivery-acknowledged",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !flags.PeerRedeliveryAcknowledged {
		t.Fatal("--peer-redelivery-acknowledged was dropped; the unknown-flag path must never absorb it")
	}
	if !flags.PayloadLossAcknowledged {
		t.Fatal("--acknowledge-payload-loss was dropped")
	}
	if flags.Signer != "5c9ea6448b3caf58" {
		t.Fatalf("signer = %q, want the value passed", flags.Signer)
	}
	if flags.Reason != "unprovable since the June restart" {
		t.Fatalf("reason = %q, want the --reason= form to parse", flags.Reason)
	}
}

// TestFenceAbandonFlagsRequireTheTwoAcknowledgements keeps the refusals in the
// order an operator meets them, and pins that neither acknowledgement is
// optional: this decision is the only one in SAGE whose fate the chain never
// settled.
func TestFenceAbandonFlagsRequireTheTwoAcknowledgements(t *testing.T) {
	complete := fenceAbandonFlags{Signer: "ab12", Reason: "why", PayloadLossAcknowledged: true}
	if err := complete.validate(); err != nil {
		t.Fatalf("a complete decision was refused: %v", err)
	}

	missingPayload := fenceAbandonFlags{Signer: "ab12", Reason: "why"}
	if err := missingPayload.validate(); err == nil || !strings.Contains(err.Error(), "--acknowledge-payload-loss") {
		t.Fatalf("want the payload-loss acknowledgement to be required, got %v", err)
	}
	missingReason := fenceAbandonFlags{Signer: "ab12", PayloadLossAcknowledged: true}
	if err := missingReason.validate(); err == nil || !strings.Contains(err.Error(), "--reason") {
		t.Fatalf("want the reason to be required, got %v", err)
	}
	missingSigner := fenceAbandonFlags{Reason: "why", PayloadLossAcknowledged: true}
	if err := missingSigner.validate(); err == nil || !strings.Contains(err.Error(), "--signer") {
		t.Fatalf("want the signer to be required, got %v", err)
	}
}

// TestParseFenceAbandonArgsRejectsUnknownFlags is the sibling of the first test:
// a mistyped acknowledgement must fail loudly rather than parse as "no
// acknowledgement given", which is the shape that silently produced an
// unreachable recovery route in the field.
func TestParseFenceAbandonArgsRejectsUnknownFlags(t *testing.T) {
	if _, err := parseFenceAbandonArgs([]string{"--acknowledge-peer-redelivery"}); err == nil {
		t.Fatal("a mistyped flag was accepted; the acknowledgement would have been silently dropped")
	}
}
