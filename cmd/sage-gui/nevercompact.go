package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Recall-backed compaction (a.k.a. "never-compact") captures the conversation
// turns that a harness evicts during full compaction as governed SAGE memories,
// so a later SessionStart can restore the thread verbatim instead of relying on a
// lossy model summary. This file owns the CONSENT and RETENTION product surface;
// hook_precompact.go owns capture and hook.go owns recall.
//
// Durable verbatim transcript retention is a new data flow, distinct from
// ordinary silent updater behavior, so it is DEFAULT-OFF and requires an explicit,
// versioned acknowledgment before any capture occurs.

const (
	// neverCompactConsentVersion pins the acknowledged capture scope. A material
	// expansion of what is captured MUST bump this so the user acknowledges again;
	// a stale version is treated as no consent.
	neverCompactConsentVersion = 1

	neverCompactDirName     = ".nevercompact"
	neverCompactConsentFile = "consent.json"

	// Tag grammar. Every captured chunk carries the thread tag (for thread-scoped
	// recall and purge) and a content-derived chunk tag (for idempotent
	// dedup/reconciliation). These are the durable governed identity of a capture.
	neverCompactThreadTagPrefix = "nc:thread:"
	neverCompactChunkTagPrefix  = "ncchunk:"
	neverCompactTag             = "nevercompact"
)

// neverCompactConsent is the persisted, versioned acknowledgment.
type neverCompactConsent struct {
	Version    int    `json:"version"`
	AcceptedAt string `json:"accepted_at"`
	Mode       string `json:"mode"` // "interactive" | "headless"
}

func neverCompactHomeDir() string {
	return filepath.Join(SageHome(), neverCompactDirName)
}

func neverCompactConsentPath() string {
	return filepath.Join(neverCompactHomeDir(), neverCompactConsentFile)
}

// neverCompactEnvOptIn reports the headless / centrally-managed opt-in. An env
// var is an acceptable consent surface ONLY where there is no interactive one;
// it stays default-off (unset/false → no capture).
func neverCompactEnvOptIn() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SAGE_NEVERCOMPACT"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func readNeverCompactConsent() (*neverCompactConsent, error) {
	raw, err := os.ReadFile(neverCompactConsentPath())
	if err != nil {
		return nil, err
	}
	var c neverCompactConsent
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse consent: %w", err)
	}
	return &c, nil
}

// neverCompactCapturePermitted is the single gate the capture path consults.
// Returns (permitted, mode). It is default-off: capture happens only when the
// user has recorded a current-version interactive acknowledgment, or an operator
// has set the headless env opt-in.
func neverCompactCapturePermitted() (bool, string) {
	if neverCompactEnvOptIn() {
		return true, "headless"
	}
	c, err := readNeverCompactConsent()
	if err != nil || c == nil {
		return false, ""
	}
	if c.Version != neverCompactConsentVersion {
		// Scope changed since the acknowledgment: require a fresh one.
		return false, ""
	}
	return true, "interactive"
}

// neverCompactDisclosure is the exact text shown before an interactive
// acknowledgment. It states what is captured, its classification, retention and
// deletion behavior, that governed records may persist beyond the local
// transcript, that consent is revocable without deleting prior records, and that
// the acknowledgment is versioned.
func neverCompactDisclosure() string {
	return strings.TrimSpace(fmt.Sprintf(`
Recall-backed compaction (never-compact) — please read before enabling.

WHAT IS CAPTURED
  When your harness compacts the conversation, the user and assistant TURNS that
  would be evicted are written VERBATIM to your SAGE node as governed memories, so
  a later session can restore the thread instead of a lossy summary. Tool-call
  inputs and outputs are NOT captured (they are re-derivable). Only full-compaction
  turns are captured; micro-compaction and /clear are not.

CLASSIFICATION
  Captured chunks are stored at classification %d (Confidential) by default and are
  never stored Public. Override with SAGE_NEVERCOMPACT_CLASSIFICATION (1..4).

RETENTION & DELETION
  Captured chunks are retained until you purge them:  sage-gui nevercompact purge.
  SAGE has no hard delete — purge issues a governed deprecation: the chunks are
  hidden from recall and search, but an audit row is retained on-chain, and because
  memory is governed, records may persist beyond your local transcript and on other
  nodes you federate with. Deletion is therefore best understood as "no longer
  recalled", not "erased everywhere".

CONSENT IS REVOCABLE (BUT NOT RETROACTIVE)
  sage-gui nevercompact disable stops all future capture. It does NOT delete
  records already captured; purge them explicitly if you want them gone.

VERSIONED
  This acknowledgment is version %d. If a future release materially expands what is
  captured, you will be asked to acknowledge again before that new capture begins.

Enabling is separate from ordinary updates: upgrades stay silent and automatic, but
turning on a new durable transcript data flow does not.`,
		neverCompactDefaultClassification, neverCompactConsentVersion))
}

// runNeverCompact implements `sage-gui nevercompact <enable|disable|status|purge>`.
func runNeverCompact(args []string) error {
	if len(args) == 0 {
		return neverCompactStatus()
	}
	switch args[0] {
	case "enable":
		return neverCompactEnable()
	case "disable":
		return neverCompactDisable()
	case "status":
		return neverCompactStatus()
	case "purge":
		return neverCompactPurge(args[1:])
	case "--help", "-h", "help":
		printNeverCompactUsage()
		return nil
	default:
		printNeverCompactUsage()
		return fmt.Errorf("nevercompact: unknown subcommand %q", args[0])
	}
}

func printNeverCompactUsage() {
	fmt.Fprintln(os.Stdout, "Usage: sage-gui nevercompact <command>")
	fmt.Fprintln(os.Stdout, "  enable    Show the disclosure and record consent to capture evicted turns")
	fmt.Fprintln(os.Stdout, "  disable   Stop future capture (does not delete already-captured records)")
	fmt.Fprintln(os.Stdout, "  status    Show whether capture is enabled and how")
	fmt.Fprintln(os.Stdout, "  purge [--thread ID | --all]   Deprecate captured records (hidden from recall)")
}

func neverCompactEnable() error {
	fmt.Println(neverCompactDisclosure())
	fmt.Println()
	fmt.Print("Type 'yes' to acknowledge and enable capture: ")
	var answer string
	// A non-interactive stdin (no tty) yields EOF/empty; refuse and point at the
	// env opt-in, which is the correct surface for headless/centrally-managed use.
	if _, err := fmt.Scanln(&answer); err != nil || strings.ToLower(strings.TrimSpace(answer)) != "yes" {
		fmt.Println("Not enabled. (For headless or centrally-managed deployments, set SAGE_NEVERCOMPACT=1 instead.)")
		return nil
	}
	c := neverCompactConsent{
		Version:    neverCompactConsentVersion,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
		Mode:       "interactive",
	}
	raw, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("encode consent: %w", err)
	}
	if err := neverCompactAtomicWrite(neverCompactConsentPath(), raw); err != nil {
		return fmt.Errorf("record consent: %w", err)
	}
	fmt.Println("Recall-backed compaction is ENABLED. Evicted turns will be captured on the next compaction.")
	return nil
}

func neverCompactDisable() error {
	err := os.Remove(neverCompactConsentPath())
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("revoke consent: %w", err)
	}
	fmt.Println("Recall-backed compaction is DISABLED for future compactions.")
	fmt.Println("Already-captured records are NOT deleted. Run 'sage-gui nevercompact purge' to deprecate them.")
	if neverCompactEnvOptIn() {
		fmt.Println("NOTE: SAGE_NEVERCOMPACT is still set in the environment, which re-enables capture. Unset it to fully disable.")
	}
	return nil
}

func neverCompactStatus() error {
	permitted, mode := neverCompactCapturePermitted()
	if permitted {
		fmt.Printf("Recall-backed compaction: ENABLED (%s consent, version %d).\n", mode, neverCompactConsentVersion)
	} else {
		if c, err := readNeverCompactConsent(); err == nil && c != nil && c.Version != neverCompactConsentVersion {
			fmt.Printf("Recall-backed compaction: DISABLED — a newer consent version (%d) is required; run 'sage-gui nevercompact enable'.\n", neverCompactConsentVersion)
		} else {
			fmt.Println("Recall-backed compaction: DISABLED (default). Run 'sage-gui nevercompact enable' to review and turn it on.")
		}
	}
	return nil
}

// neverCompactPurge deprecates captured records (governed soft-delete) for one
// thread or all threads. It resolves the caller's readable captured chunk IDs by
// tag, then issues a governed forget for each.
func neverCompactPurge(args []string) error {
	tag := neverCompactTag
	label := "all threads"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--all":
			// default
		case "--thread":
			i++
			if i >= len(args) {
				return fmt.Errorf("nevercompact purge: --thread requires a value")
			}
			thread := strings.TrimSpace(args[i]) //nolint:gosec // i is bounds-checked immediately above
			if thread == "" {
				return fmt.Errorf("nevercompact purge: --thread requires a value")
			}
			tag = neverCompactThreadTagPrefix + thread
			label = "thread " + thread
		default:
			return fmt.Errorf("nevercompact purge: unknown option %q", args[i])
		}
	}

	ids, err := neverCompactCapturedIDs(tag)
	if err != nil {
		return fmt.Errorf("resolve captured records: %w", err)
	}
	if len(ids) == 0 {
		fmt.Printf("No captured records found for %s.\n", label)
		return nil
	}
	deprecated := 0
	for _, id := range ids {
		body, _ := json.Marshal(map[string]any{"reason": "user purge of recall-backed capture"})
		if _, err := hookSignedRequest(http.MethodPost, "/v1/memory/"+url.PathEscape(id)+"/forget", body); err != nil {
			fmt.Fprintf(os.Stderr, "nevercompact purge: could not deprecate %s: %v\n", id, err)
			continue
		}
		deprecated++
	}
	fmt.Printf("Deprecated %d of %d captured record(s) for %s (hidden from recall; audit rows retained on-chain).\n",
		deprecated, len(ids), label)
	return nil
}

// neverCompactCapturedIDs lists the caller-readable memory IDs carrying the given
// tag, paginating to completeness.
func neverCompactCapturedIDs(tag string) ([]string, error) {
	seen := map[string]bool{}
	ids := []string{}
	const page = 200
	for offset := 0; ; offset += page {
		q := url.Values{}
		q.Set("tag", tag)
		q.Set("limit", fmt.Sprintf("%d", page))
		q.Set("offset", fmt.Sprintf("%d", offset))
		q.Set("sort", "oldest")
		resp, err := hookSignedRequest(http.MethodGet, "/v1/memory/list?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		var payload struct {
			Memories []struct {
				MemoryID string `json:"memory_id"`
			} `json:"memories"`
		}
		if err := json.Unmarshal(resp, &payload); err != nil {
			return nil, fmt.Errorf("parse list: %w", err)
		}
		if len(payload.Memories) == 0 {
			break
		}
		for _, m := range payload.Memories {
			if m.MemoryID != "" && !seen[m.MemoryID] {
				seen[m.MemoryID] = true
				ids = append(ids, m.MemoryID)
			}
		}
		if len(payload.Memories) < page {
			break
		}
	}
	return ids, nil
}

// neverCompactAtomicWrite writes data to path via a temp file + rename, creating
// the parent directory 0700 and the file 0600. Durable against a crash mid-write.
func neverCompactAtomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		// Windows will not replace an existing destination atomically.
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("replace: %w", removeErr)
		}
		if err := os.Rename(tmpPath, path); err != nil {
			return fmt.Errorf("rename: %w", err)
		}
	}
	return nil
}
