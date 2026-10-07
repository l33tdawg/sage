package embedding

import (
	"fmt"
	"os"
	"time"
)

const defaultHTTPTimeout = 30 * time.Second

// HTTPCallBudget is the maximum duration of an Ollama embedding call, including
// transient retries. Other HTTP providers use at most one attempt, so this is
// also a conservative budget for their callers. Saturate before multiplication
// so a valid but very large configured duration cannot wrap into a short wait.
func HTTPCallBudget() time.Duration {
	timeout := resolveHTTPTimeout()
	var backoff time.Duration
	for _, delay := range embedRetryBackoffs {
		backoff += delay
	}
	const maxDuration = time.Duration(1<<63 - 1)
	attempts := time.Duration(len(embedRetryBackoffs) + 1)
	if timeout > (maxDuration-backoff)/attempts {
		return maxDuration
	}
	return attempts*timeout + backoff
}

// resolveHTTPTimeout keeps the historical 30-second default while allowing
// CPU-only deployments to budget for model queueing. SAGE_EMBED_TIMEOUT is
// accepted as a compatibility alias for the issue's original proposal.
func resolveHTTPTimeout() time.Duration {
	value := os.Getenv("SAGE_EMBEDDING_TIMEOUT")
	name := "SAGE_EMBEDDING_TIMEOUT"
	if value == "" {
		value = os.Getenv("SAGE_EMBED_TIMEOUT")
		name = "SAGE_EMBED_TIMEOUT"
	}
	if value == "" {
		return defaultHTTPTimeout
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		fmt.Fprintf(os.Stderr, "SAGE: ignoring invalid %s=%q (want a positive duration like 60s); using %s\n", name, value, defaultHTTPTimeout)
		return defaultHTTPTimeout
	}
	return timeout
}
