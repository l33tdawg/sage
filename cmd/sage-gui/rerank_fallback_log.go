package main

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// rerankFallbackLogInterval bounds the fallback warning to one line per
// interval. A reranker that misses its timeout usually misses it on every
// recall, so per-call logging would flood the node log.
const rerankFallbackLogInterval = time.Minute

// rerankFallbackLogger returns the store's rerank fallback observer. It logs
// the first fallback at once, then at most one line per interval carrying the
// number of fallbacks folded into it, so a reranker that silently degrades
// every hybrid recall to RRF order is visible in the node log.
func rerankFallbackLogger(logger zerolog.Logger) func(candidates int, err error) {
	return newRerankFallbackLogger(logger, time.Now, rerankFallbackLogInterval)
}

func newRerankFallbackLogger(logger zerolog.Logger, now func() time.Time, interval time.Duration) func(candidates int, err error) {
	var mu sync.Mutex
	var last time.Time
	suppressed := 0
	return func(candidates int, err error) {
		mu.Lock()
		t := now()
		if !last.IsZero() && t.Sub(last) < interval {
			suppressed++
			mu.Unlock()
			return
		}
		folded := suppressed
		last, suppressed = t, 0
		mu.Unlock()
		logger.Warn().
			Err(err).
			Int("candidates", candidates).
			Int("fallbacks_since_last_log", folded).
			Msg("hybrid recall reranker failed; results kept RRF order (lower SAGE_RERANK_MAX_CANDIDATES or raise SAGE_RERANK_TIMEOUT_MS)")
	}
}
