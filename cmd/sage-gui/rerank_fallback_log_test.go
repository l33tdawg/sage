package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRerankFallbackLoggerRateLimits(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	now := time.Unix(1_700_000_000, 0)
	observe := newRerankFallbackLogger(logger, func() time.Time { return now }, time.Minute)

	observe(100, context.DeadlineExceeded)
	require.Equal(t, 1, strings.Count(buf.String(), "\n"), "the first fallback is logged at once")
	require.Contains(t, buf.String(), `"candidates":100`)
	require.Contains(t, buf.String(), `"reason":"timeout"`)

	now = now.Add(10 * time.Second)
	observe(100, context.DeadlineExceeded)
	observe(100, context.DeadlineExceeded)
	require.Equal(t, 1, strings.Count(buf.String(), "\n"), "fallbacks inside the interval are folded, not logged")

	now = now.Add(time.Minute)
	observe(10, context.DeadlineExceeded)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	require.Contains(t, lines[1], `"fallbacks_since_last_log":2`)
	require.Contains(t, lines[1], `"candidates":10`)
}

func TestRerankFallbackLoggerDoesNotLogUpstreamContent(t *testing.T) {
	var buf bytes.Buffer
	observe := rerankFallbackLogger(zerolog.New(&buf))
	observe(10, errors.New("rerank http 400: private-query-and-document"))
	require.NotContains(t, buf.String(), "private-query-and-document")
	require.Contains(t, buf.String(), `"reason":"upstream_error_or_invalid_scores"`)
}
