package federation

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

const (
	// maxFanOutConcurrency bounds simultaneous outbound peer queries in one
	// recall fan-out.
	maxFanOutConcurrency = 8
	// maxMergedPerPeer caps how many results a single peer may contribute to a
	// merged recall response (defense against a peer ignoring our topK).
	maxMergedPerPeer = maxFedTopK
	// maxMergedBytesPerPeer bounds the total content bytes a single peer may
	// contribute. The count cap alone still lets a peer pack up to
	// maxFedResponseBytes (16 MiB) into a few huge results, and a wildcard
	// fan-out retains every peer's slice at once, so without a byte budget one
	// recall could hold N x 16 MiB.
	maxMergedBytesPerPeer = 1 << 20 // 1 MiB merged content per peer
	// maxResultContentBytes bounds a single federated result's content so one
	// oversized entry cannot blow the per-peer budget on its own.
	maxResultContentBytes = 64 << 10 // 64 KiB
)

// FanOutRecall is the OFF-consensus recall proxy (Mode 1 — exchange): it
// queries the requested peers concurrently and returns per-peer outcomes for
// the REST layer to merge into its response. Foreign results are stamped with
// SourceChainID provenance and chain-qualified SubmittingAgent here, so every
// consumer sees them; they are NEVER written to any store — merge-in-response
// only.
//
// targets semantics: nil/empty or a single "*" fans out to every active
// agreement; otherwise each named chain is resolved individually and a
// non-active/unknown agreement becomes an error OUTCOME (disclosed, not
// silently dropped).
func (m *Manager) FanOutRecall(ctx context.Context, targets []string, qr *QueryRequest) []PeerRecallOutcome {
	var chains []string
	wildcard := len(targets) == 0 || (len(targets) == 1 && targets[0] == "*")
	if wildcard {
		for _, agreement := range m.ActiveAgreements() {
			chains = append(chains, agreement.RemoteChainID)
		}
	} else {
		seen := make(map[string]bool)
		for _, t := range targets {
			t = strings.TrimSpace(t)
			if t == "" || seen[t] {
				continue
			}
			seen[t] = true
			chains = append(chains, t)
		}
	}
	if len(chains) == 0 {
		return nil
	}

	// Bound concurrent outbound peer connections: a wildcard fan-out over many
	// agreements (or a malicious local caller repeating federated=true) would
	// otherwise open one goroutine + fresh TLS handshake per agreement with no
	// ceiling.
	sem := make(chan struct{}, maxFanOutConcurrency)
	outcomes := make([]PeerRecallOutcome, len(chains))
	var wg sync.WaitGroup
	for i, chain := range chains {
		wg.Add(1)
		go func(i int, chain string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			outcome := PeerRecallOutcome{ChainID: chain}
			resp, err := m.QueryPeer(ctx, chain, qr)
			if err != nil {
				outcome.Err = err
			} else if resp.ChainID != chain {
				// A peer serving under agreement X must identify as X.
				outcome.Err = fmt.Errorf("peer identifies as %q, agreement expects %q", resp.ChainID, chain)
			} else {
				results := resp.Results
				// Cap per-peer results by COUNT and by BYTES: a malicious peer can
				// return up to maxFedResponseBytes of JSON regardless of our topK,
				// and the merge appends them all into the local caller's response.
				if len(results) > maxMergedPerPeer {
					results = results[:maxMergedPerPeer]
				}
				budget := maxMergedBytesPerPeer
				kept := results[:0]
				for _, res := range results {
					if res == nil || budget <= 0 {
						break
					}
					if len(res.Content) > maxResultContentBytes {
						res.Content = res.Content[:maxResultContentBytes] // bound one oversized entry
					}
					kept = append(kept, res)
					budget -= len(res.Content) + len(res.SubmittingAgent) + len(res.DomainTag) + 128
				}
				results = kept
				for _, res := range results {
					// SourceChainID is authoritative (we set it to the peer we
					// actually authenticated + queried). SubmittingAgent is
					// peer-controlled, so re-derive its chain qualifier from the
					// trusted SourceChainID — strip any peer-supplied "@suffix"
					// (which could spoof provenance as a third chain).
					res.SourceChainID = chain
					if res.SubmittingAgent != "" {
						base := res.SubmittingAgent
						if at := strings.IndexByte(base, '@'); at >= 0 {
							base = base[:at]
						}
						res.SubmittingAgent = base + "@" + chain
					}
				}
				outcome.Results = results
			}
			outcomes[i] = outcome
		}(i, chain)
	}
	wg.Wait()
	return outcomes
}
