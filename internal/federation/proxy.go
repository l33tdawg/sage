package federation

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

	outcomes := make([]PeerRecallOutcome, len(chains))
	var wg sync.WaitGroup
	for i, chain := range chains {
		wg.Add(1)
		go func(i int, chain string) {
			defer wg.Done()
			outcome := PeerRecallOutcome{ChainID: chain}
			resp, err := m.QueryPeer(ctx, chain, qr)
			if err != nil {
				outcome.Err = err
			} else if resp.ChainID != chain {
				// A peer serving under agreement X must identify as X.
				outcome.Err = fmt.Errorf("peer identifies as %q, agreement expects %q", resp.ChainID, chain)
			} else {
				for _, res := range resp.Results {
					res.SourceChainID = chain
					if res.SubmittingAgent != "" && !strings.Contains(res.SubmittingAgent, "@") {
						res.SubmittingAgent = res.SubmittingAgent + "@" + chain
					}
				}
				outcome.Results = resp.Results
			}
			outcomes[i] = outcome
		}(i, chain)
	}
	wg.Wait()
	return outcomes
}
