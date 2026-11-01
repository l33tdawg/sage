package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/require"
)

func TestFreshGenesisDoesNotUseHistoricalMemoryDisclosureFallback(t *testing.T) {
	for _, version := range []uint64{24, 25, 29, 30} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			s := newTestBadger(t)
			require.NoError(t, s.BootstrapFreshGenesis(FreshGenesisActivation{Version: FreshGenesisVersion, AppVersion: version, Scope: strings.Repeat("a", 64), BootstrapDigest: strings.Repeat("b", 64), RootID: strings.Repeat("c", 64), Validators: []FreshGenesisValidator{{ValidatorID: strings.Repeat("d", 64), Power: 10}}}))
			record := &memory.MemoryRecord{MemoryID: "incomplete-envelope", Content: "fresh content", ContentHash: memory.ComputeContentHash("fresh content"), DomainTag: "general", Status: memory.StatusCommitted}
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, "committed"))
			_, disposition, err := s.ClassifyMemoryProjection(record)
			if version >= 25 {
				require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
				require.Equal(t, MemoryProjectionQuarantined, disposition)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
