package statesync

import (
	"testing"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
	ssproto "github.com/cometbft/cometbft/proto/tendermint/statesync"
)

// stoppedPeer supplies the Peer surface through its embedded interface. The
// invalid-message path only needs IsRunning after it logs and rejects the peer.
type stoppedPeer struct {
	p2p.Peer
}

func (stoppedPeer) IsRunning() bool { return false }

func TestReceiveOversizedSnapshotWithoutActiveSyncDoesNotPanic(t *testing.T) {
	cfg := config.DefaultStateSyncConfig()
	reactor := NewReactor(*cfg, nil, nil, NopMetrics())
	reactor.SetSwitch(&p2p.Switch{})
	if err := reactor.Start(); err != nil {
		t.Fatalf("start reactor: %v", err)
	}
	t.Cleanup(func() {
		if err := reactor.Stop(); err != nil {
			t.Errorf("stop reactor: %v", err)
		}
	})

	reactor.Receive(p2p.Envelope{
		ChannelID: SnapshotChannel,
		Src:       stoppedPeer{},
		Message: &ssproto.SnapshotsResponse{
			Height: 1,
			Chunks: cfg.MaxSnapshotChunks + 1,
			Hash:   []byte{1},
		},
	})
}
