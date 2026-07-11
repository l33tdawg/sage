package p2p

import (
	"net"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var errListenerClosed = net.ErrClosed

// Listener adapts inbound streams for one protocol to net.Listener. It owns
// only the stream handler; closing it does not close the shared libp2p host.
type Listener struct {
	host      host.Host
	protocol  protocol.ID
	incoming  chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
	stateMu   sync.RWMutex
	closed    bool
	addr      net.Addr
}

func newListener(h host.Host, protocolID protocol.ID, queue int) *Listener {
	if queue < 1 {
		queue = 64
	}
	l := &Listener{
		host:     h,
		protocol: protocolID,
		incoming: make(chan net.Conn, queue),
		done:     make(chan struct{}),
		addr:     streamAddr{value: withPeer(h.Addrs()[0], h.ID().String())},
	}
	h.SetStreamHandler(protocolID, l.handleStream)
	return l
}

func (l *Listener) handleStream(stream network.Stream) {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	if l.closed {
		_ = stream.Reset()
		return
	}
	conn := newStreamConn(stream)
	select {
	case l.incoming <- conn:
	case <-l.done:
		_ = stream.Reset()
	default:
		// Bound the unauthenticated pre-TLS queue. A peer that opens streams
		// faster than HTTP can accept them loses the excess stream rather than
		// consuming unbounded memory.
		_ = stream.Reset()
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.incoming:
		return conn, nil
	case <-l.done:
		return nil, errListenerClosed
	}
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.stateMu.Lock()
		defer l.stateMu.Unlock()
		l.closed = true
		l.host.RemoveStreamHandler(l.protocol)
		close(l.done)
		for {
			select {
			case conn := <-l.incoming:
				_ = conn.Close()
			default:
				return
			}
		}
	})
	return nil
}

func (l *Listener) Addr() net.Addr { return l.addr }
