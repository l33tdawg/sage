package p2p

import (
	"net"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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
	allowed   map[peer.ID]struct{}
	enforce   bool
	active    int
	byPeer    map[peer.ID]int
	maxActive int
	maxPeer   int
	// beforeEnqueue is a test-only barrier used to prove Close cannot overtake
	// an admitted handler. It is nil in production.
	beforeEnqueue func()
}

func newListener(h host.Host, protocolID protocol.ID, queue int, allowed map[peer.ID]struct{}, enforce bool, maxActive, maxPeer int) *Listener {
	if queue < 1 {
		queue = 64
	}
	if maxActive < 1 {
		maxActive = 128
	}
	if maxPeer < 1 {
		maxPeer = 16
	}
	l := &Listener{
		host:      h,
		protocol:  protocolID,
		incoming:  make(chan net.Conn, queue),
		done:      make(chan struct{}),
		addr:      streamAddr{value: withPeer(h.Addrs()[0], h.ID().String())},
		allowed:   allowed,
		enforce:   enforce,
		byPeer:    make(map[peer.ID]int),
		maxActive: maxActive,
		maxPeer:   maxPeer,
	}
	h.SetStreamHandler(protocolID, l.handleStream)
	return l
}

func (l *Listener) handleStream(stream network.Stream) {
	remotePeer := stream.Conn().RemotePeer()
	l.stateMu.Lock()
	if l.closed {
		l.stateMu.Unlock()
		_ = stream.Reset()
		return
	}
	if _, ok := l.allowed[remotePeer]; l.enforce && !ok {
		l.stateMu.Unlock()
		_ = stream.Reset()
		return
	}
	if l.active >= l.maxActive || l.byPeer[remotePeer] >= l.maxPeer {
		l.stateMu.Unlock()
		_ = stream.Reset()
		return
	}
	l.active++
	l.byPeer[remotePeer]++
	conn := newTrackedStreamConn(stream, func() { l.release(remotePeer) })
	if l.beforeEnqueue != nil {
		l.beforeEnqueue()
	}
	// Keep stateMu through the non-blocking handoff. Close must not mark the
	// listener closed and drain the queue between admission and enqueue.
	select {
	case l.incoming <- conn:
		l.stateMu.Unlock()
	default:
		// Bound the unauthenticated pre-TLS queue. A peer that opens streams
		// faster than HTTP can accept them loses the excess stream rather than
		// consuming unbounded memory.
		conn.releaseOnce.Do(func() {}) // release accounting explicitly under stateMu
		l.releaseLocked(remotePeer)
		l.stateMu.Unlock()
		_ = stream.Reset()
	}
}

func (l *Listener) release(remotePeer peer.ID) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	l.releaseLocked(remotePeer)
}

func (l *Listener) releaseLocked(remotePeer peer.ID) {
	if l.active > 0 {
		l.active--
	}
	if l.byPeer[remotePeer] <= 1 {
		delete(l.byPeer, remotePeer)
	} else {
		l.byPeer[remotePeer]--
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.incoming:
		l.stateMu.RLock()
		closed := l.closed
		l.stateMu.RUnlock()
		if closed {
			_ = conn.Close()
			return nil, errListenerClosed
		}
		return conn, nil
	case <-l.done:
		return nil, errListenerClosed
	}
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.stateMu.Lock()
		l.closed = true
		l.host.RemoveStreamHandler(l.protocol)
		close(l.done)
		l.stateMu.Unlock()
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
