package p2p

import (
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// streamConn adapts one multiplexed libp2p stream to net.Conn so the existing
// federation TLS and HTTP stacks can run inside it unchanged.
type streamConn struct {
	network.Stream
	releaseOnce sync.Once
	onRelease   func()
}

func newStreamConn(stream network.Stream) net.Conn {
	return &streamConn{Stream: stream}
}

func newTrackedStreamConn(stream network.Stream, onRelease func()) *streamConn {
	return &streamConn{Stream: stream, onRelease: onRelease}
}

func (c *streamConn) Close() error {
	err := c.Stream.Close()
	c.release()
	return err
}

func (c *streamConn) release() {
	c.releaseOnce.Do(func() {
		if c.onRelease != nil {
			c.onRelease()
		}
	})
}

func (c *streamConn) LocalAddr() net.Addr {
	conn := c.Conn()
	return streamAddr{value: withPeer(conn.LocalMultiaddr(), conn.LocalPeer().String())}
}

func (c *streamConn) RemoteAddr() net.Addr {
	conn := c.Conn()
	return streamAddr{value: withPeer(conn.RemoteMultiaddr(), conn.RemotePeer().String())}
}

func (c *streamConn) SetDeadline(deadline time.Time) error {
	return c.Stream.SetDeadline(deadline)
}

func (c *streamConn) SetReadDeadline(deadline time.Time) error {
	return c.Stream.SetReadDeadline(deadline)
}

func (c *streamConn) SetWriteDeadline(deadline time.Time) error {
	return c.Stream.SetWriteDeadline(deadline)
}

type streamAddr struct {
	value string
}

func (streamAddr) Network() string  { return "libp2p" }
func (a streamAddr) String() string { return a.value }

func withPeer(addr ma.Multiaddr, peerID string) string {
	peerPart, err := ma.NewMultiaddr("/p2p/" + peerID)
	if err != nil {
		return addr.String()
	}
	return addr.Encapsulate(peerPart).String()
}
