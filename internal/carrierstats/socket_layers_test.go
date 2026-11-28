package carrierstats

import (
	"net"
	"testing"
)

// layer is a host's wrapper that names the connection beneath it.
type layer struct{ net.Conn }

func (l layer) NetConn() net.Conn { return l.Conn }

// named is a carrier half that names its connection by CarrierConn, as a
// WebSocket carrier does, under a wrapper whose NetConn is nil.
type named struct{ c net.Conn }

func (n named) CarrierConn() any { return n.c }

type nilConn struct{ named }

func (nilConn) NetConn() net.Conn { return nil }

// self is a wrapper that names itself.
type self struct{ net.Conn }

func (s *self) NetConn() net.Conn { return s }

// TCPConnOf finds the socket under any stack of NetConn layers, and stops at
// a layer that names itself.
func TestTCPConnOfLayers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	tcp := c.(*net.TCPConn)
	if got := TCPConnOf(layer{layer{layer{tcp}}}); got != tcp {
		t.Fatalf("three layers: %v", got)
	}
	if got := TCPConnOf(nilConn{named{layer{tcp}}}); got != tcp {
		t.Fatalf("a CarrierConn under a nil NetConn: %v", got)
	}
	if got := TCPConnOf(&self{tcp}); got != nil {
		t.Fatalf("a self-naming layer: %v", got)
	}
}
