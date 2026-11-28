package carrierio

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
)

type ctxConn struct{ ctx context.Context }

func (c ctxConn) Context() context.Context { return c.ctx }

// closedSocket reports whether pc was closed.
func closedSocket(pc net.PacketConn) bool {
	_, err := pc.WriteTo([]byte{0}, pc.LocalAddr())
	return errors.Is(err, net.ErrClosed)
}

// DialPacket refuses an address with no host, and closes the socket when
// the dial fails and once a dialled connection ends.
func TestDialPacketOwnsSocket(t *testing.T) {
	ctx := context.Background()
	_, _, err := DialPacket(ctx, ":443", func(net.PacketConn, net.Addr) (ctxConn, error) {
		t.Fatal("dialled with no host")
		return ctxConn{}, nil
	})
	if err == nil {
		t.Fatal("an address with no host")
	}
	errDial := errors.New("dial failed")
	var sock net.PacketConn
	_, _, err = DialPacket(ctx, "127.0.0.1:9", func(pc net.PacketConn, _ net.Addr) (ctxConn, error) {
		sock = pc
		return ctxConn{}, errDial
	})
	if !errors.Is(err, errDial) {
		t.Fatal(err)
	}
	if !closedSocket(sock) {
		t.Fatal("the socket of a failed dial is open")
	}
	synctest.Test(t, func(t *testing.T) {
		cctx, cancel := context.WithCancel(context.Background())
		_, done, err := DialPacket(cctx, "127.0.0.1:9", func(pc net.PacketConn, dst net.Addr) (ctxConn, error) {
			if pc.LocalAddr().(*net.UDPAddr).IP.To4() == nil || dst.String() != "127.0.0.1:9" {
				t.Errorf("socket %v for %v", pc.LocalAddr(), dst)
			}
			sock = pc
			return ctxConn{cctx}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if closedSocket(sock) {
			t.Fatal("the socket of a live connection is closed")
		}
		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("the ended connection did not signal socket closure")
		}
		if !closedSocket(sock) {
			t.Fatal("the socket of an ended connection is open")
		}
	})
}
