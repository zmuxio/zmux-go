package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx/memnet"
	"github.com/djylb/timeshim"
)

// closeFormsPair starts a client over a and a server over b on the real
// clock (in a synctest bubble, its fake time) and has the client fill the
// server's window on a stream the server never reads, so that a graceful
// close has data to wait for.
func closeFormsPair(t *testing.T, a, b net.Conn) (client, server *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch := make(chan *Conn, 1)
	go func() {
		s, err := Server(ctx, b, &Config{ClockDomain: NewClockDomain(timeshim.Real), InitialStreamReceiveWindow: 64 << 10, MaxStreamReceiveWindow: 64 << 10})
		if err != nil {
			s = nil
		}
		ch <- s
	}()
	client, err := Client(ctx, a, &Config{ClockDomain: NewClockDomain(timeshim.Real)})
	if err != nil {
		t.Fatal(err)
	}
	if server = <-ch; server == nil {
		t.Fatal("no server session")
	}
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(make([]byte, 1<<20)) }()
	in, err := server.AcceptStream(ctx) // and never read
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtime.KeepAlive(in); runtime.KeepAlive(st) }) // no handle is collected
	if _, err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return client, server
}

// wantAppClose checks that err is the application close of code and
// reason, remote as given.
func wantAppClose(t *testing.T, who string, err error, code ApplicationErrorCode, reason string, remote bool) {
	t.Helper()
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.ErrorCode != code || ae.ErrorMessage != reason || ae.Remote != remote {
		t.Fatalf("%s ended with %v, want the application close %#x %q (remote %v)", who, err, uint64(code), reason, remote)
	}
}

// TestCloseForms (N17): a coded close then an abort ends the flush at once
// and sends the close's code; an abort then a coded close returns at once
// and sends nothing more; a coded close whose data the peer's window holds
// back gives the data up at 1 s and the peer still sees the code.
func TestCloseForms(t *testing.T) {
	t.Run("close then abort", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a, b := net.Pipe()
			client, server := closeFormsPair(t, a, b)
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				_ = client.CloseWithError(0x2a, "bye")
			}()
			synctest.Wait()
			select {
			case <-closed:
				t.Fatal("the close returned with data still waiting for credit")
			default:
			}
			client.Abort(5, "abort")
			synctest.Wait()
			select {
			case <-closed:
			default:
				t.Fatal("the abort did not release the close")
			}
			wantAppClose(t, "the peer", server.Wait(), 0x2a, "bye", true)
			wantAppClose(t, "the closer", client.Wait(), 0x2a, "bye", false)
		})
	})
	t.Run("abort then close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a, b := net.Pipe()
			client, server := closeFormsPair(t, a, b)
			t0 := time.Now()
			client.Abort(5, "abort")
			_ = client.CloseWithError(0x2a, "bye")
			if d := time.Since(t0); d != 0 {
				t.Fatalf("abort then close took %v", d)
			}
			wantAppClose(t, "the peer", server.Wait(), 5, "abort", true)
			wantAppClose(t, "the closer", client.Wait(), 5, "abort", false)
		})
	})
	t.Run("flush limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a, b := net.Pipe()
			client, server := closeFormsPair(t, a, b)
			t0 := time.Now()
			_ = client.CloseWithError(0x2a, "bye")
			if d := time.Since(t0); d != closeFlushLimit {
				t.Fatalf("the close took %v with unsent data, want the flush limit %v", d, closeFlushLimit)
			}
			// A later close or abort changes nothing.
			_ = client.CloseWithError(7, "again")
			client.Abort(8, "late")
			wantAppClose(t, "the peer", server.Wait(), 0x2a, "bye", true)
			wantAppClose(t, "the closer", client.Wait(), 0x2a, "bye", false)
		})
	})
}

// TestDone (N17): Done is closed exactly when Wait returns, and a select
// on it with a timer against a peer that never reads gives up without
// changing the session, which still ends with the close.
func TestDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := net.Pipe()
		sr := &holdReads{Conn: b, closed: make(chan struct{})}
		client, server := bubblePairOn(t, a, sr, closeTestConfig)
		if _, err := client.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		done := client.Done()
		if client.Done() != done {
			t.Fatal("Done returned another channel")
		}
		select {
		case <-done:
			t.Fatal("Done closed on a running session")
		default:
		}
		sr.stall.Store(true) // the peer reads nothing more
		_ = client.CloseWithError(0x2a, "bye")
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("Done closed though the peer has not read the close")
		default:
		}
		if err := context.Cause(server.Context()); err != nil {
			t.Fatalf("the peer's session changed: %v", err)
		}
		<-done // the linger timeout
		waited := make(chan error, 1)
		go func() { waited <- client.Wait() }()
		synctest.Wait()
		select {
		case err := <-waited:
			wantAppClose(t, "the closer", err, 0x2a, "bye", false)
		default:
			t.Fatal("Wait blocks after Done was closed")
		}
		_ = server.Close()
		_ = server.Wait()
	})
}

// holdReads, while stalled, takes every byte from the carrier and gives
// none to the reader until it is closed: a peer that reads nothing more,
// though its carrier takes the bytes.
type holdReads struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
	stall  atomic.Bool
}

func (h *holdReads) Read(b []byte) (int, error) {
	n, err := h.Conn.Read(b)
	if h.stall.Load() {
		_, _ = io.Copy(io.Discard, h.Conn)
		<-h.closed
		return 0, net.ErrClosed
	}
	return n, err
}

func (h *holdReads) Close() error {
	h.once.Do(func() { close(h.closed) })
	return h.Conn.Close()
}

// closeReceived signals when the server acts on the peer's close and
// preserves half-close for both TCP and TLS.
type closeReceived struct {
	net.Conn
	received chan struct{}
	once     sync.Once
}

func (c *closeReceived) CloseWrite() error {
	return c.Conn.(interface{ CloseWrite() error }).CloseWrite()
}

func (c *closeReceived) TLSRecords() bool { return tlsRecords(c.Conn) }

func (c *closeReceived) RecordCloseCode(ApplicationErrorCode, string) {
	c.once.Do(func() { close(c.received) })
}

// closeDelivery runs N16 on a session pair. The peer reads 1 MiB with FIN
// and ends with the close code. The open stream fails with the session's
// error. If readAfterClose is set, the peer takes the data from the read
// drain after receiving CONNECTION_CLOSE.
func closeDelivery(t *testing.T, client, server *Conn, code ApplicationErrorCode, reason string, readAfterClose <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	msg := make([]byte, 1<<20)
	for i := range msg {
		msg[i] = byte(i * 7)
	}
	read := make(chan error, 1)
	accepted := make(chan struct{})
	go func() {
		in, err := server.AcceptStream(ctx)
		close(accepted)
		if err == nil {
			if readAfterClose != nil {
				select {
				case <-readAfterClose:
				case <-ctx.Done():
					read <- ctx.Err()
					return
				}
			}
			var got []byte
			got, err = io.ReadAll(in)
			if err != nil {
				err = fmt.Errorf("after %d bytes: %w", len(got), err)
			}
			if err == nil && !bytes.Equal(got, msg) {
				err = fmt.Errorf("read %d bytes, not the 1 MiB written", len(got))
			}
		}
		read <- err
	}()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	open, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open.Write([]byte("left open")); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, accepted, "server stream acceptance")
	_ = client.CloseWithError(code, reason)
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("the peer's read: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for the peer's read: %v", ctx.Err())
	}
	waitTestSignal(t, client.Done(), "client session end")
	waitTestSignal(t, server.Done(), "server session end")
	wantAppClose(t, "the peer", server.Wait(), code, reason, true)
	wantAppClose(t, "the closer", client.Wait(), code, reason, false)
	if _, err := open.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the open stream after the close: %v", err)
	} else {
		wantAppClose(t, "the open stream", err, code, reason, false)
	}
}

// TestCloseDeliveryByteStream (N16) over a half-closeable byte stream,
// with and without TLS and a close code.
func TestCloseDeliveryByteStream(t *testing.T) {
	// The slow reader must be able to buffer the entire stream before
	// reading it. The connection also needs credit for the second stream.
	window := &Config{
		InitialStreamReceiveWindow:     1 << 20,
		MaxStreamReceiveWindow:         1 << 20,
		InitialConnectionReceiveWindow: 2 << 20,
		MaxConnectionReceiveWindow:     2 << 20,
	}
	for _, name := range []string{"plain", "tls"} {
		for _, c := range []struct {
			reason string
			code   ApplicationErrorCode
		}{{code: 0x2a, reason: "bye"}, {code: 0, reason: ""}} {
			for _, slow := range []bool{false, true} {
				testName := fmt.Sprintf("%s/%#x", name, uint64(c.code))
				if slow {
					testName += "/slow-reader"
				}
				t.Run(testName, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						a, b := memnet.Pipe()
						if name == "tls" {
							srvTLS, cliTLS := selfSignedTLS(t)
							a, b = tls.Client(a, cliTLS), tls.Server(b, srvTLS)
						}
						var received <-chan struct{}
						if slow {
							peer := &closeReceived{Conn: b, received: make(chan struct{})}
							b, received = peer, peer.received
						}
						// The virtual clock also refills the carrier's pacing
						// budget while Close waits for the finished stream.
						domain := NewClockDomain(timeshim.Real)
						scfg := window.Clone()
						scfg.ClockDomain = domain
						client, server := pair(t, a, b, &Config{ClockDomain: domain}, scfg)
						closeDelivery(t, client, server, c.code, c.reason, received)
					})
				})
			}
		}
	}
}

// codeConn records the close codes zmux passes to it.
type codeConn struct {
	net.Conn
	reason string
	code   ApplicationErrorCode
	mu     sync.Mutex
}

func (c *codeConn) RecordCloseCode(code ApplicationErrorCode, reason string) {
	c.mu.Lock()
	c.code, c.reason = code, reason
	c.mu.Unlock()
}

// A session that ends without an application close passes 0 and no
// reason to CloseCodeRecorder, replacing what an earlier session left.
func TestCloseCodeRecorderNonApplicationEnd(t *testing.T) {
	a, b := tcpPair(t)
	cc := &codeConn{Conn: a, code: 7, reason: "earlier session"}
	client, _ := pair(t, cc, b, nil, nil)
	_ = b.Close() // the carrier fails: no application close
	_ = client.Wait()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.code != 0 || cc.reason != "" {
		t.Fatalf("recorded %#x %q after a carrier failure", uint64(cc.code), cc.reason)
	}
}
