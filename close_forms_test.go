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
		s, err := Server(ctx, b, &Config{Clock: timeshim.Real, InitialStreamReceiveWindow: 64 << 10, MaxStreamReceiveWindow: 64 << 10})
		if err != nil {
			s = nil
		}
		ch <- s
	}()
	client, err := Client(ctx, a, &Config{Clock: timeshim.Real})
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
			go func() {
				time.Sleep(100 * time.Millisecond)
				client.Abort(5, "abort")
			}()
			t0 := time.Now()
			_ = client.CloseWithError(0x2a, "bye")
			if d := time.Since(t0); d != 100*time.Millisecond {
				t.Fatalf("the close returned after %v, not at the abort", d)
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
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Server(ctx, sr, &Config{Clock: timeshim.Real})
			if err != nil {
				s = nil
			}
			ch <- s
		}()
		client, err := Client(ctx, a, &Config{Clock: timeshim.Real})
		if err != nil {
			t.Fatal(err)
		}
		server := <-ch
		if server == nil {
			t.Fatal("no server session")
		}
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
		select {
		case <-done:
			t.Fatal("Done closed though the peer has not read the close")
		case <-time.After(50 * time.Millisecond):
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

// closeDelivery runs N16 on a session pair: the client writes 1 MiB on a
// stream (with FIN) and a few bytes on a second it leaves open, closes
// with code and reason and waits for the whole end within 5 s. The peer
// reads the whole 1 MiB in order and ends with the code; the close
// returned within 1 s; the wait ended before the shortest linger timeout,
// so the peer read the close; the open stream failed with the session's
// error. With readDelay the peer starts reading only after the close
// arrived, and still gets everything within the linger timeout.
func closeDelivery(t *testing.T, client, server *Conn, code ApplicationErrorCode, reason string, readDelay time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg := make([]byte, 1<<20)
	for i := range msg {
		msg[i] = byte(i * 7)
	}
	read := make(chan error, 1)
	go func() {
		in, err := server.AcceptStream(ctx)
		if err == nil {
			time.Sleep(readDelay) // a slow reader: the close arrives first
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
	t0 := time.Now()
	_ = client.CloseWithError(code, reason)
	if d := time.Since(t0); d > closeLimit {
		t.Fatalf("the close took %v", d)
	}
	t1 := time.Now()
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the close was neither delivered nor given up within 5 s")
	}
	if d := time.Since(t1); d >= 250*time.Millisecond {
		t.Fatalf("the wait took %v: the linger timeout, not the peer's end", d)
	}
	if err := <-read; err != nil {
		t.Fatalf("the peer's read: %v", err)
	}
	wantAppClose(t, "the peer", server.Wait(), code, reason, true)
	wantAppClose(t, "the closer", client.Wait(), code, reason, false)
	if _, err := open.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the open stream after the close: %v", err)
	} else {
		wantAppClose(t, "the open stream", err, code, reason, false)
	}
}

// TestCloseDeliveryByteStream (N16) over TCP and TLS, with a coded close
// and with the plain one.
func TestCloseDeliveryByteStream(t *testing.T) {
	srvTLS, cliTLS := selfSignedTLS(t)
	for name, wrap := range map[string]func(a, b net.Conn) (net.Conn, net.Conn){
		"tcp": func(a, b net.Conn) (net.Conn, net.Conn) { return a, b },
		"tls": func(a, b net.Conn) (net.Conn, net.Conn) { return tls.Client(a, cliTLS), tls.Server(b, srvTLS) },
	} {
		for _, c := range []struct {
			code   ApplicationErrorCode
			reason string
		}{{0x2a, "bye"}, {0, ""}} {
			t.Run(fmt.Sprintf("%s/%#x", name, uint64(c.code)), func(t *testing.T) {
				a, b := wrap(tcpPair(t))
				client, server := pair(t, a, b, nil, nil)
				closeDelivery(t, client, server, c.code, c.reason, 0)
			})
			t.Run(fmt.Sprintf("%s/%#x/slow-reader", name, uint64(c.code)), func(t *testing.T) {
				a, b := wrap(tcpPair(t))
				client, server := pair(t, a, b, nil, nil)
				closeDelivery(t, client, server, c.code, c.reason, 100*time.Millisecond)
			})
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
