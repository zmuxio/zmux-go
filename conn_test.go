package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// pair returns a client and a server session over a carrier pair.
func pair(t testing.TB, a, b net.Conn, ccfg, scfg *Config) (*Conn, *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type res struct {
		c   *Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := Server(ctx, b, scfg)
		ch <- res{s, err}
	}()
	c, err := Dial(ctx, a, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = r.c.Close()
		waitDone(t, c)
		waitDone(t, r.c)
	})
	return c, r.c
}

func waitDone(t testing.TB, c *Conn) {
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("session did not end")
	}
}

func tcpPair(t testing.TB) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return a, <-ch
}

func carriers() map[string]func(testing.TB) (net.Conn, net.Conn) {
	return map[string]func(testing.TB) (net.Conn, net.Conn){
		"pipe": func(testing.TB) (net.Conn, net.Conn) { return net.Pipe() },
		"tcp":  tcpPair,
	}
}

func TestEcho(t *testing.T) {
	for name, mk := range carriers() {
		t.Run(name, func(t *testing.T) {
			leakcheck.Check(t)
			a, b := mk(t)
			c, s := pair(t, a, b, nil, nil)
			go func() {
				st, err := s.AcceptStream(context.Background())
				if err != nil {
					return
				}
				_, _ = io.Copy(st, st)
				_ = st.Close()
			}()
			st, err := c.OpenStreamSync(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			msg := make([]byte, 300000)
			rand.Read(msg)
			go func() {
				_, _ = st.Write(msg)
				_ = st.Close()
			}()
			got, err := io.ReadAll(st)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, msg) {
				t.Fatalf("echo mismatch: %d bytes", len(got))
			}
		})
	}
}

func TestManyStreams(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, nil)
	go func() {
		for {
			st, err := s.AcceptStream(context.Background())
			if err != nil {
				return
			}
			go func() {
				_, _ = io.Copy(st, st)
				_ = st.Close()
			}()
		}
	}()
	errs := make(chan error, 50)
	for i := range 50 {
		go func() {
			st, err := c.OpenStreamSync(context.Background())
			if err != nil {
				errs <- err
				return
			}
			msg := bytes.Repeat([]byte{byte(i)}, 20000)
			go func() { _, _ = st.Write(msg); _ = st.Close() }()
			got, err := io.ReadAll(st)
			if err == nil && !bytes.Equal(got, msg) {
				err = errors.New("mismatch")
			}
			errs <- err
		}()
	}
	for range 50 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseWakesReaders(t *testing.T) {
	leakcheck.Check(t)
	a, b := net.Pipe()
	c, s := pair(t, a, b, nil, nil)
	st, _ := c.OpenStreamSync(context.Background())
	_, _ = st.Write([]byte("x"))
	ss, err := s.AcceptStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, 10)
		_, _ = ss.Read(buf)
		_, err := ss.Read(buf)
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = c.CloseWithError(7, "bye")
	select {
	case err := <-errc:
		var ae *ApplicationError
		if !errors.As(err, &ae) || ae.ErrorCode != 7 || !ae.Remote {
			t.Fatalf("read error %v", err)
		}
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("not net.ErrClosed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader not woken")
	}
	runtime.KeepAlive(st) // a collected handle would reset the stream first
}

func TestDeadlines(t *testing.T) {
	leakcheck.Check(t)
	a, b := net.Pipe()
	c, s := pair(t, a, b, &Config{SendBuffer: 64 << 10}, &Config{InitialStreamReceiveWindow: 4096, MaxStreamReceiveWindow: 4096})
	st, _ := c.OpenStreamSync(context.Background())
	_, _ = st.Write([]byte("x"))
	ss, err := s.AcceptStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ss.Read(make([]byte, 1))
	// A deadline set while a read is blocked without one interrupts it.
	errc := make(chan error, 1)
	go func() {
		_, err := ss.Read(make([]byte, 1))
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = ss.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	select {
	case err := <-errc:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked read not interrupted by a new deadline")
	}
	// Cleared, the stream reads again.
	_ = ss.SetReadDeadline(time.Time{})
	_, _ = st.Write([]byte("y"))
	if _, err := io.ReadFull(ss, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	// A write blocked on flow control, then a deadline.
	go func() {
		_, err := st.Write(make([]byte, 1<<20))
		errc <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = st.SetWriteDeadline(time.Now())
	select {
	case err := <-errc:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked write not interrupted by a new deadline")
	}
	runtime.KeepAlive(st)
	runtime.KeepAlive(ss)
}
