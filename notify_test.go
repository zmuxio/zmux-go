package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/netx"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// idlePairT is streamPairT with the server side's first byte read: nothing
// is readable on ss.
func idlePairT(t *testing.T) (cs, ss *Stream) {
	t.Helper()
	cs, ss = streamPairT(t)
	if n, err := ss.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	return cs, ss
}

// note registers on s and returns a channel closed by f, with a count of
// the calls.
func note(s *Stream) (<-chan struct{}, *atomic.Int32, func() bool) {
	ch := make(chan struct{})
	var calls atomic.Int32
	stop := s.NotifyReadable(func() {
		if calls.Add(1) == 1 {
			close(ch)
		}
	})
	return ch, &calls, stop
}

func mustFire(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no notification", what)
	}
}

func mustNotFire(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: notified", what)
	case <-time.After(50 * time.Millisecond):
	}
}

// Every event that makes TryRead return anything but ErrWouldBlock calls f
// once, and TryRead then returns what Read would.
func TestNotifyReadableTriggers(t *testing.T) {
	leakcheck.Check(t)
	buf := make([]byte, 8)
	cases := []struct {
		event func(t *testing.T, cs, ss *Stream)
		check func(n int, err error) bool
		name  string
	}{
		{name: "data", event: func(t *testing.T, cs, _ *Stream) {
			t.Helper()
			if _, err := cs.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}, check: func(n int, err error) bool { return n == 1 && err == nil }},
		{name: "fin", event: func(_ *testing.T, cs, _ *Stream) { _ = cs.Close() },
			check: func(n int, err error) bool { return n == 0 && errors.Is(err, io.EOF) }},
		{name: "reset", event: func(_ *testing.T, cs, _ *Stream) { cs.CancelWrite(7) },
			check: func(_ int, err error) bool {
				var se *StreamError
				return errors.As(err, &se) && se.ErrorCode == 7
			}},
		{name: "cancel read", event: func(_ *testing.T, _, ss *Stream) { ss.CancelRead(3) },
			check: func(n int, err error) bool { return n == 0 && err != nil && !errors.Is(err, ErrWouldBlock) }},
		{name: "conn close", event: func(_ *testing.T, _, ss *Stream) {
			_ = StreamConn(ss, nil, nil).Close()
		}, check: func(n int, err error) bool { return n == 0 && err != nil && !errors.Is(err, ErrWouldBlock) }},
		{name: "session close", event: func(_ *testing.T, _, ss *Stream) { _ = ss.h.c.Close() },
			check: func(n int, err error) bool { return n == 0 && err != nil && !errors.Is(err, ErrWouldBlock) }},
		{name: "peer session close", event: func(_ *testing.T, cs, _ *Stream) { _ = cs.h.c.Close() },
			check: func(n int, err error) bool { return n == 0 && err != nil && !errors.Is(err, ErrWouldBlock) }},
		{name: "deadline", event: func(_ *testing.T, _, ss *Stream) {
			_ = ss.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
		}, check: func(n int, err error) bool { return n == 0 && errors.Is(err, os.ErrDeadlineExceeded) }},
		{name: "deadline passed", event: func(_ *testing.T, _, ss *Stream) {
			_ = ss.SetReadDeadline(time.Now().Add(-time.Second))
		}, check: func(n int, err error) bool { return n == 0 && errors.Is(err, os.ErrDeadlineExceeded) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs, ss := idlePairT(t)
			if n, err := ss.TryRead(buf); n != 0 || !errors.Is(err, ErrWouldBlock) {
				t.Fatalf("idle: %d %v", n, err)
			}
			ch, calls, stop := note(ss)
			mustNotFire(t, ch, "idle")
			tc.event(t, cs, ss)
			mustFire(t, ch, tc.name)
			if n, err := ss.TryRead(buf); !tc.check(n, err) {
				t.Fatalf("TryRead after %s: %d %v", tc.name, n, err)
			}
			if stop() {
				t.Fatal("stop after the call returned true")
			}
			time.Sleep(10 * time.Millisecond)
			if c := calls.Load(); c != 1 {
				t.Fatalf("%d calls", c)
			}
		})
	}
}

// A deadline moved while a call is pending calls f when the new one
// passes, not the old one; a deadline cleared calls nothing.
func TestNotifyReadableDeadlineMoved(t *testing.T) {
	leakcheck.Check(t)
	_, ss := idlePairT(t)
	_ = ss.SetReadDeadline(time.Now().Add(time.Hour))
	ch, _, _ := note(ss)
	_ = ss.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	mustFire(t, ch, "moved earlier")

	_ = ss.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	ch, _, stop := note(ss)
	_ = ss.SetReadDeadline(time.Time{})
	mustNotFire(t, ch, "cleared")
	_ = ss.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	mustFire(t, ch, "set again")
	if stop() {
		t.Fatal("stop after the call returned true")
	}
}

// A readable stream calls f before NotifyReadable returns.
func TestNotifyReadableAtRegistration(t *testing.T) {
	leakcheck.Check(t)
	_, ss := streamPairT(t) // its first byte is readable
	called := false
	stop := ss.NotifyReadable(func() { called = true })
	if !called {
		t.Fatal("not called synchronously")
	}
	if stop() {
		t.Fatal("stop after the call returned true")
	}
}

// stop before the event keeps f from running; a later registration works.
func TestNotifyReadableStop(t *testing.T) {
	leakcheck.Check(t)
	cs, ss := idlePairT(t)
	ch, _, stop := note(ss)
	if !stop() {
		t.Fatal("stop of a pending call returned false")
	}
	if stop() {
		t.Fatal("second stop returned true")
	}
	if _, err := cs.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	mustNotFire(t, ch, "stopped")
	ch2, _, _ := note(ss) // readable now: called at once
	mustFire(t, ch2, "after stop")
}

// f may register again from inside f; a second registration while one is
// pending panics.
func TestNotifyReadableReRegister(t *testing.T) {
	leakcheck.Check(t)
	cs, ss := idlePairT(t)
	var got []byte
	done := make(chan struct{})
	buf := make([]byte, 4)
	var f func()
	f = func() {
		for {
			n, err := ss.TryRead(buf)
			got = append(got, buf[:n]...)
			switch {
			case errors.Is(err, ErrWouldBlock):
				ss.NotifyReadable(f)
				return
			case err != nil:
				close(done)
				return
			}
		}
	}
	ss.NotifyReadable(f)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("second pending registration did not panic")
			}
		}()
		ss.NotifyReadable(func() {})
	}()
	for _, s := range []string{"ab", "cdefg", "h"} {
		if _, err := cs.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = cs.Close()
	mustFire(t, done, "end")
	if string(got) != "abcdefgh" {
		t.Fatalf("read %q", got)
	}
}

// Close, which finishes the sending part, and stream datagrams leave the
// receiving part unreadable.
func TestNotifyReadableNotTriggered(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, ss := idlePairT(t)
	ch, _, stop := note(ss)
	_ = ss.Close()
	mustNotFire(t, ch, "local Close")
	if !stop() {
		t.Fatal("stop returned false")
	}

	a, b := tcpPair(t)
	cfg := &Config{EnableDatagrams: true}
	client, server := pair(t, a, b, cfg, cfg)
	cs, ss2 := openPackets(t, ctx, client, server)
	ch, _, stop = note(ss2)
	if _, err := ss2.TryRead(make([]byte, 64)); !errors.Is(err, ErrWouldBlock) {
		// The preamble is not data; a registration made while it was
		// pending may already have been called by its arrival.
		t.Logf("TryRead: %v", err)
	}
	if err := cs.SendDatagram(0, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ss2.ReceiveDatagram(ctx); err != nil {
		t.Fatal(err)
	}
	mustNotFire(t, ch, "datagram")
	if !stop() {
		t.Fatal("stop returned false")
	}
}

// ErrWouldBlock is recognised by its WouldBlock method, without this
// package.
func TestErrWouldBlockMethod(t *testing.T) {
	var wb interface{ WouldBlock() bool }
	if !errors.As(ErrWouldBlock, &wb) || !wb.WouldBlock() {
		t.Fatal("ErrWouldBlock has no WouldBlock method")
	}
	if !netx.IsWouldBlock(ErrWouldBlock) || !netx.IsWouldBlock(fmt.Errorf("wrapped: %w", ErrWouldBlock)) {
		t.Fatal("netx.IsWouldBlock does not recognise ErrWouldBlock")
	}
}

// stop and the call race: exactly one wins, f runs at most once and only
// when stop lost.
func TestNotifyReadableStopRace(t *testing.T) {
	leakcheck.Check(t)
	cs, ss := idlePairT(t)
	buf := make([]byte, 64)
	deadline := time.Now().Add(2 * time.Second)
	for i := 0; i < 300 && time.Now().Before(deadline); i++ {
		var calls atomic.Int32
		stop := ss.NotifyReadable(func() { calls.Add(1) })
		var wg sync.WaitGroup
		wg.Go(func() { _, _ = cs.Write([]byte("x")) })
		if i%2 == 0 {
			time.Sleep(time.Duration(i%7) * 10 * time.Microsecond)
		}
		stopped := stop()
		wg.Wait()
		// Let the arrival be delivered, then drain it.
		for {
			if n, _ := ss.TryRead(buf); n > 0 {
				break
			}
			if err := ss.WaitReadable(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Millisecond)
		c := calls.Load()
		if stopped && c != 0 || !stopped && c != 1 {
			t.Fatalf("round %d: stop %v, %d calls", i, stopped, c)
		}
		for {
			if _, err := ss.TryRead(buf); errors.Is(err, ErrWouldBlock) {
				break
			}
		}
	}
}

// relayRig is a netx.Relay between a TCP connection and the server end of a
// stream, with the far ends of both in the test's hands: x is the TCP peer
// and cs the stream's client end.
type relayRig struct {
	x    net.Conn
	cs   *Stream
	ss   *Stream
	done chan relayResult
}

type relayResult struct {
	err        error
	aToB, bToA int64
}

func startRelay(t *testing.T, opts ...netx.RelayOption) *relayRig {
	t.Helper()
	cs, ss := idlePairT(t)
	x, y := tcpPair(t)
	t.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	r := &relayRig{x: x, cs: cs, ss: ss, done: make(chan relayResult, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	go func() {
		var res relayResult
		res.aToB, res.bToA, res.err = netx.Relay(ctx, y, StreamConn(ss, nil, nil), opts...)
		r.done <- res
	}()
	return r
}

func (r *relayRig) wait(t *testing.T, what string) relayResult {
	t.Helper()
	select {
	case res := <-r.done:
		return res
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: relay did not end", what)
		return relayResult{}
	}
}

func pattern(n, salt int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + salt + i>>8)
	}
	return b
}

// netx.Relay copies both directions between a TCP connection and a stream
// to the end, each 256 KiB and more, passing EOF on in each direction.
func TestRelayStreamBothDirections(t *testing.T) {
	leakcheck.Check(t)
	r := startRelay(t, netx.WithRelayHalfClose())
	toStream, toTCP := pattern(300<<10+17, 1), pattern(270<<10+5, 2)
	var gotStream, gotTCP []byte
	var wg sync.WaitGroup
	var errTCP, errStream error
	wg.Go(func() { // the TCP peer
		if _, err := r.x.Write(toStream); err != nil {
			errTCP = err
			return
		}
		_ = r.x.(*net.TCPConn).CloseWrite()
	})
	wg.Go(func() {
		gotTCP, errTCP = io.ReadAll(r.x)
	})
	wg.Go(func() { // the stream's client
		if _, err := r.cs.Write(toTCP); err != nil {
			errStream = err
			return
		}
		_ = r.cs.Close()
	})
	wg.Go(func() {
		gotStream, errStream = io.ReadAll(r.cs)
	})
	wg.Wait()
	if errTCP != nil || errStream != nil {
		t.Fatal(errTCP, errStream)
	}
	// The stream's first byte was read before the relay started.
	if !bytes.Equal(gotStream, toStream) || !bytes.Equal(gotTCP, toTCP) {
		t.Fatalf("relayed %d/%d bytes, want %d/%d", len(gotStream), len(gotTCP), len(toStream), len(toTCP))
	}
	res := r.wait(t, "both directions")
	if res.err != nil || res.aToB != int64(len(toStream)) || res.bToA != int64(len(toTCP)) {
		t.Fatalf("relay: %+v", res)
	}
}

func relayStreamReaders() string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var found []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "(*Stream).Read(") || strings.Contains(g, "(*Stream).WaitReadable(") ||
			strings.Contains(g, "(*Stream).WriteTo(") || strings.Contains(g, "netx.(*notifyCopy).run(") {
			found = append(found, g)
		}
	}
	return strings.Join(found, "\n\n")
}

// An idle relay holds no goroutine on the stream side: only the caller of
// Relay waits, on the TCP side, and the stream is read by short tasks that
// the notification starts, which end again when TryRead would block.
func TestRelayStreamIdleNoGoroutine(t *testing.T) {
	leakcheck.Check(t)
	cs, ss := idlePairT(t)
	x, y := tcpPair(t)
	t.Cleanup(func() { _ = x.Close(); _ = y.Close() })
	done := make(chan error, 1)
	go func() {
		_, _, err := netx.Relay(context.Background(), y, StreamConn(ss, nil, nil))
		done <- err
	}()
	// The stream side is idle once it has parked on a fresh notification
	// and its last copy task has exited. The relay caller waits on TCP.
	waitIdle := func(previous *readNote) *readNote {
		t.Helper()
		var armed *readNote
		waitUntil(t, 5*time.Second, "idle relay with a pending stream notification and no reader", func() bool {
			armed = ss.h.rnote.Load()
			if armed == nil || armed == previous || armed.done.Load() || ss.h.readableNow() {
				return false
			}
			return relayStreamReaders() == ""
		})
		return armed
	}
	initial := waitIdle(nil)
	// After traffic both ways the relay is idle again.
	for range 3 {
		msg := pattern(100<<10, 3)
		if _, err := cs.Write(msg); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(x, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatal("stream to TCP", err)
		}
		if _, err := x.Write(msg); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(cs, got); err != nil || !bytes.Equal(got, msg) {
			t.Fatal("TCP to stream", err)
		}
	}
	waitIdle(initial)
	_ = cs.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("relay did not end")
	}
}

// Whatever ends the stream ends the relay and closes the TCP side.
func TestRelayStreamEnds(t *testing.T) {
	leakcheck.Check(t)
	cases := []struct {
		event func(r *relayRig)
		check func(err error) bool
		name  string
	}{
		{name: "peer session close", event: func(r *relayRig) { _ = r.cs.h.c.Close() },
			check: func(err error) bool { return err != nil }},
		{name: "local session close", event: func(r *relayRig) { _ = r.ss.h.c.Close() },
			check: func(err error) bool { return err != nil }},
		{name: "local stream close", event: func(r *relayRig) { _ = StreamConn(r.ss, nil, nil).Close() },
			check: func(err error) bool { return err != nil }},
		{name: "peer reset", event: func(r *relayRig) { r.cs.CancelWrite(7) },
			check: func(err error) bool {
				var se *StreamError
				return errors.As(err, &se) && se.ErrorCode == 7
			}},
		{name: "peer close", event: func(r *relayRig) { _ = r.cs.Close() },
			check: func(err error) bool { return err == nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := startRelay(t)
			time.Sleep(20 * time.Millisecond) // the relay is idle
			tc.event(r)
			res := r.wait(t, tc.name)
			if !tc.check(res.err) {
				t.Fatalf("relay error: %v", res.err)
			}
			_ = r.x.SetReadDeadline(time.Now().Add(5 * time.Second))
			if n, err := r.x.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("TCP side still open: %d %v", n, err)
			} else if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("TCP side not closed")
			}
		})
	}
}
