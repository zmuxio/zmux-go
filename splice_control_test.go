package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

func controlledRelay(t *testing.T, cfg *Config, opts *SpliceOptions, packets bool) (cs, ss *Stream, c *SpliceController) {
	t.Helper()
	if cfg == nil {
		cfg = &Config{ClockDomain: NewClockDomain(timeshim.NewFake(time.Unix(1, 0)))}
	}
	a, b := net.Pipe()
	client, in := spliceTestPair(t, a, b, cfg)
	a, b = net.Pipe()
	out, server := spliceTestPair(t, a, b, cfg)
	cs, receive := openSpliceTestPair(t, client, in)
	send, ss := openSpliceTestPair(t, out, server)
	start := StartSplice
	if packets {
		start = StartPacketSplice
	}
	c, err := start(t.Context(), receive, send, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Abort(0)
		<-c.Done()
	})
	return cs, ss, c
}

func spliceTestPair(t testing.TB, a, b net.Conn, cfg *Config) (*Conn, *Conn) {
	t.Helper()
	client, server := startTestPair(t, a, b, cfg, cfg)
	// End carrier I/O without waiting for a protocol timer.
	t.Cleanup(func() {
		client.Abort(0, "")
		server.Abort(0, "")
		_ = a.Close()
		_ = b.Close()
		<-client.Done()
		<-server.Done()
	})
	return client, server
}

func openSpliceTestPair(t testing.TB, client, server *Conn) (*Stream, *Stream) {
	t.Helper()
	ctx := t.Context()
	s, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	r, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Release unread FINs and resets before the pair cleanup waits for
	// Done. A peer close can start a read drain on the frozen clock.
	t.Cleanup(func() {
		s.CancelRead(0)
		r.CancelRead(0)
	})
	return s, r
}

func injectSpliceTest(t *testing.T, control *SpliceControl, in SpliceInjection) SpliceReceipt {
	t.Helper()
	r, err := control.Inject(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func readSpliceTest(t *testing.T, s *Stream, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(s, got); err != nil || string(got) != want {
		t.Fatalf("read %q, want %q: %v", got, want, err)
	}
}

func finishSpliceTest(t *testing.T, cs, ss *Stream, c *SpliceController) SpliceResult {
	t.Helper()
	_ = cs.Close()
	_ = ss.Close()
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(cs); err != nil {
		t.Fatal(err)
	}
	r, err := c.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSpliceControlGroupGenerationPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		hookInputs := make(chan SpliceMeta, 1)
		var stale SplicePayload
		first := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			part, err := u.Payload(0).Slice(0, 2)
			if err != nil {
				return SpliceDecision{}, err
			}
			replacement, err := u.Alloc(2)
			if err != nil {
				return SpliceDecision{}, err
			}
			if _, err := replacement.WriteAt([]byte("XY"), 0); err != nil {
				return SpliceDecision{}, err
			}
			stale = replacement
			return SpliceDecision{Replacement: []SplicePayload{part, replacement}}, nil
		}
		second := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			hookInputs <- u.Meta()
			var got [4]byte
			for i := range u.Len() {
				if _, err := u.Payload(i).CopyTo(got[2*i:2*i+2], 0); err != nil {
					return SpliceDecision{}, err
				}
			}
			if string(got[:]) != "abXY" {
				return SpliceDecision{}, fmt.Errorf("group %q", got)
			}
			return SpliceDecision{Wait: SpliceWait{Resume: gate}}, nil
		}
		mirror, err := NewSpliceMirror(SpliceMirrorOptions{Items: 1, MaxPayload: 2, BackingBytes: 2, Every: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer mirror.Close()
		opts := &SpliceOptions{TagA: "source", TagB: "target", MirrorAtoB: mirror, HooksAtoB: SpliceHooks{Stream: []SpliceHook{first, second}}}
		cs, ss, c := controlledRelay(t, nil, opts, false)
		r := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceStream, Data: []byte("abc")})
		if meta := <-hookInputs; meta.Sequence != r.Sequence || meta.Generation != 1 ||
			meta.Origin != SpliceInjected || meta.SourceTag != "source" || meta.TargetTag != "target" {
			t.Fatalf("metadata %+v", meta)
		}
		if _, err := stale.WriteAt([]byte("!"), 0); !errors.Is(err, ErrSplicePayload) {
			t.Fatalf("stale builder: %v", err)
		}
		pause, err := c.AtoB.Pause(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := pause.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AtoB.Inject(t.Context(), SpliceInjection{Kind: SpliceStream}); !errors.Is(err, ErrSplicePaused) {
			t.Fatalf("inject during pause: %v", err)
		}
		if generation, err := c.AtoB.ReplaceHooks(t.Context(), SpliceHooks{}); err != nil || generation != 2 {
			t.Fatalf("detach generation %d: %v", generation, err)
		}
		close(gate)
		synctest.Wait()
		select {
		case done := <-r.Done:
			t.Fatalf("paused output was delivered: %+v", done)
		default:
		}
		if _, err := ss.Write([]byte("reverse")); err != nil {
			t.Fatal(err)
		}
		readSpliceTest(t, cs, "reverse")
		if err := c.AtoB.Resume(t.Context(), pause); err != nil {
			t.Fatal(err)
		}
		r2 := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceStream, Data: []byte("z")})
		readSpliceTest(t, ss, "abXYz")
		if a, b := <-r.Done, <-r2.Done; a.Outputs != 2 || a.Bytes != 4 || a.Generation != 1 ||
			b.Outputs != 1 || b.Generation != 2 || b.Sequence <= a.Sequence {
			t.Fatalf("receipts %+v %+v", a, b)
		}
		result := finishSpliceTest(t, cs, ss, c)
		if first, _ := mirror.Next(t.Context()); string(first.Data) != "ab" || first.Phase != SpliceMirrorInput ||
			result.AtoB.HookReplaced != 1 || result.AtoB.MirrorDropped == 0 {
			t.Fatalf("mirror %+v, result %+v", first, result)
		}
		if _, err := c.AtoB.Inject(t.Context(), SpliceInjection{Kind: SpliceStream}); !errors.Is(err, ErrSpliceClosed) {
			t.Fatalf("inject after FIN: %v", err)
		}
	})
}

type spliceTimerGate struct {
	*timeshim.Fake
	arming chan struct{}
	resume chan struct{}
	armed  atomic.Bool
}

func (c *spliceTimerGate) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	if d == time.Second && c.armed.CompareAndSwap(true, false) {
		close(c.arming)
		<-c.resume
	}
	return c.Fake.AfterFunc(d, f)
}

func TestSpliceControlDatagramReordering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &spliceTimerGate{Fake: timeshim.NewFake(time.Unix(1, 0)),
			arming: make(chan struct{}), resume: make(chan struct{})}
		unhold := sync.OnceFunc(func() { close(clock.resume) })
		defer unhold()
		hook := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			switch u.Meta().ContextID {
			case 1:
				return SpliceDecision{Wait: SpliceWait{After: time.Second}}, nil
			case 3:
				return SpliceDecision{Wait: SpliceWait{Resume: make(chan struct{}), Expiry: time.Second}}, nil
			default:
				return SpliceDecision{}, nil
			}
		}
		opts := &SpliceOptions{HooksAtoB: SpliceHooks{Datagram: []SpliceHook{hook}}}
		cs, ss, c := controlledRelay(t, &Config{ClockDomain: NewClockDomain(clock), EnableDatagrams: true}, opts, false)
		r1 := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceDatagram, ContextID: 1, Data: []byte("later")})
		r2 := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceDatagram, ContextID: 2})
		r3 := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceDatagram, ContextID: 3})
		cx, data, err := ss.ReceiveDatagram(t.Context())
		if err != nil || cx != 2 || len(data) != 0 {
			t.Fatalf("first datagram %d %q: %v", cx, data, err)
		}
		if r := <-r2.Done; r.Status != SpliceSubmitted || r.Outputs != 1 || r.Bytes != 0 {
			t.Fatalf("empty datagram receipt %+v", r)
		}
		// Wake the relay from its existing timer with a command. Advance
		// between its calculation of the delay and arming the next timer.
		synctest.Wait()
		clock.armed.Store(true)
		if _, err := c.AtoB.Stats(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-clock.arming
		clock.Advance(time.Second)
		unhold()
		synctest.Wait()
		select {
		case expired := <-r3.Done:
			if expired.Status != SpliceDropped {
				t.Fatalf("expired datagram receipt %+v", expired)
			}
		default:
			t.Fatal("relay went back to sleep after the datagram deadline passed")
		}
		cx, data, err = ss.ReceiveDatagram(t.Context())
		if err != nil || cx != 1 || string(data) != "later" {
			t.Fatalf("second datagram %d %q: %v", cx, data, err)
		}
		if first := <-r1.Done; first.Status != SpliceSubmitted {
			t.Fatalf("delayed datagram receipt %+v", first)
		}
		if r := finishSpliceTest(t, cs, ss, c); r.AtoB.Drops[SpliceDropExpired] != 1 {
			t.Fatalf("result %+v", r)
		}
	})
}

func TestSpliceTransformedAndInjectedDatagramsUseRecordShaping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const pad = 32
		cfg := &Config{EnableDatagrams: true, ClockDomain: NewClockDomain(timeshim.NewFake(time.Unix(1, 0))),
			ShapingLimits: shaping.Limits{Profile: &shaping.ProfileLimits{
				WindowSeconds: 10, ExtraBytesPPM: 1000000, FloorBytes: 1 << 20, MaxAddedDelay: 2 * time.Millisecond,
			}}}
		a, b := net.Pipe()
		client, in := spliceTestPair(t, a, b, cfg)
		a, b = net.Pipe()
		carrier := &recConn{Conn: a}
		out, server := spliceTestPair(t, carrier, b, cfg)
		cs, receive := openSpliceTestPair(t, client, in)
		send, ss := openSpliceTestPair(t, out, server)
		mustSetShaper(t, out, planShaper(func(p *shaping.RecordPlan) { p.Pad = pad }), nil)
		synctest.Wait()
		carrier.start()
		hook := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			p, err := u.Alloc(1)
			if err != nil {
				return SpliceDecision{}, err
			}
			_, err = p.WriteAt([]byte{byte('a' + u.Meta().Origin)}, 0)
			return SpliceDecision{Replacement: []SplicePayload{p}}, err
		}
		c, err := StartSplice(t.Context(), receive, send, &SpliceOptions{HooksAtoB: SpliceHooks{Datagram: []SpliceHook{hook}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Abort(0); <-c.Done() })
		if err = cs.SendDatagram(1, []byte("received")); err != nil {
			t.Fatal(err)
		}
		for want := range 2 {
			if want == 1 {
				r := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceDatagram, ContextID: 2, Data: []byte("injected")})
				if got := <-r.Done; got.Status != SpliceSubmitted || got.Outputs != 1 || got.Bytes != 1 {
					t.Fatalf("injection disposition %+v", got)
				}
			}
			cx, p, err := ss.ReceiveDatagram(t.Context())
			if err != nil || cx != uint64(want+1) || len(p) != 1 || p[0] != byte('a'+want) {
				t.Fatalf("received context=%d payload=%q err=%v", cx, p, err)
			}
		}
		synctest.Wait()
		carrier.mu.Lock()
		records := bytes.Clone(carrier.out[carrier.from:])
		carrier.mu.Unlock()
		count := 0
		for len(records) > 0 {
			size, n := varint.ConsumeVarint(records)
			if n == 0 || size > uint64(len(records)-n) {
				t.Fatal("incomplete output record")
			}
			end := n + int(size)
			got := 0
			visitSpliceFrames(records[:end], func(f *wire.Frame) {
				if f.Kind == wire.KindDatagram {
					got++
				}
			})
			if got > 0 && paddedWire(int(size), end-len(bytes.TrimRight(records[:end], "\x00"))) < pad {
				t.Fatalf("datagram record had fewer than %d bytes of padding", pad)
			}
			count += got
			records = records[end:]
		}
		if count != 2 {
			t.Fatalf("expected two shaped datagrams, got %d", count)
		}
	})
}

func TestSpliceTestPairReleasesFrozenReadDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.NewFake(time.Unix(1, 0)))}
		a, b := net.Pipe()
		client, server := spliceTestPair(t, a, b, cfg)
		send, _ := openSpliceTestPair(t, client, server)
		send.CancelWrite(42)
		synctest.Wait()
		client.Abort(0, "")
		synctest.Wait() // Let the peer close start the server's read drain.
		server.Abort(0, "")
		_ = a.Close()
		_ = b.Close()
		synctest.Wait()
		select {
		case <-server.Done():
			t.Fatal("server closed before the unread reset was released")
		default:
		}
		// The stream cleanup releases the reset before the pair cleanup
		// waits for server.Done, even though the clock never advanced.
	})
}

func TestSpliceAbortPreservesFinishedDirection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan SpliceEvent, 4)
		cs, ss, c := controlledRelay(t, nil, &SpliceOptions{OnEventAtoB: func(e SpliceEvent) { events <- e }}, false)
		pause, err := c.AtoB.Pause(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err = pause.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err = cs.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = io.ReadAll(ss); err != nil {
			t.Fatal(err)
		}
		<-c.AtoB.Done()
		c.Abort(27)
		res, err := c.Wait(t.Context())
		if err != nil || res.AtoB.End.Kind != SpliceFinished || res.AtoB.Cause != nil ||
			res.BtoA.End != (SpliceEnd{Kind: SpliceCancelled, Code: 27}) || spliceCode(res.BtoA.Cause) != 27 {
			t.Fatalf("result %+v, err=%v", res, err)
		}
		if first, second := <-events, <-events; first.Name != SplicePauseAcknowledged || second.Name != SpliceDirectionFinished ||
			second.End.Kind != SpliceFinished || len(events) != 0 {
			t.Fatalf("events %+v, %+v and %d extra", first, second, len(events))
		}
	})
}

func TestSpliceTargetStopDuringDatagramReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleanup := make(chan struct{})
		defer close(cleanup)
		lim := newSpliceScriptLimiter(&spliceVectorLimiter{Hold: true}, cleanup)
		cfg := &Config{EnableDatagrams: true, ClockDomain: NewClockDomain(timeshim.NewFake(time.Unix(1, 0))),
			DatagramLimiter: DatagramLimiter{Send: lim}}
		_, ss, c := controlledRelay(t, cfg, nil, false)
		r := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceDatagram, Data: []byte("stopped")})
		synctest.Wait()
		lim.mu.Lock()
		requests := len(lim.requests)
		lim.mu.Unlock()
		if requests != 1 {
			t.Fatalf("outstanding target reservations %d, want 1", requests)
		}
		ss.CancelRead(13)
		synctest.Wait()
		lim.grant(7)
		if got := <-r.Done; got.Status != SpliceDirectionEnded || got.Outputs != 0 || got.Bytes != 0 ||
			got.End == nil || *got.End != (SpliceEnd{Kind: SpliceStopped, Code: 13}) {
			t.Fatalf("injection disposition %+v", got)
		}
		lim.mu.Lock()
		refunds := append([]int64(nil), lim.refunds...)
		lim.mu.Unlock()
		if len(refunds) != 1 || refunds[0] != 7 {
			t.Fatalf("target refunds %v", refunds)
		}
	})
}

func TestSpliceControlStopWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		wait := func(context.Context, SpliceUnit) (SpliceDecision, error) {
			return SpliceDecision{Wait: SpliceWait{Resume: gate}}, nil
		}
		opts := &SpliceOptions{HooksAtoB: SpliceHooks{Stream: []SpliceHook{wait}}}
		cs, ss, c := controlledRelay(t, nil, opts, false)
		r := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceStream, Data: []byte("pending")})
		ss.CancelRead(71)
		disposition := <-r.Done
		expected := SpliceEnd{Kind: SpliceStopped, Code: 71}
		if disposition.Status != SpliceDirectionEnded || disposition.End == nil || *disposition.End != expected || disposition.Bytes != 0 {
			t.Fatalf("receipt %+v", disposition)
		}
		close(gate)
		_ = ss.Close()
		if _, err := io.ReadAll(cs); err != nil {
			t.Fatal(err)
		}
		got, err := c.Wait(t.Context())
		if err != nil || got.AtoB.End != expected || got.BtoA.End.Kind != SpliceFinished {
			t.Fatalf("result %+v: %v", got, err)
		}
	})
}

func TestSpliceControlCapsuleMalformed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hooks := make(chan SpliceMeta, 2)
		inspect := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			hooks <- u.Meta()
			return SpliceDecision{}, nil
		}
		// Type=41 and Length=2, both encoded with two bytes.
		valid := []byte{0x40, 0x29, 0x40, 0x02, 'a', 'b'}
		// A two-byte Context cannot use bytes beyond the declared Value.
		invalid := []byte{0, 1, 0x40, 1}
		opts := &SpliceOptions{Chunk: 1, HooksAtoB: SpliceHooks{Capsule: []SpliceHook{inspect}}}
		cs, ss, c := controlledRelay(t, nil, opts, true)
		if _, err := cs.Write(append(bytes.Clone(valid), invalid...)); err != nil {
			t.Fatal(err)
		}
		_ = cs.Close()
		data, err := io.ReadAll(ss)
		var se *StreamError
		if !bytes.Equal(data, valid) || !errors.As(err, &se) || se.ErrorCode != StreamPacketError {
			t.Fatalf("capsule stream %x: %v", data, err)
		}
		got := <-hooks
		if got.Type != 41 || got.Offset != 0 || len(hooks) != 0 {
			t.Fatalf("hook metadata %+v", got)
		}
		_ = ss.Close()
		if result, err := c.Wait(t.Context()); err != nil || result.AtoB.End.Kind != SplicePacketError {
			t.Fatalf("result %+v: %v", result, err)
		}
	})
}

func TestSpliceControlReliableResetUsesOutputOffset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		resume := make(chan struct{})
		entered := make(chan struct{})
		output := []byte("different-length")
		replace := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			if u.Meta().Origin == SpliceInjected {
				return SpliceDecision{}, nil
			}
			p, err := u.Alloc(len(output))
			if err != nil {
				return SpliceDecision{}, err
			}
			if _, err := p.WriteAt(output, 0); err != nil {
				return SpliceDecision{}, err
			}
			close(entered)
			return SpliceDecision{Replacement: []SplicePayload{p}, Wait: SpliceWait{Resume: resume}}, nil
		}
		opts := &SpliceOptions{HooksAtoB: SpliceHooks{Capsule: []SpliceHook{replace}}}
		cs, ss, c := controlledRelay(t, nil, opts, true)
		r := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceCapsule, Type: 41, Data: []byte("injection")})
		<-r.Done
		raw := appendCapsuleHeader(nil, 17, 1)
		if _, err := cs.Write(append(raw, 'a')); err != nil {
			t.Fatal(err)
		}
		<-entered
		cs.SetReliableBoundary()
		cs.CancelWrite(92)
		synctest.Wait()
		close(resume)
		got, err := io.ReadAll(ss)
		expected := append(varint.AppendVarint(varint.AppendVarint(nil, 41), 9), []byte("injection")...)
		expected = append(expected, appendCapsuleHeader(nil, 17, len(output))...)
		expected = append(expected, output...)
		var se *StreamError
		if !bytes.Equal(got, expected) || !errors.As(err, &se) || se.ErrorCode != 92 {
			t.Fatalf("output was %x want %x: %v", got, expected, err)
		}
		_ = ss.Close()
		if result, err := c.Wait(t.Context()); err != nil || result.AtoB.Bytes != uint64(len(expected)) {
			t.Fatalf("result %+v: %v", result, err)
		}
	})
}

func TestSpliceControlAdmittedInjectionCanResetWithZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reset := func(context.Context, SpliceUnit) (SpliceDecision, error) {
			return SpliceDecision{Action: SpliceResetByHook}, nil
		}
		opts := &SpliceOptions{HooksAtoB: SpliceHooks{Stream: []SpliceHook{reset}}}
		cs, ss, c := controlledRelay(t, nil, opts, false)
		admission := injectSpliceTest(t, c.AtoB, SpliceInjection{Data: []byte("reset")})
		disposition := <-admission.Done
		expected := SpliceEnd{Kind: SpliceHookReset}
		if disposition.End == nil || *disposition.End != expected || disposition.Status != SpliceDirectionEnded ||
			disposition.Sequence != admission.Sequence || disposition.Bytes != 0 {
			t.Fatalf("admission %+v, disposition %+v", admission, disposition)
		}
		if _, err := io.ReadAll(ss); err == nil {
			t.Fatal("target did not receive reset code zero")
		}
		_ = ss.Close()
		if _, err := io.ReadAll(cs); err != nil {
			t.Fatal(err)
		}
		result, err := c.Wait(t.Context())
		if err != nil || result.AtoB.End != expected || result.BtoA.End.Kind != SpliceFinished {
			t.Fatalf("result %+v, err %v", result, err)
		}
	})
}

func TestSpliceSharedQueueAndMirrorRoots(t *testing.T) {
	d := spliceRelay{limits: SpliceLimits{QueueItems: 3, QueueBytes: 192}}
	m, err := NewSpliceMirror(SpliceMirrorOptions{Items: 3, BackingBytes: 192, MaxPayload: 1, Every: 1, Lease: true})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	group := func(b *core.Buf, pos int) *spliceGroup {
		b.Ref()
		return &spliceGroup{parts: []spliceMember{{n: 1, parts: []spliceChunk{{Data: b.B[pos : pos+1], buf: b}}}}}
	}
	root := core.NewBuf(make([]byte, 2, 192))
	defer root.Unref()
	copy(root.B, "ab")
	other := core.NewBuf([]byte("x"))
	defer other.Unref()
	first, second, excess := group(root, 0), group(root, 1), group(other, 0)
	if !d.enqueue(first) || !d.enqueue(second) || d.enqueue(excess) || d.queuedBytes != 192 {
		t.Fatalf("direction retains %d bytes in %d groups", d.queuedBytes, d.queuedItems)
	}
	for _, g := range []*spliceGroup{first, second, excess} {
		m.finish(m.sample(SpliceMeta{}, SpliceMirrorInput, 0, g.parts[0]), true)
	}
	d.release(first, nil)
	if d.queuedBytes != 192 || m.held != 192 || m.items != 2 {
		t.Fatalf("first release: direction %d, mirror %d/%d", d.queuedBytes, m.held, m.items)
	}
	d.release(second, nil)
	d.release(excess, nil)
	sample, err := m.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if string(sample.Data) != "a" || d.queuedBytes != 0 || m.held != 192 {
		t.Fatalf("after dequeue: sample %q, direction %d, mirror %d", sample.Data, d.queuedBytes, m.held)
	}
	m.Close()
	if root.Refs() != 1 || other.Refs() != 1 || m.items != 0 || m.held != 0 {
		t.Fatal("close did not retire the last mirror reference")
	}
}

func TestSpliceInjectionAdmissionPrecedesHookReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		hook := func(context.Context, SpliceUnit) (SpliceDecision, error) {
			close(entered)
			<-release
			return SpliceDecision{}, nil
		}
		cs, ss, c := controlledRelay(t, nil, &SpliceOptions{HooksAtoB: SpliceHooks{Stream: []SpliceHook{hook}}}, false)
		data := []byte("a")
		rc := injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceStream, Data: data})
		<-entered
		data[0] = 'x'
		ctx, cancel := context.WithCancel(t.Context())
		cancelled := make(chan error, 1)
		go func() {
			r, err := c.AtoB.Inject(ctx, SpliceInjection{Kind: SpliceStream, Data: data})
			if r.Status != SpliceRejected || r.Reason != SpliceCallCancelled {
				err = fmt.Errorf("unexpected rejection %+v", r)
			}
			cancelled <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-cancelled; !errors.Is(err, context.Canceled) {
			t.Fatalf("unaccepted injection: %v", err)
		}
		close(release)
		readSpliceTest(t, ss, "a")
		if disp := <-rc.Done; disp.Status != SpliceSubmitted || disp.Bytes != 1 {
			t.Fatalf("accepted disposition %+v", disp)
		}
		finishSpliceTest(t, cs, ss, c)
	})
}

func TestSpliceLaterBuilderIncludesEarlierRoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			p, err := u.AllocBacking(1, 192)
			return SpliceDecision{Replacement: []SplicePayload{p}}, err
		}
		second := func(_ context.Context, u SpliceUnit) (SpliceDecision, error) {
			_, _ = u.AllocBacking(1, 128)
			return SpliceDecision{}, nil
		}
		opts := &SpliceOptions{Chunk: 64, MaxPacketPayload: 32, Limits: SpliceLimits{OutputBytes: 64, BackingBytes: 256},
			HooksAtoB: SpliceHooks{Stream: []SpliceHook{first, second}}}
		_, _, c := controlledRelay(t, nil, opts, false)
		result := <-injectSpliceTest(t, c.AtoB, SpliceInjection{Kind: SpliceStream, Data: []byte("ab")}).Done
		st, err := c.AtoB.Stats(t.Context())
		if err != nil || !errors.Is(st.Cause, ErrSpliceBudget) || st.End.Kind != SpliceHookFailed ||
			result.Status != SpliceDirectionEnded || result.Bytes != 0 || result.Outputs != 0 {
			t.Fatalf("over-budget second builder published output: %+v %+v %v", result, st, err)
		}
	})
}

func TestSpliceReadyBetweenProbeAndWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := timeshim.NewFake(time.Unix(1, 0))
		d := spliceRelay{clock: newClock(NewClockDomain(fake)), finished: true, dgEnded: true}
		group := &spliceGroup{ready: core.Time(time.Second), expiry: core.Never}
		d.pendingDG = []*spliceGroup{group}
		if i, _ := d.popDatagram(); i != -1 {
			t.Fatal("group ready before its timer")
		}
		fake.Advance(time.Second)
		d.wait(t.Context())
		gate := make(chan struct{})
		group.resumes = []<-chan struct{}{gate}
		if i, _ := d.popDatagram(); i != -1 {
			t.Fatal("group ready before resume")
		}
		close(gate)
		d.wait(t.Context())
		if i, ready := d.popDatagram(); i != 0 || !ready {
			t.Fatal("owner missed readiness")
		}
	})
}

func TestSpliceCapsuleHeaderCopyMeters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pass := func(context.Context, SpliceUnit) (SpliceDecision, error) { return SpliceDecision{}, nil }
		cs, ss, c := controlledRelay(t, nil, &SpliceOptions{HooksAtoB: SpliceHooks{Capsule: []SpliceHook{pass}}}, true)
		capsule := []byte{0, 3, 7, 'a', 'b'}
		if _, err := cs.Write(capsule); err != nil {
			t.Fatal(err)
		}
		readSpliceTest(t, ss, string(capsule))
		stats := finishSpliceTest(t, cs, ss, c).AtoB
		if stats.PayloadCopyBytes != 0 || stats.EncodeBytes != 3 {
			t.Fatalf("payload copied %d, headers encoded %d", stats.PayloadCopyBytes, stats.EncodeBytes)
		}
	})
}

func visitSpliceFrames(b []byte, visit func(*wire.Frame)) {
	for len(b) != 0 {
		size, n := varint.ConsumeVarint(b)
		if n == 0 || size > uint64(len(b)-n) {
			panic("scripted carrier received a broken record")
		}
		record := b[n : n+int(size)]
		b = b[n+int(size):]
		for len(record) != 0 {
			var f wire.Frame
			n, err := wire.DecodeFrame(record, &f)
			if err != nil {
				panic(err)
			}
			visit(&f)
			record = record[n:]
		}
	}
}
