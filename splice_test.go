package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// relayEnv is client -> relay -> server over two sessions.
func relayEnv(t *testing.T) (client, relayIn, relayOut, server *Conn) {
	t.Helper()
	a1, b1 := tcpPair(t)
	client, relayIn = pair(t, a1, b1, nil, nil)
	a2, b2 := tcpPair(t)
	relayOut, server = pair(t, a2, b2, nil, nil)
	return client, relayIn, relayOut, server
}

func TestSpliceForwardsBothWays(t *testing.T) {
	leakcheck.Check(t)
	client, rin, rout, server := relayEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan SpliceResult, 1)
	go func() {
		in, err := rin.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := rout.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		r, _ := Splice(ctx, in, out, nil)
		done <- r
	}()
	go func() {
		s, err := server.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(s, s)
		_ = s.Close()
	}()
	s, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 2<<20)
	_, _ = rand.Read(msg)
	go func() { _, _ = s.Write(msg); _ = s.Close() }()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("splice echo: %d bytes, %v", len(got), err)
	}
	r := <-done
	if r.AtoB.Bytes != uint64(len(msg)) || r.BtoA.Bytes != uint64(len(msg)) || r.AtoB.End != (SpliceEnd{}) || r.BtoA.End.Kind != SpliceFinished {
		t.Fatalf("result %+v", r)
	}
}

func TestSpliceResetTravels(t *testing.T) {
	leakcheck.Check(t)
	client, rin, rout, server := relayEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		in, err := rin.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := rout.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		_, _ = Splice(ctx, in, out, nil)
	}()
	s, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write([]byte("hello"))
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(ss, buf); err != nil {
		t.Fatal(err)
	}
	s.CancelWrite(0x77)
	_, err = ss.Read(buf)
	var se *StreamError
	if !errors.As(err, &se) || se.ErrorCode != 0x77 {
		t.Fatalf("read after reset: %v", err)
	}
	s.CancelRead(0)
}

// TestSpliceEnds checks how each direction reports its end: a reset and a
// stop with their codes, the loss of the upstream or the downstream
// session, and a cancellation.
func TestSpliceEnds(t *testing.T) {
	for _, c := range []struct {
		act  func(t *testing.T, client, server *Conn, cs, ss *Stream, cancel context.CancelFunc)
		name string
		want SpliceEnd // of the direction client -> server
	}{
		{name: "reset", act: func(_ *testing.T, _, _ *Conn, cs, _ *Stream, _ context.CancelFunc) { cs.CancelWrite(0x77) },
			want: SpliceEnd{Kind: SpliceReset, Code: 0x77}},
		{name: "stopped", act: func(_ *testing.T, _, _ *Conn, _, ss *Stream, _ context.CancelFunc) { ss.CancelRead(0x55) },
			want: SpliceEnd{Kind: SpliceStopped, Code: 0x55}},
		{name: "upstream lost", act: func(_ *testing.T, client, _ *Conn, _, _ *Stream, _ context.CancelFunc) { _ = client.Close() },
			want: SpliceEnd{Kind: SpliceUpstreamLost}},
		{name: "downstream lost", act: func(_ *testing.T, _, server *Conn, _, _ *Stream, _ context.CancelFunc) { _ = server.Close() },
			want: SpliceEnd{Kind: SpliceDownstreamLost}},
		{name: "cancelled", act: func(_ *testing.T, _, _ *Conn, _, _ *Stream, cancel context.CancelFunc) { cancel() },
			want: SpliceEnd{Kind: SpliceCancelled, Code: StreamCancelled}},
	} {
		t.Run(c.name, func(t *testing.T) {
			leakcheck.Check(t)
			client, rin, rout, server := relayEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sctx, scancel := context.WithCancel(ctx)
			defer scancel()
			done := make(chan SpliceResult, 1)
			go func() {
				in, err := rin.AcceptStream(ctx)
				if err != nil {
					return
				}
				out, err := rout.OpenStreamSync(ctx)
				if err != nil {
					return
				}
				r, _ := Splice(sctx, in, out, nil)
				done <- r
			}()
			cs, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cs.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			ss, err := server.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(ss, make([]byte, 5)); err != nil {
				t.Fatal(err)
			}
			c.act(t, client, server, cs, ss, scancel)
			if c.name == "reset" || c.name == "stopped" {
				// The other direction finishes, so that the splice ends.
				_ = ss.Close()
			}
			var r SpliceResult
			select {
			case r = <-done:
			case <-ctx.Done():
				t.Fatal("the splice did not end")
			}
			if r.AtoB.End != c.want {
				t.Fatalf("client -> server ended %v, want %v (%+v)", r.AtoB.End, c.want, r)
			}
			// A lost session ends the other direction too, from its side.
			lost := map[SpliceEndKind]SpliceEndKind{SpliceUpstreamLost: SpliceDownstreamLost,
				SpliceDownstreamLost: SpliceUpstreamLost}
			if lost[c.want.Kind] != 0 && r.BtoA.End.Kind != lost[c.want.Kind] {
				t.Fatalf("server -> client ended %v", r.BtoA.End)
			}
			cs.CancelWrite(0)
			cs.CancelRead(0)
			ss.CancelWrite(0)
			ss.CancelRead(0)
		})
	}
	if got := (SpliceEnd{Kind: SpliceReset, Code: 0x77}).String(); got != "reset 0x77" {
		t.Errorf("String = %q", got)
	}
}

// TestSpliceCancelCause: a ctx cancelled with a cause that carries a stream
// code ends both streams of both directions with that code; any other cause
// or a plain cancel ends them with 0.
func TestSpliceCancelCause(t *testing.T) {
	for _, f := range []struct {
		fn   func(context.Context, *Stream, *Stream, *SpliceOptions) (SpliceResult, error)
		name string
	}{{name: "Splice", fn: Splice}, {name: "SplicePackets", fn: SplicePackets}} {
		for _, c := range []struct {
			cause error
			name  string
			want  StreamErrorCode // of the reset and of SpliceEnd
			stop  StreamErrorCode // of the stop
		}{
			{name: "code", cause: CancelCause(0x77), want: 0x77, stop: 0x77},
			{name: "wrapped code", cause: fmt.Errorf("refused: %w", CancelCause(0x77)), want: 0x77, stop: 0x77},
			{name: "stream error", cause: &StreamError{ErrorCode: 0x77, Remote: true}, want: 0x77, stop: 0x77},
			{name: "explicit zero", cause: CancelCause(0), want: 0, stop: 0},
			{name: "other cause", cause: errors.New("shutdown"), want: StreamCancelled, stop: 0},
			{name: "plain cancel", cause: nil, want: StreamCancelled, stop: 0},
		} {
			t.Run(f.name+"/"+c.name, func(t *testing.T) {
				leakcheck.Check(t)
				client, rin, rout, server := packetRelayEnv(t, "tcp")
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				sctx, scancel := context.WithCancelCause(ctx)
				defer scancel(nil)
				cs, ss, done := relayStreams(t, ctx, client, rin, rout, server,
					func(_ context.Context, a, b *Stream) (SpliceResult, error) { return f.fn(sctx, a, b, nil) })
				scancel(c.cause)
				var r SpliceResult
				select {
				case r = <-done:
				case <-ctx.Done():
					t.Fatal("the splice did not end")
				}
				want := SpliceEnd{Kind: SpliceCancelled, Code: c.want}
				if r.AtoB.End != want || r.BtoA.End != want {
					t.Fatalf("ends %v / %v, want %v", r.AtoB.End, r.BtoA.End, want)
				}
				// Each peer sees a reset on its receiving side and a stop on
				// its sending side.
				for name, s := range map[string]*Stream{"client": cs, "server": ss} {
					_, rerr := s.Read(make([]byte, 1))
					var se *StreamError
					if !errors.As(rerr, &se) || se.ErrorCode != c.want {
						t.Errorf("%s read: %v, want code %#x", name, rerr, uint64(c.want))
					}
					werr := waitStreamStop(ctx, s)
					if !errors.As(werr, &se) || se.ErrorCode != c.stop {
						t.Errorf("%s write: %v, want code %#x", name, werr, uint64(c.stop))
					}
				}
			})
		}
	}
}
