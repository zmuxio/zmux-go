package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// relayEnv is client -> relay -> server over two sessions.
func relayEnv(t *testing.T) (client, relayIn, relayOut, server *Conn) {
	a1, b1 := tcpPair(t)
	client, relayIn = pair(t, a1, b1, nil, nil)
	a2, b2 := tcpPair(t)
	relayOut, server = pair(t, a2, b2, nil, nil)
	return
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
		name string
		act  func(t *testing.T, client, server *Conn, cs, ss *Stream, cancel context.CancelFunc)
		want SpliceEnd // of the direction client -> server
	}{
		{"reset", func(_ *testing.T, _, _ *Conn, cs, _ *Stream, _ context.CancelFunc) { cs.CancelWrite(0x77) },
			SpliceEnd{Kind: SpliceReset, Code: 0x77}},
		{"stopped", func(_ *testing.T, _, _ *Conn, _, ss *Stream, _ context.CancelFunc) { ss.CancelRead(0x55) },
			SpliceEnd{Kind: SpliceStopped, Code: 0x55}},
		{"upstream lost", func(_ *testing.T, client, _ *Conn, _, _ *Stream, _ context.CancelFunc) { _ = client.Close() },
			SpliceEnd{Kind: SpliceUpstreamLost}},
		{"downstream lost", func(_ *testing.T, _, server *Conn, _, _ *Stream, _ context.CancelFunc) { _ = server.Close() },
			SpliceEnd{Kind: SpliceDownstreamLost}},
		{"cancelled", func(_ *testing.T, _, _ *Conn, _, _ *Stream, cancel context.CancelFunc) { cancel() },
			SpliceEnd{Kind: SpliceCancelled}},
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
			if lost := map[SpliceEndKind]SpliceEndKind{SpliceUpstreamLost: SpliceDownstreamLost,
				SpliceDownstreamLost: SpliceUpstreamLost}; lost[c.want.Kind] != 0 && r.BtoA.End.Kind != lost[c.want.Kind] {
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
