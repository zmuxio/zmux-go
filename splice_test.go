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
	if r.AtoB.Bytes != uint64(len(msg)) || r.BtoA.Bytes != uint64(len(msg)) || r.AtoB.End != nil {
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
