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

func TestChunkRelayWithoutCopy(t *testing.T) {
	leakcheck.Check(t)
	a1, b1 := tcpPair(t)
	c1, s1 := pair(t, a1, b1, nil, nil)
	a2, b2 := tcpPair(t)
	c2, s2 := pair(t, a2, b2, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// c1 -> s1 relays -> c2 -> s2.
	go func() {
		in, err := s1.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := c2.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		for {
			ch, err := in.ReadChunk(ctx, 1<<20)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					t.Errorf("relay read: %v", err)
				}
				_ = out.Close()
				return
			}
			if err := out.WriteChunk(ctx, ch); err != nil {
				t.Errorf("relay write: %v", err)
				return
			}
		}
	}()
	src, err := c1.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.SetPreamble([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 3<<20)
	_, _ = rand.Read(msg)
	go func() {
		_, _ = src.Write(msg)
		_ = src.Close()
	}()
	dst, err := s2.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if _, err := dst.WriteTo(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), msg) {
		t.Fatalf("relay mismatch: %d bytes", got.Len())
	}
}

func TestPreambleAndCapacity(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, &Config{InitialStreamReceiveWindow: 16 << 10})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetPreamble([]byte("open")); err != nil {
		t.Fatal(err)
	}
	if err := st.AddPreambleTLV(0x1234, []byte("v")); err != nil {
		t.Fatal(err)
	}
	st.SetPriority(1, false)
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(ss.Preamble()) != "open" || len(ss.PreambleTLVs()) != 1 || ss.PreambleTLVs()[0].Type != 0x1234 {
		t.Fatalf("preamble %q %+v", ss.Preamble(), ss.PreambleTLVs())
	}
	if err := st.SetPreamble([]byte("late")); err == nil {
		t.Fatal("preamble after the first frame accepted")
	}
	// The stream window is 16 KiB: the capacity drops as data goes out and
	// returns as the peer reads.
	n, err := st.WaitSendCapacity(ctx, 1)
	if err != nil || n == 0 || n > 16<<10 {
		t.Fatalf("capacity %d %v", n, err)
	}
	go func() { _, _ = io.Copy(io.Discard, ss) }()
	big := make([]byte, 1<<20)
	if _, err := st.Write(big); err != nil {
		t.Fatal(err)
	}
	// At least 8 KiB, or above 0 once nothing is left unsent.
	if n, err := st.WaitSendCapacity(ctx, 8<<10); err != nil || n == 0 {
		t.Fatalf("capacity after reads %d %v", n, err)
	}
	_ = st.Close()
}
