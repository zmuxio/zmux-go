package zmux

import (
	"bytes"
	"context"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// delayRelay forwards a to b, each chunk arriving d after it was read.
func delayRelay(a, b net.Conn, d time.Duration) {
	type item struct {
		at time.Time
		b  []byte
	}
	ch := make(chan item, 1<<12)
	go func() {
		defer func() { _ = b.Close() }()
		for it := range ch {
			time.Sleep(time.Until(it.at))
			if _, err := b.Write(it.b); err != nil {
				for range ch {
				}
				return
			}
		}
	}()
	buf := make([]byte, 16<<10)
	for {
		n, err := a.Read(buf)
		if n > 0 {
			ch <- item{time.Now().Add(d), append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			close(ch)
			return
		}
	}
}

// A receiver whose send direction is idle, on a carrier with a low-water
// mark (no limiter) and a long round trip, has only its own control frames to
// sample the RTT with; they are acknowledged late, and that must not keep its
// receive window at the initial value.
func TestAutotuneGrowsWithIdleReverseOnLowWaterCarrier(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := func() *Config {
		return &Config{MaxPipes: 4, PipeAuth: PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true}}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	sc := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			sc <- c
		}
		close(sc)
	}()
	// client <-> relay-in ... relay-out <-> server, 50 ms each way.
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = front.Close() }()
	cRaw, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	mid, err := front.Accept()
	if err != nil {
		t.Fatal(err)
	}
	out, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sRaw := <-sc
	const oneWay = 50 * time.Millisecond
	go delayRelay(mid, out, oneWay)
	go delayRelay(out, mid, oneWay)

	type res struct {
		c   *Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := startServerNoListener(ctx, NetPipe(sRaw), cfg())
		ch <- res{s, err}
	}()
	c, err := Start(ctx, NetPipe(cRaw), RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	s := r.c
	defer func() {
		_ = c.Close()
		_ = s.Close()
		waitDone(t, c)
		waitDone(t, s)
		_ = mid.Close()
		_ = out.Close()
	}()

	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const size = 4 << 20
	go func() { _, _ = st.Write(make([]byte, size)) }()
	as, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(as, make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	defer runtime.KeepAlive(st)
	if w := as.Stats().RecvWindow; w <= 256<<10 {
		t.Fatalf("receive window stayed at %d", w)
	}
}
