package zmux_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// benchBulk measures one bulk stream from c to s.
func benchBulk(b *testing.B, c, s *zmux.Conn) {
	b.Helper()
	ctx := context.Background()
	go func() {
		st, err := s.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, st)
		_ = st.Close()
	}()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	_ = st.Close()
	_, _ = io.ReadAll(st)
	b.StopTimer()
}

// startPair starts a client and a server session over two pipe inputs.
func startPair(b *testing.B, cp, sp zmux.PipeIO) (*zmux.Conn, *zmux.Conn) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := make(chan *zmux.Conn, 1)
	go func() {
		s, err := zmux.Start(ctx, sp, zmux.RoleServer, nil)
		if err != nil {
			b.Error(err)
		}
		ch <- s
	}()
	c, err := zmux.Start(ctx, cp, zmux.RoleClient, nil)
	if err != nil {
		b.Fatal(err)
	}
	s := <-ch
	if s == nil {
		b.FailNow()
	}
	b.Cleanup(func() { closeBoth(c, s) })
	return c, s
}

// BenchmarkBulkTLS: a TLS carrier (contiguous batches).
func BenchmarkBulkTLS(b *testing.B) {
	scfg, ccfg, err := testTLSConfigs()
	if err != nil {
		b.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ach := make(chan net.Conn, 1)
	go func() {
		a, err := ln.Accept()
		if err != nil {
			ach <- nil
			return
		}
		ach <- tls.Server(a, scfg)
	}()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	sc := <-ach
	if sc == nil {
		b.Fatal("accept failed")
	}
	ccfg.ServerName = "127.0.0.1"
	cc := tls.Client(d, ccfg)
	c, s := startPair(b, zmux.NetPipe(cc), zmux.NetPipe(sc))
	benchBulk(b, c, s)
}

// BenchmarkBulkRecords: a message-framed carrier (WebSocket stand-in).
func BenchmarkBulkRecords(b *testing.B) {
	ca, sa := memWebSocketPair()
	c, s := startPair(b, wsPipe(ca), wsPipe(sa))
	benchBulk(b, c, s)
}

// BenchmarkBulkHTTP2: a full-duplex HTTP/2 request carrier.
func BenchmarkBulkHTTP2(b *testing.B) {
	lst := mustNewListener(nil)
	hh, err := zmux.NewHTTPHandler(lst, nil)
	if err != nil {
		b.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(hh)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	defer func() { _ = lst.Close() }()
	// The request lives as long as its context: no timeout on it.
	ctx := b.Context()
	p, err := zmux.HTTPPipe(ctx, srv.Client(), srv.URL, nil, zmux.DirBidirectional, nil)
	if err != nil {
		b.Fatal(err)
	}
	c, err := zmux.Start(ctx, p, zmux.RoleClient, nil)
	if err != nil {
		b.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer closeBoth(c, s)
	b.Run("up", func(b *testing.B) { benchBulk(b, c, s) })
	b.Run("down", func(b *testing.B) { benchBulk(b, s, c) })
}
