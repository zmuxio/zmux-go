package zmux

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestStreamTryWrite: TryWrite takes the prefix that fits now and never
// waits; with WaitWritable it moves a large buffer whole, and after the
// stream's end it reports the error.
func TestStreamTryWrite(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	cfg := &Config{SendBuffer: 64 << 10}
	client, server := pair(t, a, b, cfg, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB
	n, err := st.TryWrite(data)
	if err != nil || n == 0 || n >= len(data) {
		t.Fatalf("first TryWrite %d, %v", n, err)
	}
	got := make(chan []byte, 1)
	go func() {
		rs, err := server.AcceptStream(ctx)
		if err != nil {
			got <- nil
			return
		}
		b, _ := io.ReadAll(rs)
		got <- b
	}()
	polls, writes := 0, 0
	for n < len(data) {
		room, err := st.WaitWritable(ctx)
		polls++
		if err != nil || room == 0 {
			t.Fatal(room, err)
		}
		m, err := st.TryWrite(data[n:])
		if err != nil {
			t.Fatal(err)
		}
		if uint64(m) < min(room, uint64(len(data)-n)) {
			t.Fatalf("ready with %d, TryWrite took %d", room, m)
		}
		n += m
		writes++
	}
	if polls != writes {
		t.Fatalf("%d waits for %d writes: a ready wait whose write took nothing", polls, writes)
	}
	_ = st.Close()
	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("received %d bytes", len(b))
	}
	if m, err := st.TryWrite([]byte{1}); err == nil || m != 0 {
		t.Fatalf("after Close: %d, %v", m, err)
	}
	us, err := client.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if m, err := us.TryWrite([]byte("uni")); err != nil || m != 3 {
		t.Fatalf("uni TryWrite %d, %v", m, err)
	}
	us.CancelWrite(0)
}

// TestStreamWaitWritableIsRoomNotCredit: WaitWritable is ready exactly when
// TryWrite takes bytes. Room without credit (a stream not yet opened to a
// peer that granted nothing) is ready; a full buffer with credit left
// (the peer's carrier stalls) is not, cancelled waits consume nothing,
// and the carrier's resuming makes it ready again.
func TestStreamWaitWritableIsRoomNotCredit(t *testing.T) {
	leakcheck.Check(t)
	a, b := net.Pipe()
	gb := &gatedConn{Conn: b}
	scfg := &Config{InitialStreamReceiveWindow: 32 << 20, MaxStreamReceiveWindow: 32 << 20,
		InitialConnectionReceiveWindow: 32 << 20, MaxConnectionReceiveWindow: 32 << 20}
	client, server := pair(t, a, gb, &Config{SendBuffer: 64 << 10, SendBufferMax: 64 << 10}, scfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, 256<<10)
	if _, err := st.Write(big[:1]); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, rs) }()
	// Stall the peer's carrier, then fill it and the buffer: no write
	// takes anything for a while.
	gb.stall()
	defer gb.resume()
	for {
		wctx, wcancel := context.WithTimeout(ctx, 100*time.Millisecond)
		room, err := st.WaitWritable(wctx)
		wcancel()
		if err != nil {
			break
		}
		if m, _ := st.TryWrite(big); uint64(m) < min(room, uint64(len(big))) {
			t.Fatalf("ready with %d, TryWrite took %d", room, m)
		}
	}
	if st.SendCapacity() == 0 {
		t.Fatalf("credit should be left; written %d", st.Stats().BytesWritten)
	}
	for range 3 {
		wctx, wcancel := context.WithTimeout(ctx, 10*time.Millisecond)
		if _, err := st.WaitWritable(wctx); err == nil {
			t.Fatal("ready with a full buffer")
		}
		wcancel()
	}
	if m, err := st.TryWrite([]byte{1}); m != 0 || err != nil {
		t.Fatalf("full buffer took %d, %v", m, err)
	}
	gb.resume()
	room, err := st.WaitWritable(ctx)
	if err != nil || room == 0 {
		t.Fatal(room, err)
	}
	if m, err := st.TryWrite(big); err != nil || uint64(m) < min(room, uint64(len(big))) {
		t.Fatalf("ready with %d, TryWrite took %d, %v", room, m, err)
	}
	st.CancelWrite(1)
	if _, err := st.WaitWritable(ctx); err == nil {
		t.Fatal("ready after CancelWrite")
	}
}

// TestStreamCloseWakesWaitWritable: Close of the sending part ends a
// WaitWritable that waits in another goroutine.
func TestStreamCloseWakesWaitWritable(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	gb := &gatedConn{Conn: b}
	scfg := &Config{InitialStreamReceiveWindow: 32 << 20, MaxStreamReceiveWindow: 32 << 20,
		InitialConnectionReceiveWindow: 32 << 20, MaxConnectionReceiveWindow: 32 << 20}
	client, server := pair(t, a, gb, &Config{SendBuffer: 64 << 10, SendBufferMax: 64 << 10}, scfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(io.Discard, rs) }()
	gb.stall()
	defer gb.resume()
	big := make([]byte, 256<<10)
	for {
		wctx, wcancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err := st.WaitWritable(wctx)
		wcancel()
		if err != nil {
			break
		}
		_, _ = st.TryWrite(big)
	}
	done := make(chan error, 1)
	go func() {
		_, err := st.WaitWritable(ctx)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = st.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ready after Close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wake WaitWritable")
	}
}

// gatedConn stalls the reads that start between stall and resume.
type gatedConn struct {
	net.Conn
	gate atomic.Pointer[chan struct{}]
}

func (g *gatedConn) stall() {
	ch := make(chan struct{})
	g.gate.Store(&ch)
}

func (g *gatedConn) resume() {
	if p := g.gate.Swap(nil); p != nil {
		close(*p)
	}
}

func (g *gatedConn) Read(p []byte) (int, error) {
	if ch := g.gate.Load(); ch != nil {
		<-*ch
	}
	return g.Conn.Read(p)
}
