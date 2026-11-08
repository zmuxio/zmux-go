package zmuxws

import (
	"bytes"
	"encoding/binary"
	"net"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/djylb/netx/wsconn"
	"github.com/zmuxio/zmux-go"
)

// countConn records the writes to a connection.
type countConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
}

func (c *countConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, bytes.Clone(p))
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func (c *countConn) log() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.writes)
}

func (c *countConn) reset() {
	c.mu.Lock()
	c.writes = nil
	c.mu.Unlock()
}

// frames returns the unmasked payloads of the WebSocket frames in b,
// which must hold whole frames.
func frames(t *testing.T, b []byte) [][]byte {
	t.Helper()
	var out [][]byte
	for len(b) > 0 {
		hl, l := 2, uint64(b[1]&0x7f)
		switch l {
		case 126:
			l, hl = uint64(binary.BigEndian.Uint16(b[2:])), 4
		case 127:
			l, hl = binary.BigEndian.Uint64(b[2:]), 10
		}
		var key []byte
		if b[1]&0x80 != 0 {
			key = b[hl : hl+4]
			hl += 4
		}
		if uint64(len(b)-hl) < l {
			t.Fatalf("a frame cut across writes")
		}
		p := bytes.Clone(b[hl : hl+int(l)])
		for i := range p {
			if key != nil {
				p[i] ^= key[i%4]
			}
		}
		out = append(out, p)
		b = b[hl+int(l):]
	}
	return out
}

// countedPair is pipePair with the client's writes recorded from the end
// of the handshake.
func countedPair(t *testing.T) (srv, cli zmux.PipeIO, cc *countConn) {
	t.Helper()
	ctx := testContext(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	ch := make(chan *wsconn.Conn, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			ch <- nil
			return
		}
		c, _ := wsconn.Accept(ctx, nc, &wsconn.ServerOptions{Options: options})
		ch <- c
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cc = &countConn{Conn: nc}
	c, _, err := wsconn.Client(ctx, cc, "ws://"+ln.Addr().String(), &wsconn.ClientOptions{Options: options})
	if err != nil {
		t.Fatal(err)
	}
	sc := <-ch
	if sc == nil {
		t.Fatal("no server connection")
	}
	t.Cleanup(func() {
		_ = nc.Close()
		_ = sc.NetConn().Close()
	})
	cc.reset()
	return Pipe(sc), Pipe(c), cc
}

// A batch of records leaves in one write, as one message each.
func TestWriteMessages(t *testing.T) {
	srv, cli, cc := countedPair(t)
	msgs := [][]byte{[]byte("one"), {}, bytes.Repeat([]byte{9}, zmux.MaxRecordSize), []byte("four")}
	if err := cli.W.(zmux.MessageBatchWriter).WriteMessages(msgs); err != nil {
		t.Fatal(err)
	}
	if w := cc.log(); len(w) != 1 || len(frames(t, w[0])) != len(msgs) {
		t.Fatalf("%d writes", len(w))
	}
	r := srv.R.(zmux.MessageReader)
	for _, m := range msgs {
		if got, err := r.ReadMessage(); err != nil || !bytes.Equal(got, m) {
			t.Fatalf("read %d bytes, %v", len(got), err)
		}
	}
}

// A held first flight leaves in one write, the transport parameters with
// the records that follow them, and the stream written before the release
// arrives.
func TestHoldFirstFlightWebSocket(t *testing.T) {
	ctx := testContext(t)
	srv, cli, cc := countedPair(t)
	cfg := mpConfig()
	cfg.EarlyData = zmux.EarlyData{ZmuxPeer: true, HoldFirstFlight: true}
	c, err := zmux.Start(ctx, cli, zmux.RoleClient, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sch := make(chan *zmux.Conn, 1)
	go func() {
		s, err := zmux.Start(ctx, srv, zmux.RoleServer, singlePipe(mpConfig()))
		if err != nil {
			t.Error(err)
		}
		sch <- s
	}()
	st, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// The handle stays reachable until the peer has read: a collected
	// unclosed Stream resets its stream, which then never opens.
	defer runtime.KeepAlive(st)
	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if n := len(cc.log()); n != 0 {
		t.Fatalf("%d writes before the release", n)
	}
	c.ReleaseFirstFlight()
	s := <-sch
	if s == nil {
		_ = c.Close()
		return
	}
	defer closeBoth(c, s)
	ss, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := ss.Read(got); err != nil || string(got) != "hello" {
		t.Fatalf("read %q, %v", got, err)
	}
	// The first write carries the parameters, HELLO and the stream.
	w := cc.log()
	if len(w) == 0 || !bytes.Contains(bytes.Join(frames(t, w[0]), nil), []byte("hello")) {
		t.Fatalf("the stream is not in the first of %d writes", len(w))
	}
}

// plainCarrier hides WriteMessages: a carrier written one message at a
// time, as before.
type plainCarrier struct {
	zmux.MessageReader
	zmux.MessageWriter
	io interface {
		Read([]byte) (int, error)
		Write([]byte) (int, error)
		Close() error
	}
}

func (p *plainCarrier) Read(b []byte) (int, error)  { return p.io.Read(b) }
func (p *plainCarrier) Write(b []byte) (int, error) { return p.io.Write(b) }
func (p *plainCarrier) Close() error                { return p.io.Close() }

// A batching end and one that writes a message at a time interoperate.
func TestBatchInterop(t *testing.T) {
	ctx := testContext(t)
	srv, cli := pipePair(t)
	cr := cli.W.(*carrier)
	pc := &plainCarrier{MessageReader: cr, MessageWriter: cr, io: cr}
	cli = zmux.PipeIO{R: pc, W: pc, Framing: zmux.FramingRecords, Carrier: zmux.CarrierWebSocket}
	if _, ok := cli.W.(zmux.MessageBatchWriter); ok {
		t.Fatal("plain carrier batches")
	}
	sch := make(chan *zmux.Conn, 1)
	go func() {
		s, err := zmux.Start(ctx, srv, zmux.RoleServer, singlePipe(mpConfig()))
		if err != nil {
			t.Error(err)
		}
		sch <- s
	}()
	c, err := zmux.Start(ctx, cli, zmux.RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s := <-sch
	if s == nil {
		_ = c.Close()
		return
	}
	defer closeBoth(c, s)
	echo(t, ctx, c, s, 1<<20)
	echo(t, ctx, s, c, 1<<20)
}

// singlePipe is cfg for a server session without a Listener, which takes
// no joins: no multipath and no resume.
func singlePipe(cfg *zmux.Config) *zmux.Config {
	cfg.MaxPipes, cfg.ResumeTimeout = 0, 0
	return cfg
}
