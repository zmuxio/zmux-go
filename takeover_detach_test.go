package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// takeoverEnv is a listener whose admission hands every carrier over
// with the value svc, and a fallback that runs f.
func takeoverEnv(t *testing.T, peek int, f func(context.Context, *Takeover)) *Listener {
	t.Helper()
	adm := &Admission{Peek: peek, PeekTimeout: 5 * time.Second,
		Decide: func(context.Context, CarrierInfo, []byte) Decision {
			return Decision{Verdict: VerdictFallback, Value: "svc"}
		}}
	l := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm, Fallback: f})
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// tlsCarrier hands the server end of a TLS connection over net.Pipe,
// declared Secure (SecureTLS), to l, and returns the client end once its
// handshake is done, and the channel HandlePipe's end is sent to.
func tlsCarrier(t *testing.T, l *Listener) (*tls.Conn, <-chan error) {
	t.Helper()
	srvConf, cliConf := selfSignedTLS(t)
	srvConf.MinVersion = tls.VersionTLS13
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- l.HandlePipe(context.Background(), SecureTLS(tls.Server(a, srvConf)), PipeHint{}) }()
	cli := tls.Client(b, cliConf)
	t.Cleanup(func() { _ = cli.Close() })
	if err := cli.HandshakeContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cli, done
}

func waitHandled(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HandlePipe did not return")
	}
}

// Detach keeps the carrier open after Fallback returned: another goroutine
// serves it with the bytes read replayed and every capability of the
// TLS carrier (SecureTLS) intact through the replay view, and closes it.
func TestTakeoverDetach(t *testing.T) {
	leakcheck.Check(t)
	got := make(chan *Takeover, 1)
	l := takeoverEnv(t, 4, func(_ context.Context, tk *Takeover) {
		if err := tk.Detach(); err != nil {
			t.Error(err)
		}
		if err := tk.Detach(); err != nil { // idempotent
			t.Error(err)
		}
		got <- tk
	})
	cli, done := tlsCarrier(t, l)
	if _, err := cli.Write([]byte("HOSTping")); err != nil {
		t.Fatal(err)
	}
	tk := <-got
	waitHandled(t, done) // Fallback returned and zmux is done with the carrier
	if tk.Value != "svc" || tk.Info.TLS == nil {
		t.Fatalf("value %v, info %+v", tk.Value, tk.Info)
	}
	if err := tk.Detach(); err != nil {
		t.Fatalf("Detach after return of a detached carrier: %v", err)
	}
	c, ok := tk.Conn()
	if !ok {
		t.Fatal("no Conn view")
	}
	if _, _, ok := tk.Messages(); ok {
		t.Fatal("Messages on a byte stream")
	}

	// The capabilities of the carrier, through the view.
	under, ok := c.(interface{ Unwrap() net.Conn })
	if !ok {
		t.Fatal("no Unwrap")
	}
	stc, ok := under.Unwrap().(*secureTLSConn)
	if !ok {
		t.Fatalf("Unwrap is %T", under.Unwrap())
	}
	if st := c.(ConnectionStater).ConnectionState(); !st.HandshakeComplete || st.Version != tls.VersionTLS13 {
		t.Fatalf("ConnectionState %+v", st)
	}
	if nc := c.(interface{ NetConn() net.Conn }).NetConn(); nc != stc.NetConn() {
		t.Fatal("NetConn is not the connection below TLS")
	}
	if !c.(CarrierSecure).CarrierSecure() || !c.(TLSRecordCarrier).TLSRecords() || !IsCarrierSecure(tk.Pipe()) {
		t.Fatal("Secure declaration or TLS records lost")
	}
	if err := c.(Handshaker).HandshakeContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.(Aborter); !ok {
		t.Fatal("no Aborter")
	}
	if _, ok := TerminatedTLS(tk.Pipe()); !ok {
		t.Fatal("TerminatedTLS lost")
	}
	e, ok := Exporter(tk.Pipe())
	if !ok {
		t.Fatal("no exporter")
	}
	k1, err1 := e.ExportKeyingMaterial("x", nil, 16)
	cst := cli.ConnectionState()
	k2, err2 := cst.ExportKeyingMaterial("x", nil, 16)
	if err1 != nil || err2 != nil || !bytes.Equal(k1, k2) {
		t.Fatalf("exporter: %v %v", err1, err2)
	}

	// Served by another goroutine after Fallback returned.
	served := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "HOSTping" {
			served <- errors.New("replay " + string(buf))
			return
		}
		_, err := c.Write([]byte("pong"))
		served <- err
	}()
	reply := make([]byte, 4)
	if _, err := io.ReadFull(cli, reply); err != nil || string(reply) != "pong" {
		t.Fatalf("reply %q, %v", reply, err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	// The host closes it.
	ended := make(chan error, 1) // net.Pipe: the close_notify needs a reader
	go func() { _, err := cli.Read(reply); ended <- err }()
	_ = c.Close()
	if err := <-ended; err == nil {
		t.Fatal("the carrier did not end with the host's Close")
	}
}

// Without Detach the carrier is closed when Fallback returns, and a later
// Detach is refused.
func TestTakeoverNoDetach(t *testing.T) {
	leakcheck.Check(t)
	got := make(chan *Takeover, 1)
	l := takeoverEnv(t, 4, func(_ context.Context, tk *Takeover) { got <- tk })
	cli, done := tlsCarrier(t, l)
	if _, err := cli.Write([]byte("HOST")); err != nil {
		t.Fatal(err)
	}
	tk := <-got
	read := make(chan error, 1) // net.Pipe: the close_notify needs a reader
	go func() { _, err := cli.Read(make([]byte, 1)); read <- err }()
	waitHandled(t, done)
	if err := tk.Detach(); !errors.Is(err, ErrTakeoverReturned) {
		t.Fatalf("Detach after return: %v", err)
	}
	if err := <-read; err == nil {
		t.Fatal("the carrier was not closed")
	}
}

// chanMsgHalf is an in-memory message-framed carrier half: it reads the
// messages of in and writes to out.
type chanMsgHalf struct {
	in   chan []byte
	out  chan []byte
	done chan struct{}
	once sync.Once
}

func newChanMsgHalf() *chanMsgHalf {
	return &chanMsgHalf{in: make(chan []byte, 8), out: make(chan []byte, 8), done: make(chan struct{})}
}

func (m *chanMsgHalf) Read([]byte) (int, error)  { return 0, errors.New("messages") }
func (m *chanMsgHalf) Write([]byte) (int, error) { return 0, errors.New("messages") }
func (m *chanMsgHalf) Close() error              { m.once.Do(func() { close(m.done) }); return nil }
func (m *chanMsgHalf) ReadMessage(limit int) (ReceivedMessage, error) {
	select {
	case b := <-m.in:
		if len(b) > limit {
			return ReceivedMessage{}, ErrMessageTooLarge
		}
		return OwnMessage(b), nil
	case <-m.done:
		return ReceivedMessage{}, net.ErrClosed
	}
}

func (m *chanMsgHalf) WriteMessage(p []byte) error {
	select {
	case <-m.done:
		return net.ErrClosed
	default:
	}
	m.out <- bytes.Clone(p)
	return nil
}

func (m *chanMsgHalf) CarrierConn() any { return m }

// A message-framed carrier: Messages gives its halves, the first message
// replayed first, and Value reaches the takeover; the view of its
// receiving half names the carrier's connection.
func TestTakeoverMessages(t *testing.T) {
	leakcheck.Check(t)
	got := make(chan *Takeover, 1)
	l := takeoverEnv(t, 1, func(_ context.Context, tk *Takeover) {
		_ = tk.Detach()
		got <- tk
	})
	m := newChanMsgHalf()
	m.in <- []byte("first")
	m.in <- []byte("second")
	done := make(chan error, 1)
	go func() {
		done <- l.HandlePipe(context.Background(), PipeIO{R: m, W: m, Framing: FramingRecords}, PipeHint{})
	}()
	tk := <-got
	waitHandled(t, done)
	if tk.Value != "svc" {
		t.Fatalf("value %v", tk.Value)
	}
	mr, mw, ok := tk.Messages()
	if !ok || mr == nil || mw == nil {
		t.Fatal("no message halves")
	}
	if _, ok := tk.Conn(); ok {
		t.Fatal("Conn on a message carrier")
	}
	for _, want := range []string{"first", "second"} {
		b, err := mr.ReadMessage(64)
		got := string(b.Bytes())
		b.Release()
		if err != nil || got != want {
			t.Fatalf("message %q, %v; want %q", got, err, want)
		}
	}
	if err := mw.WriteMessage([]byte("reply")); err != nil || string(<-m.out) != "reply" {
		t.Fatal("reply", err)
	}
	if cc := tk.Pipe().R.(CarrierConner).CarrierConn(); cc != m {
		t.Fatal("CarrierConn lost")
	}
	if u := tk.Pipe().R.(interface{ Unwrap() io.ReadCloser }).Unwrap(); u != m {
		t.Fatal("Unwrap is not the half")
	}
	select {
	case <-m.done:
		t.Fatal("closed after Detach")
	default:
	}
	ClosePipeIO(tk.Pipe())
	<-m.done
}

// takeoverCountConn counts its Close calls.
type takeoverCountConn struct {
	net.Conn
	mu     sync.Mutex
	closes int
}

func (c *takeoverCountConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *takeoverCountConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// plainCarrier hands one end of net.Pipe, counted, to l with first
// written, and returns the other end.
func plainCarrier(t *testing.T, l *Listener, first string) (*takeoverCountConn, <-chan error) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	c := &takeoverCountConn{Conn: a}
	done := make(chan error, 1)
	go func() { done <- l.HandlePipe(context.Background(), NetPipe(c), PipeHint{}) }()
	if _, err := b.Write([]byte(first)); err != nil {
		t.Fatal(err)
	}
	return c, done
}

// The context Fallback gets ends when it returns, also after Detach.
func TestTakeoverContextEndsWithCall(t *testing.T) {
	leakcheck.Check(t)
	ctxs := make(chan context.Context, 1)
	tks := make(chan *Takeover, 1)
	l := takeoverEnv(t, 4, func(ctx context.Context, tk *Takeover) {
		_ = tk.Detach()
		if ctx.Err() != nil {
			t.Error("ended during the call")
		}
		ctxs <- ctx
		tks <- tk
	})
	_, done := plainCarrier(t, l, "HOST")
	waitHandled(t, done)
	if (<-ctxs).Err() == nil {
		t.Fatal("the call's context outlived the call")
	}
	c, _ := (<-tks).Conn()
	_ = c.Close()
}

// The host's Close goes through the carrier's one owner: Detach after it
// reports the carrier closed, and zmux's close when Fallback returns does
// not close it again.
func TestTakeoverCloseOnce(t *testing.T) {
	leakcheck.Check(t)
	for _, detach := range []bool{true, false} {
		errs := make(chan error, 1)
		l := takeoverEnv(t, 4, func(_ context.Context, tk *Takeover) {
			c, _ := tk.Conn()
			_ = c.Close()
			_ = c.Close()
			ClosePipeIO(tk.Pipe())
			if detach {
				errs <- tk.Detach()
			} else {
				errs <- nil
			}
		})
		cc, done := plainCarrier(t, l, "HOST")
		waitHandled(t, done)
		if err := <-errs; detach && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Detach of a closed carrier: %v", err)
		}
		if n := cc.count(); n != 1 {
			t.Fatalf("detach %v: closed %d times", detach, n)
		}
	}
}
