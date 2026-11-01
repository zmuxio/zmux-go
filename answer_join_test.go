package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// joinRecord is a byte-stream record holding only a PIPE_JOIN for loc.
func joinRecord(loc []byte) []byte {
	f := wire.AppendJoin(nil, &wire.Join{Locator: loc, PipeID: 1, Dir: 2, Token: make([]byte, 16)})
	return append(varint.AppendVarint(nil, uint64(len(f))), f...)
}

// locatorFor is a 16-byte locator starting with id.
func locatorFor(id []byte) []byte {
	loc := make([]byte, 16)
	n := copy(loc, id)
	_, _ = rand.Read(loc[n:])
	return loc
}

// sendJoin hands one carrier to lst, writes first on it and returns what
// the listener wrote before it ended the carrier.
func sendJoin(t *testing.T, lst *Listener, first []byte) []byte {
	t.Helper()
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = lst.HandlePipe(ctx, NetPipe(b), JoinHint{}); close(done) }()
	_ = a.SetDeadline(time.Now().Add(5 * time.Second))
	go func() { _, _ = a.Write(first) }()
	got, _ := io.ReadAll(a)
	_ = a.Close()
	<-done
	return got
}

// TestAnswerUnknownJoins checks the listener's answer to an admitted,
// well-formed join whose locator starts with its InstanceID but names no
// session: the fixed CONNECTION_CLOSE with ZMUX_RESUME_FAILED when
// AnswerUnknownJoins is on, counted as rejected, and no byte when it is
// off, for another InstanceID, and for every other rejected carrier.
func TestAnswerUnknownJoins(t *testing.T) {
	leakcheck.Check(t)
	id := []byte("inst")
	want := []byte{0x07, 0x1c, 0x8f, 0x05, 0xf7, 0x4e, 0x00, 0x00}
	on := NewListener(&ListenerConfig{Config: mpConfig(), InstanceID: id, AnswerUnknownJoins: true})
	defer func() { _ = on.Close() }()
	off := NewListener(&ListenerConfig{Config: mpConfig(), InstanceID: id})
	defer func() { _ = off.Close() }()

	if got := sendJoin(t, on, joinRecord(locatorFor(id))); !bytes.Equal(got, want) {
		t.Fatalf("on: answered %x, want %x", got, want)
	}
	if st := on.Stats(); st.Rejected != 1 {
		t.Fatalf("on: stats %+v", st)
	}
	if got := sendJoin(t, off, joinRecord(locatorFor(id))); len(got) != 0 {
		t.Fatalf("off: answered %x", got)
	}
	if got := sendJoin(t, on, joinRecord(locatorFor([]byte("other")))); len(got) != 0 {
		t.Fatalf("other instance: answered %x", got)
	}
	if got := sendJoin(t, on, append([]byte{0x05}, bytes.Repeat([]byte{0xee}, 5)...)); len(got) != 0 {
		t.Fatalf("malformed: answered %x", got)
	}

	// A known locator with a bad token: still silent.
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = on.HandlePipe(ctx, NetPipe(b), JoinHint{}) }()
	cli, err := Client(ctx, a, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := on.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	on.mu.Lock()
	var loc []byte
	for k := range on.sessions {
		loc = []byte(k)
	}
	on.mu.Unlock()
	if !bytes.HasPrefix(loc, id) {
		t.Fatalf("locator %x does not start with the instance ID", loc)
	}
	if got := sendJoin(t, on, joinRecord(loc)); len(got) != 0 {
		t.Fatalf("bad token: answered %x", got)
	}
	_ = cli.Close()
	_ = srv.Close()
	waitDone(t, cli)
	waitDone(t, srv)

	// An admission that closes the carrier: no byte either.
	var n atomic.Int32
	adm := NewListener(&ListenerConfig{Config: mpConfig(), InstanceID: id, AnswerUnknownJoins: true,
		Admission: decideWith(VerdictClose, &n)})
	defer func() { _ = adm.Close() }()
	if got := sendJoin(t, adm, joinRecord(locatorFor(id))); len(got) != 0 || n.Load() != 1 {
		t.Fatalf("admission close: answered %x after %d decisions", got, n.Load())
	}
}

// msgCarrier is one end of an in-memory message-framed carrier.
type msgCarrier struct {
	in, out chan []byte
	once    *sync.Once
	done    chan struct{}
}

func msgCarriers() (*msgCarrier, *msgCarrier) {
	x, y, once, done := make(chan []byte, 8), make(chan []byte, 8), new(sync.Once), make(chan struct{})
	return &msgCarrier{in: x, out: y, once: once, done: done}, &msgCarrier{in: y, out: x, once: once, done: done}
}

func (m *msgCarrier) ReadMessage() ([]byte, error) {
	select {
	case b := <-m.in:
		return b, nil
	case <-m.done:
		select {
		case b := <-m.in:
			return b, nil
		default:
			return nil, io.EOF
		}
	}
}

func (m *msgCarrier) WriteMessage(p []byte) error {
	select {
	case <-m.done:
		return net.ErrClosed
	default:
	}
	m.out <- bytes.Clone(p)
	return nil
}

func (m *msgCarrier) Read([]byte) (int, error)  { return 0, errors.New("message carrier") }
func (m *msgCarrier) Write([]byte) (int, error) { return 0, errors.New("message carrier") }
func (m *msgCarrier) Close() error              { m.once.Do(func() { close(m.done) }); return nil }

// TestAnswerUnknownJoinsMessages checks the answer on a message-framed
// carrier: one 7-byte message.
func TestAnswerUnknownJoinsMessages(t *testing.T) {
	leakcheck.Check(t)
	id := []byte("inst")
	lst := NewListener(&ListenerConfig{Config: mpConfig(), InstanceID: id, AnswerUnknownJoins: true})
	defer func() { _ = lst.Close() }()
	x, y := msgCarriers()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = lst.HandlePipe(ctx, PipeIO{R: y, W: y, Framing: FramingRecords}, JoinHint{})
		close(done)
	}()
	rec := joinRecord(locatorFor(id))
	_, n := varint.ConsumeVarint(rec)
	if err := x.WriteMessage(rec[n:]); err != nil {
		t.Fatal(err)
	}
	m, err := x.ReadMessage()
	if want := []byte{0x1c, 0x8f, 0x05, 0xf7, 0x4e, 0x00, 0x00}; err != nil || !bytes.Equal(m, want) {
		t.Fatalf("answered %x, %v; want %x", m, err, want)
	}
	<-done
	if m, err := x.ReadMessage(); err == nil {
		t.Fatalf("a second message %x", m)
	}
}

// answerEnv is a client session on a listener, and a second listener with
// the same InstanceID that holds none of its sessions (a restarted
// server), answering unknown joins.
type answerEnv struct {
	client, server *Conn
	carrier        net.Conn // the client's end of its bootstrap carrier
	other          *Listener
	ctx            context.Context
}

func newAnswerEnv(t *testing.T, ccfg *Config) *answerEnv {
	t.Helper()
	id := []byte("inst")
	scfg := mpConfig()
	scfg.ResumeTimeout = 30 * time.Second
	lst := NewListener(&ListenerConfig{Config: scfg, InstanceID: id})
	other := NewListener(&ListenerConfig{Config: scfg, InstanceID: id, AnswerUnknownJoins: true})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	a, b := net.Pipe()
	go func() { _ = lst.HandlePipe(ctx, NetPipe(b), JoinHint{}) }()
	cli, err := Client(ctx, a, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
		waitDone(t, cli)
		waitDone(t, srv)
		_ = lst.Close()
		_ = other.Close()
		cancel()
	})
	return &answerEnv{client: cli, server: srv, carrier: a, other: other, ctx: ctx}
}

// join adds a pipe to the client over a carrier to the other listener.
func (e *answerEnv) join() error {
	x, y := net.Pipe()
	go func() { _ = e.other.HandlePipe(e.ctx, NetPipe(y), JoinHint{}) }()
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	_, err := e.client.AddPipe(ctx, NetPipe(x), PipeOptions{})
	return err
}

// TestResumeAnsweredUnknown checks that a suspended session whose resume
// join is answered with ZMUX_RESUME_FAILED ends at once with it, long
// before its resume timeout.
func TestResumeAnsweredUnknown(t *testing.T) {
	leakcheck.Check(t)
	ccfg := mpConfig()
	ccfg.ResumeTimeout = 30 * time.Second
	e := newAnswerEnv(t, ccfg)
	_ = e.carrier.Close()
	waitUntil(t, 5*time.Second, "suspended", func() bool { return e.client.ConnectionStats().Suspensions == 1 })
	start := time.Now()
	if err := e.join(); err == nil {
		t.Fatal("join succeeded")
	}
	select {
	case <-e.client.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session still open")
	}
	err := e.client.Wait()
	var te *TransportError
	if !errors.Is(err, ErrResumeFailed) || !errors.As(err, &te) || te.ErrorCode != TransportResumeFailed {
		t.Fatalf("session ended with %v", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("ended after %v", el)
	}
}

// TestJoinAnsweredUnknown checks that a join of a live session that is
// answered with ZMUX_RESUME_FAILED fails alone: the session goes on, and
// the refusal backoff counts it.
func TestJoinAnsweredUnknown(t *testing.T) {
	leakcheck.Check(t)
	ccfg := mpConfig()
	ccfg.ResumeTimeout = 30 * time.Second
	e := newAnswerEnv(t, ccfg)
	if err := e.join(); err == nil {
		t.Fatal("join succeeded")
	}
	if st := e.client.ConnectionStats(); st.JoinRefusals != 1 {
		t.Fatalf("join refusals %d", st.JoinRefusals)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		if s, err := e.server.AcceptStream(ctx); err == nil {
			_, _ = io.Copy(s, s)
			_ = s.Close()
		}
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if b, err := io.ReadAll(s); err != nil || string(b) != "ping" {
		t.Fatalf("echo %q, %v", b, err)
	}
}

// TestAnswerUnknownJoinsReceiveOnly checks that a carrier without a
// sending half (an HTTP split uplink request, a lane) gets no answer to
// an unknown join and is closed, counted as rejected.
func TestAnswerUnknownJoinsReceiveOnly(t *testing.T) {
	leakcheck.Check(t)
	id := []byte("inst")
	lst := NewListener(&ListenerConfig{Config: mpConfig(), InstanceID: id, AnswerUnknownJoins: true})
	defer func() { _ = lst.Close() }()

	// A receive-only carrier with an in-band join.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := io.NopCloser(bytes.NewReader(joinRecord(locatorFor(id))))
	if err := lst.HandlePipe(ctx, PipeIO{R: r}, JoinHint{}); err == nil {
		t.Fatal("lane join accepted")
	}

	// An HTTP split uplink with an out-of-band join.
	srv := httptest.NewServer(mustHTTPHandler(t, lst))
	defer srv.Close()
	f := wire.AppendJoin(nil, &wire.Join{Locator: locatorFor(id), PipeID: 1, Dir: 1, Token: make([]byte, 16)})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, io.MultiReader(bytes.NewReader([]byte("x"))))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(HeaderJoin, base64.RawURLEncoding.EncodeToString(f))
	if resp, err := srv.Client().Do(req); err == nil {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if len(b) != 0 {
			t.Fatalf("uplink: response body %x", b)
		}
	}
	srv.Client().Transport.(*http.Transport).CloseIdleConnections()
	if st := lst.Stats(); st.Rejected != 2 {
		t.Fatalf("stats %+v", st)
	}
}
