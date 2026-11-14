package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// mpEnv is a listener on TCP (newMPEnv) or in memory (newMemMPEnv, for
// tests in a testing/synctest bubble) and a client session dialled to it.
type mpEnv struct {
	ln     net.Listener
	dial   func(context.Context) (net.Conn, error) // connects to ln
	lst    *Listener
	client *Conn
	server *Conn
	cancel context.CancelFunc
}

func mpConfig() *Config {
	return &Config{MaxPipes: 4, PipeAuth: PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true},
		SchedulerParams: SchedulerParams{Probation: -1}}
}

func newMPEnv(t *testing.T, ccfg, scfg *Config) *mpEnv {
	t.Helper()
	return newMPEnvOn(t, ccfg, &ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
}

func newMPEnvOn(t *testing.T, ccfg *Config, lcfg *ListenerConfig) *mpEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return startMPEnv(t, ccfg, lcfg, ln, func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	})
}

// newMemMPEnv is newMPEnv over in-memory pipes; both configurations take
// the host clock as Config.Clock, which the bubble's fake clock drives on
// every platform (the session's own monotonic source on Windows is
// outside it).
func newMemMPEnv(t *testing.T, ccfg, scfg *Config) *mpEnv {
	t.Helper()
	ccfg.Clock, scfg.Clock = timeshim.Real, timeshim.Real
	ln := netx.NewChanListener(nil, 0)
	return startMPEnv(t, ccfg, &ListenerConfig{Config: scfg, InstanceID: []byte("instance")}, ln,
		func(ctx context.Context) (net.Conn, error) { return ln.Dial(ctx, nil) })
}

// pipeDialer is a PipePool.Dial to e's listener.
func (e *mpEnv) pipeDialer() func(context.Context) (PipeIO, error) {
	return func(ctx context.Context) (PipeIO, error) {
		nc, err := e.dial(ctx)
		if err != nil {
			return PipeIO{}, err
		}
		return NetPipe(nc), nil
	}
}

func startMPEnv(t *testing.T, ccfg *Config, lcfg *ListenerConfig, ln net.Listener, dial func(context.Context) (net.Conn, error)) *mpEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e := &mpEnv{ln: ln, dial: dial, lst: mustNewListener(lcfg), cancel: cancel}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = e.lst.HandlePipe(ctx, NetPipe(c), PipeHint{}) }()
		}
	}()
	dctx, dcancel := context.WithTimeout(ctx, 60*time.Second)
	defer dcancel()
	nc, err := dial(dctx)
	if err != nil {
		t.Fatal(err)
	}
	e.client, err = Client(dctx, nc, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	e.server, err = e.lst.Accept(dctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.client.Close()
		_ = e.server.Close()
		waitDone(t, e.client)
		waitDone(t, e.server)
		_ = ln.Close()
		_ = e.lst.Close()
		cancel()
	})
	return e
}

func (e *mpEnv) addPipe(t *testing.T, o PipeOptions) *Pipe {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, err := e.dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.client.AddPipe(ctx, NetPipe(nc), o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMultipathTransferAndFailover(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	if st := e.client.ConnectionState(); !st.Multipath {
		t.Fatal("multipath not negotiated")
	}
	p2 := e.addPipe(t, PipeOptions{})
	e.client.SetScheduler(SchedulerSpreadRoundRobin())

	payload := make([]byte, 4<<20)
	_, _ = rand.Read(payload)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	done := make(chan []byte, 1)
	go func() {
		s, err := e.server.AcceptStream(ctx)
		if err != nil {
			done <- nil
			return
		}
		b, _ := io.ReadAll(s)
		done <- b
	}()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	half := len(payload) / 2
	if _, err := s.Write(payload[:half]); err != nil {
		t.Fatal(err)
	}
	// Both pipes carry data; then the added pipe fails in the middle and
	// its unacknowledged data is reinjected.
	waitUntil(t, 60*time.Second, "data on pipe 2", func() bool {
		st, ok := p2.Stats()
		return ok && st.BytesSent >= 1<<16
	})
	if err := p2.Abort(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(payload[half:]); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	got := <-done
	if !bytes.Equal(got, payload) {
		t.Fatalf("received %d bytes, want %d intact", len(got), len(payload))
	}
	var used int
	for _, ps := range e.client.PipeStats() {
		if ps.BytesSent > 1<<16 {
			used++
		}
	}
	if used < 1 {
		t.Fatalf("no pipe carried data: %+v", e.client.PipeStats())
	}
	// The abort is one failure of the two attachments; whatever it
	// reinjected went again as copies of the stream.
	cs := e.client.ConnectionStats()
	if cs.PipesAdded != 2 || cs.PipeFailures != 1 || cs.Reinjections > 1 {
		t.Fatalf("added %d, failures %d, reinjections %d", cs.PipesAdded, cs.PipeFailures, cs.Reinjections)
	}
	// What went again earned nothing the second time.
	if m := e.client.ShapingMeters(); m.SentBytes == 0 || m.SentBytes+cs.BytesReinjected+cs.BytesRedundant > cs.BytesSent {
		t.Fatalf("earned %d of %d sent, %d again, %d copies", m.SentBytes, cs.BytesSent, cs.BytesReinjected, cs.BytesRedundant)
	}
	if st, ok := p2.Stats(); ok && (st.FailureCount != 1 || st.Failure == nil) {
		t.Fatalf("the failed pipe: %+v", st)
	}
	if ss := s.Stats(); ss.BytesWritten != uint64(len(payload)) || ss.BytesSent < ss.BytesWritten {
		t.Fatalf("stream stats %+v", ss)
	}
}

func TestMultipathEventsAndStats(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	w := uint64(3)
	p := e.addPipe(t, PipeOptions{Weight: Set(w), Label: "second"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := map[EventKind]bool{}
	for !seen[EventPipeJoined] {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen[ev.Kind] = true
	}
	st, ok := p.Stats()
	if !ok {
		t.Fatal("pipe gone")
	}
	if st.Label != "second" || st.Weight != 3 || st.Outbound != OutboundActive {
		t.Fatalf("stats %+v", st)
	}
	if !st.Local || st.LanesOut != 1 || st.LanesIn != 1 || st.Lanes != nil {
		t.Fatalf("own attachment without lanes: %+v", st)
	}
	// The joined pipe's first PIPE_PING is answered by an acknowledgement.
	waitUntil(t, 5*time.Second, "an answered PIPE_PING", func() bool {
		st, ok := p.Stats()
		return ok && st.PingsSent > 0 && st.PingsAnswered > 0 && st.PingsAnswered <= st.PingsSent
	})
	if cs := e.client.ConnectionStats(); cs.PipesActive != 2 || cs.PipesAdded != 2 {
		t.Fatalf("pipes active %d, added %d, want 2, 2", cs.PipesActive, cs.PipesAdded)
	}
	for _, ps := range e.server.PipeStats() {
		if ps.PipeID != 0 && ps.Local {
			t.Fatalf("the acceptor reports the peer's join as its own: %+v", ps)
		}
	}
	if err := p.Update(PipeUpdate{SendPriority: Set[uint64](5)}); err != nil {
		t.Fatal(err)
	}
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == EventPipePriorityChanged {
			if ev.PipeID != p.ID() || ev.Priority != 5 {
				t.Fatalf("event %+v", ev)
			}
			break
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Kind == EventPipeClosed && ev.PipeID == p.ID() ||
			ev.Kind == EventPipesRetired && slices.Contains(ev.Last, p.ID()) {
			break
		}
	}
}

// TestStreamPipeBound checks the companion of StreamStats.PipeID: an
// unbound sending direction reports PipeBound false, and one that the
// affinity scheduler bound to pipe 0, whose ID is also the zero value,
// reports PipeBound true.
func TestStreamPipeBound(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.PipeBound || st.PipeID != 0 {
		t.Fatalf("before any data: %+v", st)
	}
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	for st := s.Stats(); !st.PipeBound; st = s.Stats() {
		if ctx.Err() != nil {
			t.Fatalf("never bound: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	if st := s.Stats(); st.PipeID != 0 {
		t.Fatalf("bound to pipe %d, want the only pipe 0", st.PipeID)
	}
	s.CancelWrite(0)
}

// TestClearRedundant checks that ClearRedundant hands a stream's
// redundancy back to the session: an urgent stream with redundancy
// turned off sends no copies, and once cleared it follows the session's
// default (on for urgency 0 to 2) and sends copies on the second pipe.
func TestClearRedundant(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	e.addPipe(t, PipeOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	redundant := func() (n uint64) {
		for _, ps := range e.client.PipeStats() {
			n += ps.BytesRedundant
		}
		return n
	}
	s, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.SetPriority(1, false)
	s.SetRedundant(false)
	rs := make(chan *Stream, 1)
	go func() {
		r, err := e.server.AcceptStream(ctx)
		if err != nil {
			rs <- nil
			return
		}
		rs <- r
	}()
	chunk := make([]byte, 16<<10)
	exchange := func(r *Stream) {
		t.Helper()
		if _, err := s.Write(chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(r, make([]byte, len(chunk))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Write(chunk); err != nil {
		t.Fatal(err)
	}
	r := <-rs
	if r == nil {
		t.Fatal("no stream accepted")
	}
	if _, err := io.ReadFull(r, make([]byte, len(chunk))); err != nil {
		t.Fatal(err)
	}
	if n := redundant(); n != 0 {
		t.Fatalf("%d redundant bytes with redundancy off", n)
	}
	s.ClearRedundant()
	for redundant() == 0 {
		if ctx.Err() != nil {
			t.Fatal("no redundant copies after ClearRedundant")
		}
		exchange(r)
	}
	s.CancelWrite(0)
	r.CancelRead(0)
}

// TestPipeStatsAbsentDirection checks the Go form of an absent direction:
// a pipe joined send-only reports, at the joiner, InboundNone and every
// inbound field 0, and at the peer OutboundNone and every outbound field
// 0 (priority sources and weight included), while the present direction
// keeps its values.
func TestPipeStatsAbsentDirection(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{Direction: DirectionSendOnly})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var peer PipeStats
	for {
		var found bool
		for _, ps := range e.server.PipeStats() {
			if ps.PipeID == p.ID() && ps.Inbound == InboundOpen && ps.BytesReceived > 0 {
				peer, found = ps, true
			}
		}
		if found {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the peer never saw pipe %d open: %+v", p.ID(), e.server.PipeStats())
		}
		time.Sleep(time.Millisecond)
	}
	want := PipeStats{PipeID: peer.PipeID, Local: peer.Local, PeerAttached: peer.PeerAttached, FateGroup: peer.FateGroup, Label: peer.Label,
		Carrier: peer.Carrier, Inbound: peer.Inbound, AttachedFor: peer.AttachedFor,
		FailureCount: peer.FailureCount, Failure: peer.Failure, ReceivePriority: peer.ReceivePriority,
		ReceiveStandby: peer.ReceiveStandby, RequestSeq: peer.RequestSeq, BytesReceived: peer.BytesReceived,
		RecordsReceived: peer.RecordsReceived, BytesDuplicateReceived: peer.BytesDuplicateReceived,
		SinceLastReceive: peer.SinceLastReceive, SinceLastTraffic: peer.SinceLastTraffic, LanesIn: peer.LanesIn,
		Lanes: peer.Lanes}
	if !reflect.DeepEqual(peer, want) {
		t.Fatalf("receive-only pipe at the peer: %+v", peer)
	}
	if peer.BytesReceived == 0 || peer.LanesIn == 0 {
		t.Fatalf("the present inbound direction reads 0: %+v", peer)
	}
	own, ok := p.Stats()
	if !ok {
		t.Fatal("pipe gone")
	}
	if own.Inbound != InboundNone || own.BytesReceived != 0 || own.RecordsReceived != 0 || own.LanesIn != 0 ||
		own.SinceLastReceive != 0 || own.ReceivePriority != 0 || own.RequestSeq != 0 {
		t.Fatalf("send-only pipe at the joiner: %+v", own)
	}
	if own.Outbound == OutboundNone || own.BytesSent == 0 || own.Weight == 0 || own.SendPrioritySource == "" {
		t.Fatalf("the present outbound direction reads 0: %+v", own)
	}
}

// TestRetiredPipeHandle checks a handle after retirement: its reads
// report absence, and its operations fail with ErrPipeGone.
func TestRetiredPipeHandle(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{})
	if _, ok := p.Stats(); !ok {
		t.Fatal("a live pipe reads as gone")
	}
	if err := p.Abort(nil); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the pipe retired", func() bool { _, ok := p.Stats(); return !ok })
	for name, err := range map[string]error{"SetWeight": p.Update(PipeUpdate{Weight: Set[uint64](2)}), "SetSendPriority": p.Update(PipeUpdate{SendPriority: Set[uint64](1)}),
		"Revalidate": p.Revalidate(), "Close": p.Close(), "Abort": p.Abort(nil)} {
		if !errors.Is(err, ErrPipeGone) {
			t.Errorf("%s on a retired pipe: %v", name, err)
		}
	}
}

// TestPipesRetiredEvent checks retirement through the session: a pipe
// that fails and retires while nobody reads events leaves no event of
// its own, only an EventPipesRetired that names it and counts its unread
// failure; no EventPipeClosed stands for the retirement.
func TestPipesRetiredEvent(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	p := e.addPipe(t, PipeOptions{})
	if err := p.Abort(nil); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, "the pipe retired", func() bool { _, ok := p.Stats(); return !ok })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sum *Event
	for {
		ev, err := e.client.NextEvent(ctx)
		if err != nil {
			break
		}
		if ev.PipeID == p.ID() && pipeEvent(ev.Kind) {
			t.Fatalf("event %v of the retired pipe", ev.Kind)
		}
		if ev.Kind == EventPipesRetired {
			sum = &ev
			break
		}
	}
	if sum == nil || sum.Retired != 1 || sum.Failed != 1 || !slices.Equal(sum.Last, []PipeID{p.ID()}) {
		t.Fatalf("summary %+v", sum)
	}
}
