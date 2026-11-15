package zmux

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

const testRecordCap = MinMaxRecord + 64 // the floor and the room a Sealed join takes

// checkRecordCap: the capped end's datagrams fit its records, the other
// end's do not see the bound, and a transfer on pipe 0 goes out in records
// of at most testRecordCap.
func checkRecordCap(t *testing.T, ctx context.Context, capped, other *Conn) {
	t.Helper()
	m := capped.ConnectionState().MaxDatagramPayload
	if m <= 0 || m >= testRecordCap {
		t.Fatalf("capped MaxDatagramPayload %d", m)
	}
	if o := other.ConnectionState().MaxDatagramPayload; o <= testRecordCap {
		t.Fatalf("uncapped MaxDatagramPayload %d", o)
	}
	if err := capped.SendDatagram(make([]byte, m)); err != nil {
		t.Fatal(err)
	}
	var tl *DatagramTooLargeError
	if err := capped.SendDatagram(make([]byte, m+1)); !errors.As(err, &tl) {
		t.Fatalf("one byte above: %v", err)
	}
	const n = 200_000
	before := pipe0Records(t, capped)
	st, err := capped.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(make([]byte, n)); _ = st.Close() }()
	rs, err := other.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(rs); err != nil || len(b) != n {
		t.Fatalf("read %d, %v", len(b), err)
	}
	if got := pipe0Records(t, capped) - before; got < n/testRecordCap {
		t.Fatalf("%d records for %d bytes", got, n)
	}
}

func pipe0Records(t *testing.T, c *Conn) uint64 {
	t.Helper()
	p, ok := c.Pipe(0)
	if !ok {
		t.Fatal("no pipe 0")
	}
	s, _ := p.Stats()
	return s.RecordsSent
}

func capped(c net.Conn) PipeIO {
	p := NetPipe(c)
	p.MaxRecord = testRecordCap
	return p
}

// TestPipeMaxRecordFirstPipe: PipeIO.MaxRecord bounds pipe 0 whichever way
// the session starts.
func TestPipeMaxRecordFirstPipe(t *testing.T) {
	cfg := func() *Config { return &Config{EnableDatagrams: true} }
	run := func(t *testing.T, clientCapped bool, start func(ctx context.Context, a, b net.Conn) (*Conn, *Conn)) {
		t.Helper()
		leakcheck.Check(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a, b := tcpPair(t)
		cli, srv := start(ctx, a, b)
		defer func() { _ = cli.Close(); _ = srv.Close(); waitDone(t, cli); waitDone(t, srv) }()
		if clientCapped {
			checkRecordCap(t, ctx, cli, srv)
		} else {
			checkRecordCap(t, ctx, srv, cli)
		}
	}
	both := func(ctx context.Context, ca, sa PipeIO, lst *Listener) (*Conn, *Conn) {
		type res struct {
			c   *Conn
			err error
		}
		ch := make(chan res, 1)
		go func() {
			if lst != nil {
				go func() { _ = lst.HandlePipe(ctx, sa, PipeHint{}) }()
				s, err := lst.Accept(ctx)
				ch <- res{s, err}
				return
			}
			s, err := Start(ctx, sa, RoleServer, cfg())
			ch <- res{s, err}
		}()
		c, err := Start(ctx, ca, RoleClient, cfg())
		if err != nil {
			t.Fatal(err)
		}
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}
		return c, r.c
	}
	t.Run("connect", func(t *testing.T) {
		run(t, true, func(ctx context.Context, a, b net.Conn) (*Conn, *Conn) {
			return both(ctx, capped(a), NetPipe(b), nil)
		})
	})
	t.Run("accept", func(t *testing.T) {
		run(t, false, func(ctx context.Context, a, b net.Conn) (*Conn, *Conn) {
			return both(ctx, NetPipe(a), capped(b), nil)
		})
	})
	t.Run("listener", func(t *testing.T) {
		lst := mustNewListener(&ListenerConfig{Config: cfg()})
		defer func() { _ = lst.Close() }()
		run(t, false, func(ctx context.Context, a, b net.Conn) (*Conn, *Conn) {
			return both(ctx, NetPipe(a), capped(b), lst)
		})
	})
}

// TestPipeMaxRecordAddedPipe: an added pipe's MaxRecord bounds the
// records written on it.
func TestPipeMaxRecordAddedPipe(t *testing.T) {
	leakcheck.Check(t)
	scfg := mpConfig()
	scfg.EnableDatagrams = true
	lst := mustNewListener(&ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	carrier := func() net.Conn {
		a, b := tcpPair(t)
		go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
		return a
	}
	ccfg := mpConfig()
	ccfg.EnableDatagrams = true
	cli, err := Client(ctx, carrier(), ccfg)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close(); _ = srv.Close(); waitDone(t, cli); waitDone(t, srv) }()
	p, err := cli.AddPipe(ctx, capped(carrier()), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Each pipe carries a share of a large transfer; the added pipe's
	// records stay within its bound (a record is its Size field and at
	// most testRecordCap bytes).
	const n = 4 << 20
	st, err := cli.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write(make([]byte, n)); _ = st.Close() }()
	rs, err := srv.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(rs); err != nil || len(b) != n {
		t.Fatalf("read %d, %v", len(b), err)
	}
	s, _ := p.Stats()
	if s.RecordsSent == 0 || s.BytesSent > s.RecordsSent*(testRecordCap+2) {
		t.Fatalf("added pipe: %d bytes in %d records", s.BytesSent, s.RecordsSent)
	}
}

// TestPipeMaxRecordInvalid: a MaxRecord that is neither 0 nor at least
// MinMaxRecord is ErrInvalidConfig, with no upper bound; a
// CarrierRecordLimit below the floor is too.
func TestPipeMaxRecordInvalid(t *testing.T) {
	for _, v := range []int{-1, 1, MinMaxRecord - 1} {
		if err := (PipeIO{MaxRecord: v}).validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("MaxRecord %d: %v", v, err)
		}
	}
	for _, v := range []int{0, MinMaxRecord, MaxRecordSize, MaxRecordSize + 1, 1 << 20, math.MaxInt32} {
		if err := (PipeIO{MaxRecord: v}).validate(); err != nil {
			t.Fatalf("MaxRecord %d: %v", v, err)
		}
	}
	if got := carrierRecordLimit(PipeIO{W: recLimitW(4500), MaxRecord: 5000}); got != 4500 {
		t.Fatal(got)
	}
	if got := carrierRecordLimit(PipeIO{W: recLimitW(6000), MaxRecord: 5000}); got != 5000 {
		t.Fatal(got)
	}
	// A carrier's own limit below the floor is an invalid configuration.
	for _, v := range []int{1, MinMaxRecord - 1} {
		if err := (PipeIO{W: recLimitW(v)}).validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("carrier limit %d: %v", v, err)
		}
	}
	if err := (PipeIO{W: recLimitW(MinMaxRecord)}).validate(); err != nil {
		t.Fatal(err)
	}
	if got := carrierRecordLimit(PipeIO{W: recLimitW(0), MaxRecord: 5000}); got != 5000 {
		t.Fatal(got) // a half's 0 is no limit
	}
}

type recLimitW int

func (recLimitW) Write(p []byte) (int, error) { return len(p), nil }
func (recLimitW) Close() error                { return nil }
func (w recLimitW) MaxRecord() int            { return int(w) }

// TestPipeMaxRecordPaired: a paired carrier takes the write half's
// MaxRecord; the read half's does not bound what this end writes.
func TestPipeMaxRecordPaired(t *testing.T) {
	up, down := PipeIO{MaxRecord: 5000}, PipeIO{MaxRecord: 4500}
	if got := PairedPipe(up, down).MaxRecord; got != 5000 {
		t.Fatalf("write half capped: %d", got)
	}
	if got := PairedPipe(down, up).MaxRecord; got != 4500 {
		t.Fatalf("write half capped (swapped): %d", got)
	}
	if got := PairedPipe(PipeIO{}, PipeIO{MaxRecord: 4500}).MaxRecord; got != 0 {
		t.Fatalf("read half only: %d", got)
	}
}

// TestPipeMaxRecordAboveImplementationMax: a MaxRecord above the
// implementation's record limit is accepted and behaves like that limit.
func TestPipeMaxRecordAboveImplementationMax(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state := func(limit int) int {
		a, b := tcpPair(t)
		pa, pb := NetPipe(a), NetPipe(b)
		pa.MaxRecord = limit
		cfg := &Config{EnableDatagrams: true}
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Start(ctx, pb, RoleServer, cfg)
			if err != nil {
				t.Error(err)
			}
			ch <- s
		}()
		c, err := Start(ctx, pa, RoleClient, cfg)
		if err != nil {
			t.Fatal(err)
		}
		s := <-ch
		defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = st.Write(make([]byte, 300_000)); _ = st.Close() }()
		rs, err := s.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := io.ReadAll(rs); err != nil || len(b) != 300_000 {
			t.Fatalf("read %d, %v", len(b), err)
		}
		return c.ConnectionState().MaxDatagramPayload
	}
	base := state(0)
	for _, v := range []int{MaxRecordSize + 1, 1 << 20, math.MaxInt32} {
		if got := state(v); got != base {
			t.Fatalf("MaxRecord %d: MaxDatagramPayload %d, default %d", v, got, base)
		}
	}
}
