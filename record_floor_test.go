package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// sealedFloorCap is the lower carrier limit whose Sealed records hold
// exactly MinMaxRecord bytes of plaintext on a byte stream.
var sealedFloorCap = MinMaxRecord + sealOverhead(FramingByteStream)

// An application close reason is cut at 1024 bytes on a character boundary,
// by CloseWithError and by Abort alike, and the peer sees the cut phrase.
func TestCloseReasonCutAtCharacterBoundary(t *testing.T) {
	leakcheck.Check(t)
	long := strings.Repeat("\u65e5\u672c\u8a9e", 5000/9+1) // 3-byte characters: the 1024th byte is inside one
	if len(long) < 5000 {
		t.Fatal(len(long))
	}
	want := long[:1023]
	for name, closer := range map[string]func(c *Conn){
		"close": func(c *Conn) { _ = c.CloseWithError(7, long) },
		"abort": func(c *Conn) { c.Abort(7, long) },
	} {
		t.Run(name, func(t *testing.T) {
			a, b := tcpPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ch := make(chan *Conn, 1)
			go func() {
				s, err := Server(ctx, b, &Config{})
				if err != nil {
					s = nil
				}
				ch <- s
			}()
			client, err := Client(ctx, a, &Config{})
			if err != nil {
				t.Fatal(err)
			}
			server := <-ch
			if server == nil {
				t.Fatal("no server")
			}
			closer(client)
			for who, c := range map[string]*Conn{"closer": client, "peer": server} {
				var ae *ApplicationError
				if err := c.Wait(); !errors.As(err, &ae) || ae.ErrorCode != 7 {
					t.Fatalf("%s: %v", who, err)
				}
				if len(ae.ErrorMessage) > 1024 || !utf8.ValidString(ae.ErrorMessage) || ae.ErrorMessage != want {
					t.Fatalf("%s: reason of %d bytes, valid %v", who, len(ae.ErrorMessage), utf8.ValidString(ae.ErrorMessage))
				}
			}
		})
	}
}

// A carrier's own record limit below the floor is an invalid configuration
// at the pipe's attachment (Start, AddPipe, HandlePipe), after the overhead
// of a Sealed join too.
func TestCarrierRecordLimitBelowFloor(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	defer func() { _ = a.Close(); _ = b.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	low := PipeIO{R: a, W: limitedWriter{a, MinMaxRecord - 1}}
	if _, err := Start(ctx, low, RoleClient, &Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start: %v", err)
	}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), InstanceID: []byte("instance")})
	defer func() { _ = lst.Close() }()
	if err := lst.HandlePipe(ctx, low, PipeHint{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("HandlePipe: %v", err)
	}
	// A Sealed join takes sealOverhead off the lower limit: the plaintext
	// limit must still hold a floor record.
	e := newMPEnv(t, mpConfig(), mpConfig())
	nc, err := e.dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := NetPipe(nc)
	p.MaxRecord = MinMaxRecord
	if _, err := e.client.AddPipe(ctx, p, PipeOptions{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("AddPipe at the floor over Sealed: %v", err)
	}
}

type limitedWriter struct {
	net.Conn
	lim int
}

func (w limitedWriter) MaxRecord() int { return w.lim }

// A multipath session whose pipes all take records of the floor (8192
// bytes of plaintext): the first record's parameters, the epoch exchange,
// ACKs, datagrams and bulk data in both directions all go in such records.
func TestMultipathAtRecordFloor(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// The first pipe is no Sealed carrier, the joined ones are: a Sealed
	// pipe's lower limit holds the sealing on top of the floor. The listener
	// cannot tell the first from a join, so it takes the larger limit.
	capped := func(c net.Conn, limit int) PipeIO {
		p := NetPipe(c)
		p.MaxRecord = limit
		return p
	}
	scfg, ccfg := mpConfig(), mpConfig()
	scfg.EnableDatagrams, ccfg.EnableDatagrams = true, true
	lst := mustNewListener(&ListenerConfig{Config: scfg, InstanceID: []byte("instance")})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = lst.HandlePipe(ctx, capped(c, sealedFloorCap), PipeHint{}) }()
		}
	}()
	dial := func() net.Conn {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	cli, err := Start(ctx, capped(dial(), MinMaxRecord), RoleClient, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cli.Close()
		_ = srv.Close()
		waitDone(t, cli)
		waitDone(t, srv)
		_ = ln.Close()
		_ = lst.Close()
	}()
	for range 2 {
		if _, err := cli.AddPipe(ctx, capped(dial(), sealedFloorCap), PipeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cli.AdvanceEpoch(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 30*time.Second, "epoch 1 at both ends", func() bool { return cli.Epoch() == 1 && srv.Epoch() == 1 })
	cli.SetScheduler(SchedulerSpreadRoundRobin())
	for _, pair := range [][2]*Conn{{cli, srv}, {srv, cli}} {
		from, to := pair[0], pair[1]
		const n = 2 << 20
		st, err := from.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = st.Write(make([]byte, n)); _ = st.Close() }()
		rs, err := to.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := io.ReadAll(rs); err != nil || len(b) != n {
			t.Fatalf("read %d, %v", len(b), err)
		}
	}
	if m := cli.ConnectionState().MaxDatagramPayload; m <= 0 || m >= MinMaxRecord {
		t.Fatalf("MaxDatagramPayload %d", m)
	} else if err := cli.SendDatagram(make([]byte, m)); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Conn{cli, srv} {
		for i, ps := range c.PipeStats() {
			if ps.RecordsSent == 0 || ps.BytesSent > ps.RecordsSent*uint64(sealedFloorCap+2) {
				t.Fatalf("pipe %d: %d bytes in %d records", i, ps.BytesSent, ps.RecordsSent)
			}
		}
		if c.ConnectionStats().DatagramsDroppedSend != 0 {
			t.Fatalf("datagrams dropped: %+v", c.ConnectionStats())
		}
	}
}
