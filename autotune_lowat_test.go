package zmux

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// virtualTCPDelay advances the receiving session's clock before its
// control bytes reach the network. NetConn exposes the actual TCP socket
// so the driver can apply the TCP low-water mark.
type virtualTCPDelay struct {
	net.Conn
	clock   *timeshim.Fake
	enabled atomic.Bool
}

const controlDelay = 50 * time.Millisecond

func (c *virtualTCPDelay) NetConn() net.Conn { return c.Conn }

func (c *virtualTCPDelay) Write(p []byte) (int, error) {
	if c.enabled.Load() {
		c.clock.Advance(controlDelay)
	}
	return c.Conn.Write(p)
}

// A receiver with an idle send direction samples its RTT from control
// frames. Even when those frames take time to deliver, the receive window
// must grow on a TCP carrier with a low-water mark.
func TestAutotuneGrowsWithIdleReverseOnLowWaterCarrier(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	a, b := tcpPair(t)
	clock := timeshim.NewFake(time.Now())
	receiver := &virtualTCPDelay{Conn: b, clock: clock}
	receiver.enabled.Store(true)
	ccfg, scfg := mpConfig(), mpConfig()
	scfg.ClockDomain = NewClockDomain(clock)
	closeCarrier := func() { _ = a.Close(); _ = b.Close() }
	defer closeCarrier()
	stop := context.AfterFunc(ctx, closeCarrier)
	defer stop()
	type result struct {
		conn *Conn
		err  error
	}
	server := make(chan result, 1)
	go func() {
		s, err := startServerNoListener(ctx, NetPipe(receiver), scfg)
		server <- result{s, err}
	}()
	c, err := Client(ctx, a, ccfg)
	r := <-server
	defer func() {
		for _, conn := range []*Conn{c, r.conn} {
			if conn != nil {
				conn.Abort(0, "")
			}
		}
		closeCarrier()
		for _, conn := range []*Conn{c, r.conn} {
			if conn != nil {
				waitDone(t, conn)
			}
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	s := r.conn
	pipe, ok := s.Pipe(0)
	if !ok {
		t.Fatal("no receiver bootstrap pipe")
	}
	if err := pipe.Revalidate(); err != nil {
		t.Fatal(err)
	}
	for pipeStat(t, pipe).Revalidating {
		waitTestEvent(t, ctx, s, "receiver probe acknowledgement", func(e Event) bool {
			return e.Kind == EventPipeHealthChanged && e.PipeID == pipe.ID() && e.Direction == DirectionSendOnly
		})
	}
	receiver.enabled.Store(false)
	rtt := pipeStat(t, pipe).MinRTT
	if rtt < controlDelay {
		t.Fatalf("probe RTT %v, want at least %v", rtt, controlDelay)
	}

	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The first burst gives the rate sampler a baseline. The next one
	// crosses its RTT interval and drives the receiver's credit updates.
	burst := make([]byte, 192<<10)
	if _, err := st.Write(burst); err != nil {
		t.Fatal(err)
	}
	as, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(as, burst); err != nil {
		t.Fatal(err)
	}
	clock.Advance(rtt)
	for range 4 {
		chunk := burst[:64<<10]
		if _, err := st.Write(chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(as, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if w := as.Stats().RecvWindow; w <= 256<<10 {
		t.Fatalf("receive window stayed at %d; pipes %+v", w, s.PipeStats())
	}
	if initial, limit := as.ReceiveWindow(); initial != 256<<10 || limit != 0 {
		t.Fatalf("receive window setting %d %d after auto-tuning, want 262144 0", initial, limit)
	}
}
