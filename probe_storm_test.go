package zmux

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"
	"github.com/zmuxio/zmux-go/shaping"
)

// probeTraffic returns the PIPE_PING frames and records both ends of e
// sent and the client's smoothed RTT.
func probeTraffic(e *mpEnv) (pings, recs uint64, srtt time.Duration) {
	for _, c := range []*Conn{e.client, e.server} {
		for _, st := range c.PipeStats() {
			pings += st.PingsSent
			recs += st.RecordsSent
			if c == e.client {
				srtt = st.SmoothedRTT
			}
		}
	}
	return
}

// pingStormConfig is an idle session whose client probes every 15 s and
// whose server probes at an hour, so that the client's probes are the only
// exchanges it starts: each is answered by the server's PIPE_ACK with at
// most one PIPE_PING of its own, at most 8 PIPE_PING frames a minute.
func pingStormConfig(c *Config) {
	c.MaxIdleTimeout = -1
	c.Liveness.KeepAlivePeriod = -1
	c.Liveness.ProbeInterval = 15 * time.Second
	c.Liveness.ProbeJitter = -1
}

const pingStormLimit = 10 // per minute: 8 and the window's edges

// Both ends shape every record with a 2 s delay. A record that carries a
// PIPE_ACK goes no later than zmux_max_ack_delay after the acknowledgement
// became due, so the peer's working pipe never turns suspect. Held for the
// whole delay, every exchange made the pipe suspect, the suspicion's
// PIPE_PING was answered as late, and the pair kept exchanging PIPE_PING
// about once a second.
func TestShapingDelayKeepsAcknowledgementsInTime(t *testing.T) {
	t.Parallel()
	clk, cc, sc := fakeConfigs(pingStormConfig)
	sc.Liveness.ProbeInterval = time.Hour
	e := newMPEnv(t, cc, sc)
	var plans atomic.Int64
	shp := func(ShapedPipe) shaping.Shaper { return delayCounter{2 * time.Second, &plans} }
	e.client.SetShaper(shp, nil)
	e.server.SetShaper(shp, nil)
	const inc = 20 * time.Millisecond
	step(clk, 20*time.Second, inc)
	p0, _, _ := probeTraffic(e)
	step(clk, time.Minute, inc)
	p1, _, _ := probeTraffic(e)
	if n := p1 - p0; n > pingStormLimit {
		t.Fatalf("%d PIPE_PING in a minute of idle probing", n)
	}
	for _, c := range []*Conn{e.client, e.server} {
		for _, st := range c.PipeStats() {
			if st.Outbound != OutboundActive {
				t.Fatalf("pipe %d %v under a 2 s shaping delay", st.PipeID, st.Outbound)
			}
		}
	}
}

// A path with a 4 s round trip, above the T_suspect of the initial
// estimate: every probe turns the pipe suspect before its answer. The
// PIPE_PING of the suspicion gives a sample even though the pipe became
// suspect again before its acknowledgement, so the estimate learns the
// path and the suspicions stop. Discarded, every sample of the pipe was of
// bytes sent before its latest suspicion, the estimate stayed at 333 ms
// and the pipe was suspect at every probe.
func TestLongRoundTripLearnedDespiteSuspicion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		pingStormConfig(cc)
		pingStormConfig(sc)
		sc.Liveness.ProbeInterval = time.Hour
		ln := netx.NewChanListener(nil, 0)
		k := link{2 * time.Second, 1e9, 1 << 30}
		// The first record arrives a round trip after the carrier: above
		// the 2 s default first-record timeout.
		e := startMPEnv(t, cc, &ListenerConfig{Config: sc, InstanceID: []byte("instance"), FirstRecordTimeout: 10 * time.Second},
			&linkListener{Listener: ln, links: []link{k}},
			func(ctx context.Context) (net.Conn, error) {
				c, err := ln.Dial(ctx, nil)
				if err != nil {
					return c, err
				}
				return newDelayConn(c, k.d, k.rate, k.cap), nil
			})
		time.Sleep(30 * time.Second)
		p0, _, _ := probeTraffic(e)
		time.Sleep(time.Minute)
		p1, _, srtt := probeTraffic(e)
		if srtt < 3*time.Second {
			t.Fatalf("smoothed RTT %v on a 4 s path", srtt)
		}
		if n := p1 - p0; n > pingStormLimit {
			t.Fatalf("%d PIPE_PING in a minute of idle probing", n)
		}
	})
}
