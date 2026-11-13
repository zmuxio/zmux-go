package zmuxquic

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/carrier"
)

// TestCandidate dials in auto mode a candidate that does not answer, then
// a QUIC one that does: the first times out or fails, the second is chosen,
// and its connection closes with the session.
func TestCandidate(t *testing.T) {
	e := newEnv(t, mpConfig())
	dead, err := net.ListenPacket("udp", "127.0.0.1:0") // reads nothing
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dead.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := mpConfig()
	cfg.HandshakeTimeout = time.Second
	// A mode holds one candidate per kind: the dead one stands in for
	// another kind.
	first := Candidate(dead.LocalAddr().String(), e.cli, nil)
	first.Kind = zmux.CarrierWebTransport
	c, err := zmux.DialChoice(ctx, zmux.AutoChoice(first, Candidate(e.ln.Addr().String(), e.cli, nil)),
		cfg, &zmux.CarrierChoicePolicy{Join: zmux.SwitchOff})
	if err != nil {
		t.Fatal(err)
	}
	st := c.ConnectionStats()
	if st.Carrier != zmux.CarrierQUIC || len(st.CarrierCandidates) != 2 || st.CarrierCandidates[0].Outcome == zmux.CandidateUsable ||
		st.CarrierCandidates[1].Outcome != zmux.CandidateUsable {
		t.Fatalf("%v %+v", st.Carrier, st.CarrierCandidates)
	}
	s, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	echo(t, c, s, 64<<10)
	_ = c.Close()
	_ = s.Close()
	_ = c.Wait()
	_ = s.Wait()
}

// A joined QUIC stream declares the Bound contract: Bound keeps lanes, and Sealed has none.
func TestQUICJoinModes(t *testing.T) {
	for _, tc := range []struct {
		mode  zmux.RecordMode
		lanes bool
	}{{zmux.RecordBound, true}, {zmux.RecordSealed, false}} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			scfg := mpConfig()
			scfg.AllowRecordMode = func(_ zmux.RecordPolicyInput, m zmux.RecordMode, _ any) bool { return m == tc.mode }
			e := newEnv(t, scfg)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			p := NewPool(e.ln.Addr().String(), e.cli, nil, nil, carrier.PoolLimits{MaxPipesPerConnection: 1})
			defer p.Shutdown()
			first, err := p.OpenPipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, first, zmux.RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			s, err := e.lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = c.Close()
				_ = s.Close()
				_ = c.Wait()
				_ = s.Wait()
			}()
			second, err := p.OpenPipe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.AddPipe(ctx, second, zmux.PipeOptions{RecordMode: &tc.mode}); err != nil {
				t.Fatal(err)
			}
			// The traffic goes to the joined pipe, which then wants lanes of its own.
			if err := c.Pipes()[0].SetSendStandby(true); err != nil {
				t.Fatal(err)
			}
			echo(t, c, s, 1<<20)
			if n := len(c.PipeStats()); n != 2 {
				t.Fatalf("%d pipes", n)
			}
			lanes := func() (n int) {
				for _, ps := range c.PipeStats() {
					if ps.PipeID != 0 {
						n = max(n, ps.LanesOut)
					}
				}
				return n
			}
			if !tc.lanes && lanes() > 1 {
				t.Fatalf("a Sealed pipe has %d lanes", lanes())
			}
			for tc.lanes && lanes() <= 1 {
				if ctx.Err() != nil {
					t.Fatal("a Bound pipe opened no lane")
				}
				echo(t, c, s, 1<<20)
			}
		})
	}
}
