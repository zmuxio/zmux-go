package zmuxquic

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
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
	c, err := zmux.DialMode(ctx, zmux.AutoMode(first, Candidate(e.ln.Addr().String(), e.cli, nil)),
		cfg, &zmux.ModePolicy{NoJoin: true})
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
