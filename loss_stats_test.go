package zmux_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

// A busy pipe that loses nothing reports no loss.
func TestPipeStatsLoss(t *testing.T) {
	t.Parallel()
	e := newRelayEnv(t, relayConfig(0), relayConfig(0), zmuxtest.RelayOptions{}, true)
	nc, err := net.Dial("tcp", e.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sb := true // the data goes on the bootstrap pipe until it is suspect
	if _, err := e.client.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{SendStandby: &sb}); err != nil {
		t.Fatal(err)
	}
	busy(t, e)
	time.Sleep(300 * time.Millisecond)
	p, _ := e.client.Pipe(0)
	st, _ := p.Stats()
	if st.BytesLost != 0 || st.RecordsLost != 0 {
		t.Fatalf("without loss: %d bytes, %d records lost", st.BytesLost, st.RecordsLost)
	}
	// A failed pipe retires at once here, so its failure figures are
	// checked in the core (TestStatsBytesLost).
	if st.BytesRetransmitted != 0 || st.LossRate != 0 {
		t.Fatalf("loopback carrier: %d retransmitted, rate %v", st.BytesRetransmitted, st.LossRate)
	}
}
