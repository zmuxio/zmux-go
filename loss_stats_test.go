package zmux_test

import (
	"context"
	"io"
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
	// The data goes on the bootstrap pipe until it is suspect.
	if _, err := e.client.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{SendStandby: zmux.Set(true)}); err != nil {
		t.Fatal(err)
	}
	go echoAll(ctx, e.server)
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("traffic")); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	if got, err := io.ReadAll(st); err != nil || string(got) != "traffic" {
		t.Fatalf("echo %q, %v", got, err)
	}
	p, _ := e.client.Pipe(0)
	stats, ok := p.Stats()
	if !ok {
		t.Fatal("bootstrap pipe retired")
	}
	if stats.BytesLost != 0 || stats.RecordsLost != 0 {
		t.Fatalf("without loss: %d bytes, %d records lost", stats.BytesLost, stats.RecordsLost)
	}
	// A failed pipe retires at once here, so its failure figures are
	// checked in the core (TestStatsBytesLost).
}
