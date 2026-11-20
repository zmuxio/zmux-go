package zmuxquic

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// A connection whose first stream falls back reaches ListenerConfig.Fallback
// with its bytes replayed; TakeoverConn gives its QUIC connection, and the
// connection is closed once the fallback returned.
func TestServeTakeover(t *testing.T) {
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddr("127.0.0.1:0", srv, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	type seen struct {
		read []byte
		conn bool
	}
	got := make(chan seen, 1)
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(),
		Admission: &zmux.Admission{Peek: 4, PeekTimeout: 5 * time.Second,
			Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
				return zmux.Decision{Verdict: zmux.VerdictFallback}
			}},
		Fallback: func(_ context.Context, tk *zmux.Takeover) {
			qc, ok := TakeoverConn(tk)
			b := make([]byte, 6)
			_, _ = io.ReadFull(tk.Pipe().R, b)
			got <- seen{read: b, conn: ok && qc != nil}
		}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = Serve(ctx, lst, ln); close(done) }()
	defer func() { cancel(); _ = ln.Close(); _ = lst.Close(); <-done }()

	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	s, err := qc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	select {
	case x := <-got:
		if string(x.read) != "abcdef" || !x.conn {
			t.Fatalf("takeover %+v", x)
		}
	case <-ctx.Done():
		t.Fatal("no takeover")
	}
	select {
	case <-qc.Context().Done():
	case <-ctx.Done():
		t.Fatal("the connection outlived its takeover")
	}
	if st := lst.Stats().Admission; st != (zmux.AdmissionStats{Fallback: 1}) {
		t.Fatalf("stats %+v", st)
	}
}
