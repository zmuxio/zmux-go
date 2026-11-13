package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// A WrapPipe error, or one that outlasts the handshake timeout, closes the
// carrier and counts a setup failure.
func TestWrapPipeFailure(t *testing.T) {
	leakcheck.Check(t)
	for _, mode := range []string{"error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			cfg := mpConfig()
			cfg.HandshakeTimeout = 200 * time.Millisecond
			wrap := func(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
				if mode == "error" {
					return PipeIO{}, errors.New("refused")
				}
				var b [1]byte
				_, err := p.R.Read(b[:]) // ignores ctx: ends when p is closed
				return PipeIO{}, err
			}
			lst := mustNewListener(&ListenerConfig{Config: cfg, WrapPipe: WrapPipeFunc(wrap)})
			defer func() { _ = lst.Close() }()
			a, b := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- lst.HandlePipe(context.Background(), NetPipe(a), PipeHint{}) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("HandlePipe succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("WrapPipe not bounded")
			}
			var x [1]byte
			if _, err := b.Read(x[:]); err == nil {
				t.Fatal("carrier not closed")
			}
			_ = b.Close()
			if st := lst.Stats(); st.SetupFailures != 1 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// DialChoice refuses a mode with two candidates of one kind.
func TestDialModeDuplicateKind(t *testing.T) {
	dial := func(context.Context, CarrierCredential) (PipeIO, error) { return PipeIO{}, errors.New("unused") }
	m := AutoChoice(Candidate{Kind: CarrierStream, Dial: dial}, Candidate{Kind: CarrierExchange, Dial: dial},
		Candidate{Kind: CarrierStream, Dial: dial})
	if _, err := DialChoice(context.Background(), m, nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err %v, want ErrInvalidConfig", err)
	}
}
