package zmux

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestMaintainPipesRedials(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- e.client.MaintainPipes(ctx, PipePool{Count: 2, MinBackoff: 10 * time.Millisecond,
			Dial: func(ctx context.Context) (PipeIO, error) {
				nc, err := net.Dial("tcp", e.ln.Addr().String())
				if err != nil {
					return PipeIO{}, err
				}
				return NetPipe(nc), nil
			}})
	}()
	count := func() int {
		n := 0
		for _, p := range e.client.Pipes() {
			if p.ID != 0 && p.State == "active" {
				n++
			}
		}
		return n
	}
	wait := func(want int) {
		deadline := time.Now().Add(5 * time.Second)
		for count() != want {
			if time.Now().After(deadline) {
				t.Fatalf("%d pool pipes, want %d", count(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(2)
	for _, p := range e.client.Pipes() {
		if p.ID != 0 {
			pp, _ := e.client.Pipe(p.ID)
			pp.Abort(nil)
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	wait(2)
	cancel()
	<-done
}
