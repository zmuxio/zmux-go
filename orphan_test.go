package zmux

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// A stream handle collected unclosed inside a synctest bubble is queued
// by the runtime's cleanup, which runs outside the bubble and touches no
// channel or timer of the session; the session releases it within
// orphanSweep without other activity, and the peer's stream ends.
func TestOrphanReleasedInBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cfg := func() *Config {
			return &Config{Clock: timeshim.Real, Liveness: Liveness{KeepAlivePeriod: -1}, MaxIdleTimeout: -1}
		}
		a, b := net.Pipe()
		ch := make(chan *Conn, 1)
		go func() {
			s, err := Server(ctx, b, cfg())
			if err != nil {
				s = nil
			}
			ch <- s
		}()
		client, err := Client(ctx, a, cfg())
		if err != nil {
			t.Fatal(err)
		}
		server := <-ch
		if server == nil {
			t.Fatal("no server session")
		}
		defer func() { _ = client.Close(); _ = server.Close(); _ = client.Wait(); _ = server.Wait() }()
		func() {
			st, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}()
		peer, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Only the atomic queue is looked at: taking the session lock would
		// release the orphan before the sweep.
		for i := 0; client.orphans.head.Load() == nil; i++ {
			if i == 1000 {
				t.Fatal("the handle was not collected")
			}
			runtime.GC()
			runtime.Gosched()
		}
		read := make(chan error, 1)
		go func() {
			buf := make([]byte, 16)
			for {
				if _, err := peer.Read(buf); err != nil {
					read <- err
					return
				}
			}
		}()
		time.Sleep(orphanSweep)
		synctest.Wait()
		client.lock()
		n := client.handles
		client.unlock()
		if n != 0 {
			t.Fatalf("%d handles unreleased after the sweep", n)
		}
		select {
		case err := <-read:
			var se *StreamError
			if !errors.As(err, &se) || se.ErrorCode != StreamCancelled {
				t.Fatalf("the read ended with %v, want a reset with StreamCancelled", err)
			}
		default:
			t.Fatal("the peer's stream did not end")
		}
	})
}
