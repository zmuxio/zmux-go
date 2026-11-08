package zmux

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// firstDelay delays the first record of each pipe it shapes (or every
// record with all).
type firstDelay struct {
	delay time.Duration
	all   bool
	n     int
}

func (d *firstDelay) Capabilities() shaping.Caps { return shaping.CapPlan }
func (d *firstDelay) Plan(_ *shaping.RecordView, p *shaping.RecordPlan) {
	if d.n == 0 || d.all {
		p.Delay = d.delay
	}
	d.n++
}
func (d *firstDelay) OnEvent(*shaping.Event, *shaping.Effects) {}

// A shaping delay on the first record of the bootstrap pipe, installed
// from the session's creation, holds the transport parameters, which
// still lead the pipe once it ends: the peer accepts the session.
func TestShapingDelayFirstRecordKeepsParametersFirst(t *testing.T) {
	for _, all := range []bool{false, true} {
		a, b := tcpPair(t)
		f := func(ShapedPipe) shaping.Shaper { return &firstDelay{delay: 20 * time.Millisecond, all: all} }
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		type res struct {
			c   *Conn
			err error
		}
		sc := make(chan res, 1)
		go func() {
			c, err := Server(ctx, b, &Config{Shaper: f})
			sc <- res{c, err}
		}()
		c, err := Client(ctx, a, &Config{Shaper: f})
		if err != nil {
			t.Fatalf("all=%v: client: %v", all, err)
		}
		s := <-sc
		if s.err != nil {
			t.Fatalf("all=%v: server: %v", all, s.err)
		}
		go func() {
			st, err := s.c.AcceptStream(ctx)
			if err == nil {
				_, _ = io.Copy(st, st)
				_ = st.Close()
			}
		}()
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = st.Write([]byte("hello"))
		_ = st.Close()
		got, err := io.ReadAll(st)
		if err != nil || string(got) != "hello" {
			t.Fatalf("all=%v: echo %q %v", all, got, err)
		}
		_ = c.Close()
		_ = s.c.Close()
		waitDone(t, c)
		waitDone(t, s.c)
		cancel()
	}
}

// A session closed while its first record waits for a shaping delay sends
// the parameters and CONNECTION_CLOSE at once, together: the peer sees
// the application's error code, not a peer that is not zmux.
func TestShapingDelayedFirstRecordCloseAtOnce(t *testing.T) {
	a, b := tcpPair(t)
	f := func(ShapedPipe) shaping.Shaper { return &firstDelay{delay: time.Hour} }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The server learns the close while it establishes, or after.
	sc := make(chan error, 1)
	go func() {
		s, err := Server(ctx, b, nil)
		if err == nil {
			err = s.Wait()
		}
		sc <- err
	}()
	c, err := Client(ctx, a, &Config{Shaper: f})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWithError(0x77, "bye"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-sc:
		var ae *ApplicationError
		if !errors.As(err, &ae) || !ae.Remote || ae.ErrorCode != 0x77 {
			t.Fatalf("peer ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer saw no close")
	}
	waitDone(t, c)
}
