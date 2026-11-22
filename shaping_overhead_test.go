package zmux

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// PipeIO.WriteOverhead reaches the ShaperFactory, and the first record of
// each carrier write is marked in the record views.
func TestShapingWriteOverhead(t *testing.T) {
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv := make(chan *Conn, 1)
	go func() {
		s, err := Start(ctx, NetPipe(b), RoleServer, nil)
		if err != nil {
			t.Error(err)
		}
		srv <- s
	}()
	p := NetPipe(a)
	p.WriteOverhead = 22
	c, err := Start(ctx, p, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := <-srv
	if s == nil {
		t.FailNow()
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	var made, viewed atomic.Int64
	made.Store(-1)
	viewed.Store(-1)
	c.SetShaper(func(sp ShapedPipe) shaping.Shaper {
		made.Store(int64(sp.WriteOverhead))
		return &fnShaper{caps: shaping.CapPlan, plan: func(v *shaping.RecordView, _ *shaping.RecordPlan) {
			if v.WriteStart {
				viewed.Store(1)
			}
		}}
	}, nil)
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if x, err := s.AcceptStream(ctx); err == nil {
			_, _ = x.Read(make([]byte, 16))
		}
	}()
	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	for made.Load() < 0 || viewed.Load() < 0 {
		if ctx.Err() != nil {
			t.Fatal("no record planned")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if made.Load() != 22 {
		t.Fatalf("ShapedPipe.WriteOverhead %d, want 22", made.Load())
	}
	for _, o := range []int{-1, MaxWriteOverhead + 1} {
		q := NetPipe(&net.TCPConn{})
		q.WriteOverhead = o
		if _, err := c.AddPipe(ctx, q, PipeOptions{}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("AddPipe with WriteOverhead %d: %v", o, err)
		}
		if _, err := Start(ctx, q, RoleClient, nil); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Start with WriteOverhead %d: %v", o, err)
		}
	}
}

// The listener keeps the WriteOverhead that WrapPipe sets, and counts a
// WrapPipe result outside the range as a setup failure.
func TestShapingWriteOverheadWrapPipe(t *testing.T) {
	for _, tc := range []struct {
		over int
		ok   bool
	}{{22, true}, {MaxWriteOverhead + 1, false}} {
		var got atomic.Int64
		got.Store(-1)
		l := mustNewListener(&ListenerConfig{WrapPipe: WrapPipeFunc(func(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
			p.WriteOverhead = tc.over
			return p, nil
		})})
		a, b := net.Pipe()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		herr := make(chan error, 1)
		go func() { herr <- l.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
		c, err := Client(ctx, a, nil)
		if !tc.ok {
			if err == nil {
				_ = c.Close()
				t.Fatal("session over a WrapPipe result outside the range")
			}
			if herr := <-herr; herr == nil {
				t.Fatal("HandlePipe accepted a WrapPipe result outside the range")
			}
			if n := l.Stats().SetupFailures; n != 1 {
				t.Fatalf("%d setup failures, want 1", n)
			}
			cancel()
			_ = l.Close()
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		s, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.SetShaper(func(sp ShapedPipe) shaping.Shaper {
			got.Store(int64(sp.WriteOverhead))
			return &fnShaper{}
		}, nil)
		st, err := s.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = st.Write([]byte("x"))
		for got.Load() < 0 && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
		}
		if got.Load() != 22 {
			t.Fatalf("server ShapedPipe.WriteOverhead %d, want 22", got.Load())
		}
		_ = c.Close()
		_ = s.Close()
		waitDone(t, c)
		waitDone(t, s)
		cancel()
		_ = l.Close()
		if herr := <-herr; herr != nil {
			t.Fatalf("HandlePipe: %v", herr)
		}
	}
}
