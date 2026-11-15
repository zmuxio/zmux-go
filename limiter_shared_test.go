package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// poolLimiter hands out bytes from a pool that refill tops up; it fails
// the test if the pool ever goes negative or more is refunded than taken.
// It counts the Acquire calls in progress.
type poolLimiter struct {
	t         *testing.T
	more      chan struct{}
	pool      int64
	burst     int64
	netOut    int64 // taken and not refunded
	acquiring int   // Acquire calls waiting
	mu        sync.Mutex
}

func newPoolLimiter(t *testing.T, burst int64) *poolLimiter {
	t.Helper()
	return &poolLimiter{t: t, burst: burst, pool: burst, more: make(chan struct{})}
}

// take grants within the contract: at least min (or the burst, if less),
// as much as the pool has up to max.
func (l *poolLimiter) take(minBytes, maxBytes int64) int64 {
	if maxBytes <= 0 {
		return 0
	}
	minBytes = min(max(minBytes, 1), maxBytes, l.burst)
	if l.pool < minBytes {
		return 0
	}
	n := min(maxBytes, l.pool)
	l.pool -= n
	l.netOut += n
	return n
}

func (l *poolLimiter) Acquire(ctx context.Context, minBytes, maxBytes int64) (int64, error) {
	l.mu.Lock()
	l.acquiring++
	defer func() {
		l.mu.Lock()
		l.acquiring--
		l.mu.Unlock()
	}()
	for {
		more := l.more
		n := l.take(minBytes, maxBytes)
		l.mu.Unlock()
		if n > 0 || maxBytes <= 0 {
			return n, nil
		}
		select {
		case <-more:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		l.mu.Lock()
	}
}

func (l *poolLimiter) TryAcquire(minBytes, maxBytes int64) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := l.take(minBytes, maxBytes); n > 0 {
		return n, nil
	}
	return 0, errors.New("would wait")
}

func (l *poolLimiter) Refund(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n > l.netOut {
		l.t.Errorf("refunded %d with %d taken", n, l.netOut)
	}
	l.netOut -= n
	l.pool = min(l.pool+n, l.burst)
	close(l.more)
	l.more = make(chan struct{})
}

// refill restores the pool to its burst.
func (l *poolLimiter) refill() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pool = l.burst
	close(l.more)
	l.more = make(chan struct{})
}

// TestSpliceSharedLimiterNeverOverGrants: two splices share one limiter;
// what they take and keep is exactly what they forward, and the pool,
// taken when handed out, never goes below zero however they interleave.
func TestSpliceSharedLimiterNeverOverGrants(t *testing.T) {
	client, rin, rout, server := relayEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lim := newPoolLimiter(t, 8<<10)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				lim.refill()
			}
		}
	}()
	const streams = 2
	results := make(chan SpliceResult, streams)
	go func() {
		for range streams {
			in, err := rin.AcceptStream(ctx)
			if err != nil {
				return
			}
			out, err := rout.OpenStreamSync(ctx)
			if err != nil {
				return
			}
			go func() {
				r, _ := Splice(ctx, in, out, &SpliceOptions{AtoB: lim, BtoA: lim})
				results <- r
			}()
		}
	}()
	go func() {
		for {
			s, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(s, s); _ = s.Close() }()
		}
	}()
	msg := make([]byte, 256<<10)
	_, _ = rand.Read(msg)
	var wg sync.WaitGroup
	for range streams {
		s, err := client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			go func() { _, _ = s.Write(msg); _ = s.Close() }()
			got, err := io.ReadAll(s)
			if err != nil || !bytes.Equal(got, msg) {
				t.Errorf("echo: %d bytes, %v", len(got), err)
			}
		})
	}
	wg.Wait()
	var forwarded uint64
	for range streams {
		r := <-results
		forwarded += r.AtoB.Bytes + r.BtoA.Bytes
	}
	lim.mu.Lock()
	defer lim.mu.Unlock()
	if uint64(lim.netOut) != forwarded || forwarded != streams*2*uint64(len(msg)) {
		t.Fatalf("kept %d, forwarded %d", lim.netOut, forwarded)
	}
}

// badLimiter returns a fixed grant, or panics.
type badLimiter struct {
	grant    int64
	panics   bool
	refunded int64
}

func (l *badLimiter) Acquire(context.Context, int64, int64) (int64, error) {
	if l.panics {
		panic("acquire")
	}
	return l.grant, nil
}

func (l *badLimiter) TryAcquire(int64, int64) (int64, error) {
	if l.panics {
		panic("try")
	}
	return l.grant, nil
}

func (l *badLimiter) Refund(n int64) {
	if l.panics {
		panic("refund")
	}
	l.refunded += n
}

// TestGuardedLimiterFailures: an invalid grant or a panic fails Acquire,
// counts as nothing in TryAcquire (a positive invalid grant is refunded),
// and a panic in Refund is ignored.
func TestGuardedLimiterFailures(t *testing.T) {
	ctx := context.Background()
	for _, grant := range []int64{100, 0, -5} {
		o := &badLimiter{grant: grant}
		g := guardedLimiter{o}
		if n, err := g.Acquire(ctx, 1, 30); n != 0 || !errors.Is(err, errLimiterFailed) {
			t.Fatalf("Acquire grant %d: %d %v", grant, n, err)
		}
		// 0 is a refusal, not a failure.
		if n, err := g.TryAcquire(1, 30); n != 0 || grant != 0 && !errors.Is(err, errLimiterFailed) {
			t.Fatalf("TryAcquire grant %d: %d %v", grant, n, err)
		}
		if want := 2 * max(grant, 0); o.refunded != want {
			t.Fatalf("grant %d: refunded %d, want %d", grant, o.refunded, want)
		}
	}
	p := &badLimiter{grant: 10, panics: true}
	g := guardedLimiter{p}
	if n, err := g.Acquire(ctx, 1, 30); n != 0 || !errors.Is(err, errLimiterFailed) {
		t.Fatalf("panicking Acquire: %d %v", n, err)
	}
	if n, _ := g.TryAcquire(1, 30); n != 0 {
		t.Fatalf("panicking TryAcquire: %d", n)
	}
	g.Refund(5)
}

// TestLimiterContract: the arguments as the contract reads them (max <= 0
// takes nothing, min < 1 is 1, min > max is max), (n, n) all or nothing,
// a grant of a burst below min, and a cancelled Acquire leaves nothing.
func TestLimiterContract(t *testing.T) {
	ctx := context.Background()
	p := newPoolLimiter(t, 10)
	g := guardedLimiter{p}
	if n, err := g.Acquire(ctx, 5, 0); n != 0 || err != nil || p.netOut != 0 {
		t.Fatalf("max 0: %d %v", n, err)
	}
	if n, err := g.TryAcquire(1, -1); n != 0 || err != nil || p.netOut != 0 {
		t.Fatalf("max < 0: %d %v", n, err)
	}
	if n, err := g.Acquire(ctx, -3, 4); n != 4 || err != nil {
		t.Fatalf("min < 1: %d %v", n, err)
	}
	if n, err := g.TryAcquire(9, 3); n != 3 || err != nil {
		t.Fatalf("min > max: %d %v", n, err)
	}
	// 3 left: (4, 4) all or nothing refuses, (3, 3) passes.
	if n, ok := g.police(4); ok || n != 0 || p.netOut != 7 {
		t.Fatalf("police over the pool: %d %v, kept %d", n, ok, p.netOut)
	}
	if n, ok := g.police(3); !ok || n != 3 || p.netOut != 10 {
		t.Fatalf("police within the pool: %d %v", n, ok)
	}
	if n, ok := g.police(0); !ok || n != 0 {
		t.Fatal("an empty datagram was refused")
	}
	g.Refund(10)
	// A unit above the burst passes on the burst (no debt here).
	if n, ok := g.police(25); !ok || n != 10 {
		t.Fatalf("police above the burst: %d %v", n, ok)
	}
	g.Refund(10)
	p.mu.Lock()
	p.pool = 0
	p.mu.Unlock()
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if n, err := g.Acquire(cctx, 1, 5); n != 0 || !errors.Is(err, context.DeadlineExceeded) || p.netOut != 0 {
		t.Fatalf("cancelled Acquire: %d %v, kept %d", n, err, p.netOut)
	}
}

// TestSpliceHoldsNoGrantWhileIdle: a direction whose source has nothing
// to read waits without a grant and without an Acquire in progress.
func TestSpliceHoldsNoGrantWhileIdle(t *testing.T) {
	client, rin, rout, server := relayEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lim := newPoolLimiter(t, 1<<20)
	done := make(chan SpliceResult, 1)
	go func() {
		in, err := rin.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := rout.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		r, _ := Splice(ctx, in, out, &SpliceOptions{AtoB: lim, BtoA: lim})
		done <- r
	}()
	s, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	srv, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(srv, buf); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // both directions idle
	lim.mu.Lock()
	out, acq := lim.netOut, lim.acquiring
	lim.mu.Unlock()
	if out != 5 || acq != 0 {
		t.Fatalf("idle splice keeps %d (forwarded 5), %d Acquire calls waiting", out, acq)
	}
	_ = s.Close()
	_ = srv.Close()
	select {
	case r := <-done:
		if r.AtoB.Bytes != 5 {
			t.Fatalf("result %+v", r)
		}
	case <-ctx.Done():
		t.Fatal("splice did not end")
	}
}
