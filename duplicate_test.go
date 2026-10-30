package zmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestResolveDuplicatePicksTheSameSurvivor(t *testing.T) {
	leakcheck.Check(t)
	// Endpoint A dials s1 to B; B dials s2 to A. A holds s1 (dialed
	// locally) and s2' (accepted); B holds s1' and s2.
	a1, b1 := tcpPair(t)
	s1, s1p := pair(t, a1, b1, nil, nil)
	b2, a2 := tcpPair(t)
	s2, s2p := pair(t, b2, a2, nil, nil)
	idA, idB := []byte("alpha"), []byte("bravo")
	ra, err := ResolveDuplicate(s1, s2p, &DuplicateOptions{LocalID: idA, PeerID: idB})
	if err != nil {
		t.Fatal(err)
	}
	rb, err := ResolveDuplicate(s1p, s2, &DuplicateOptions{LocalID: idB, PeerID: idA})
	if err != nil {
		t.Fatal(err)
	}
	// alpha < bravo: the session alpha dialed (s1) survives at both ends.
	if ra.Survivor != s1 || rb.Survivor != s1p {
		t.Fatalf("survivors differ: %p %p", ra.Survivor, rb.Survivor)
	}
	waitDone(t, s2)
	waitDone(t, s2p)
	if _, err := ResolveDuplicate(s1, s1, nil); err == nil {
		t.Fatal("same session resolved")
	}
}

func autoDuplicatePair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	a, b := tcpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type answer struct {
		conn *Conn
		err  error
	}
	done := make(chan answer, 1)
	go func() { c, err := Start(ctx, NetPipe(b), RoleAuto, nil); done <- answer{c, err} }()
	first, err := Start(ctx, NetPipe(a), RoleAuto, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := <-done
	if other.err != nil {
		t.Fatal(other.err)
	}
	first.DesignateDialer(false)
	other.conn.DesignateDialer(true)
	t.Cleanup(func() { _ = first.Close(); _ = other.conn.Close(); waitDone(t, first); waitDone(t, other.conn) })
	return first, other.conn
}

func TestResolveDuplicateRejectsDifferentNonceDomainsWithoutChangingSessions(t *testing.T) {
	leakcheck.Check(t)
	environment := newMPEnv(t, mpConfig(), mpConfig())
	multipath := environment.client
	auto, _ := autoDuplicatePair(t)
	if _, err := ResolveDuplicate(multipath, auto, nil); !errors.Is(err, ErrNoDecision) {
		t.Fatalf("MP explicit-role and non-MP auto have no common nonce domain: %v", err)
	}
	// A failed decision cannot start draining either session.
	for _, c := range []*Conn{multipath, auto} {
		stream, err := c.OpenStream()
		if err != nil {
			t.Fatalf("failed resolution changed session: %v", err)
		}
		stream.CancelWrite(0)
		stream.CancelRead(0)
	}
}

func TestResolveDuplicateCommitsDrainBeforeReturning(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	first, _ := pair(t, a, b, nil, nil)
	c, d := tcpPair(t)
	_, loser := pair(t, c, d, nil, nil)
	existing, err := loser.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	defer existing.CancelWrite(0)
	defer existing.CancelRead(0)
	result, err := ResolveDuplicate(first, loser, &DuplicateOptions{LocalID: []byte{1}, PeerID: []byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Loser != loser {
		t.Fatal("unexpected loser")
	}
	next, err := loser.OpenStream()
	if next != nil {
		next.CancelWrite(0)
		next.CancelRead(0)
	}
	if !errors.Is(err, ErrGoingAway) {
		t.Fatalf("new open after resolution: %v", err)
	}
}

func TestResolveDuplicateRejectsNilAndClosedSessions(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	first, peer := pair(t, a, b, nil, nil)
	if _, err := ResolveDuplicate(nil, first, nil); !errors.Is(err, ErrNoDecision) {
		t.Fatalf("nil input: %v", err)
	}
	if _, err := ResolveDuplicate(first, nil, nil); !errors.Is(err, ErrNoDecision) {
		t.Fatalf("nil input: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, first)
	if _, err := ResolveDuplicate(first, peer, &DuplicateOptions{LocalID: []byte{1}, PeerID: []byte{2}}); !errors.Is(err, ErrNoDecision) {
		t.Fatalf("closed input: %v", err)
	}
}
