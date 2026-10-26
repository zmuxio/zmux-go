package zmux

import (
	"testing"

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
