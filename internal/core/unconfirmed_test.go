package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

func TestUnconfirmedEvictsAtBound(t *testing.T) {
	var u Unconfirmed[int]
	pjs := []*PeerJoin{{Mode: join.Trusted}, {Mode: join.Trusted}, {Mode: join.Trusted}}
	for i, pj := range pjs[:2] {
		if _, ok := u.Hold(pj, i, 2); ok {
			t.Fatalf("join %d evicted below the bound", i)
		}
	}
	if old, ok := u.Hold(pjs[2], 2, 2); !ok || old != 0 {
		t.Fatalf("at the bound: evicted %d, %v; want the oldest, 0", old, ok)
	}
	if u.Confirm(pjs[0]) {
		t.Fatal("an evicted join claimed confirmation")
	}
	if !u.Confirm(pjs[1]) {
		t.Fatal("registered join lost its confirmation")
	}
	if _, ok := u.Hold(&PeerJoin{Mode: join.Trusted}, 3, 2); ok {
		t.Fatal("evicted after a confirmed join left room")
	}
	bound := &PeerJoin{Mode: join.Bound}
	if !u.Confirm(bound) {
		t.Fatal("Bound confirmation required a slot")
	}
	if _, ok := u.Hold(bound, 4, 2); ok || u.Len() != 2 {
		t.Fatalf("a Bound join took a place: %d held", u.Len())
	}
}
