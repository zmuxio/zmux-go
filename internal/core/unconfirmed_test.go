package core

import (
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

func TestUnconfirmedEvictsAtBound(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SessionPolicy.Limits.PendingPeerJoins = 2
	s := New(cfg, 0)
	pjs := []*PeerJoin{{owner: s, Mode: join.Trusted}, {owner: s, Mode: join.Trusted}, {owner: s, Mode: join.Trusted}}
	for i, pj := range pjs[:2] {
		if got := s.holdPeerJoin(pj, i); len(got) != 0 {
			t.Fatalf("join %d evicted below the bound", i)
		}
	}
	if got := s.holdPeerJoin(pjs[2], 2); !slices.Equal(got, []any{0}) {
		t.Fatalf("at the bound: evicted %v, want the oldest, 0", got)
	}
	if s.DropPeerJoin(pjs[0]) {
		t.Fatal("an evicted join claimed confirmation")
	}
	if !s.DropPeerJoin(pjs[1]) {
		t.Fatal("registered join lost its confirmation")
	}
	if got := s.holdPeerJoin(&PeerJoin{owner: s, Mode: join.Trusted}, 3); len(got) != 0 {
		t.Fatal("evicted after a confirmed join left room")
	}
	bound := &PeerJoin{owner: s, Mode: join.Bound}
	if got := s.holdPeerJoin(bound, 4); len(got) != 0 || s.PendingPeerJoins() != 2 {
		t.Fatalf("a Bound join took a place: %d held", s.PendingPeerJoins())
	}
	if !s.DropPeerJoin(bound) {
		t.Fatal("Bound confirmation required a slot")
	}
}
