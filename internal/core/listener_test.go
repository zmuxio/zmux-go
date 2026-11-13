package core

import "testing"

// A carrier that gives up withdraws only itself; one a partner already
// took stays the session's.
func TestSplitPairingWithdraw(t *testing.T) {
	var sp SplitPairing
	a, b, c := [16]byte{1}, [16]byte{2}, "x"
	if r, _ := sp.Offer(a, true, "A-up"); r != PairWait {
		t.Fatal(r)
	}
	if r, _ := sp.Offer(b, true, "B-up"); r != PairWait {
		t.Fatal(r)
	}
	if !sp.Withdraw(a, "A-up") {
		t.Fatal("own carrier not withdrawn")
	}
	if r, got := sp.Offer(b, false, "B-down"); r != PairDone || got != "B-up" {
		t.Fatalf("the other pairing was disturbed: %v %v", r, got)
	}
	if sp.Withdraw(b, "B-up") {
		t.Fatal("a paired carrier was withdrawn")
	}
	if r, _ := sp.Offer(b, true, c); r != PairReject {
		t.Fatal("a used token paired again")
	}
	sp.Forget(b)
	if len(sp.used) != 0 || len(sp.wait) != 0 {
		t.Fatalf("state %v %v", sp.used, sp.wait)
	}
}

// Pending-carrier limits: IPv6 sources count per /64; a full listener
// with none stalled refuses a new carrier, a full source refuses.
func TestPendingCarriers(t *testing.T) {
	for in, want := range map[string]string{
		"2001:db8:1:2::1":      "2001:db8:1:2::/64",
		"2001:db8:1:2:ffff::9": "2001:db8:1:2::/64",
		"fe80::1%eth0":         "fe80::/64",
		"::ffff:192.0.2.1":     "192.0.2.1",
		"192.0.2.1":            "192.0.2.1",
		"front-end 7":          "front-end 7",
	} {
		if got := SourceKey(in); got != want {
			t.Errorf("SourceKey(%q) = %q, want %q", in, got, want)
		}
	}
	p := PendingCarriers{MaxTotal: 4, MaxPerSource: 2}
	enter := func(src string) (*PendingSlot, EnterResult) {
		sl, ev, r := p.Enter(src)
		if ev != nil {
			t.Fatal("an Enter evicted without a stalled carrier")
		}
		return sl, r
	}
	a, _ := enter("x")
	enter("x")
	if _, r := enter("x"); r != EnterSourceFull {
		t.Fatalf("a third carrier of a full source: %v", r)
	}
	enter("y")
	d, _ := enter("z")
	if _, r := enter("w"); r != EnterFull || p.Total() != 4 {
		t.Fatalf("a carrier beyond a full total with none stalled: %v, total %d", r, p.Total())
	}
	p.Leave(a)
	p.Leave(a) // twice: no effect
	if p.Total() != 3 || p.Held("x") != 1 {
		t.Fatalf("Leave: total %d, x holds %d", p.Total(), p.Held("x"))
	}
	if _, r := enter("w"); r != EnterOK {
		t.Fatalf("after Leave: %v", r)
	}
	p.Leave(d)
	unlimited := PendingCarriers{MaxTotal: -1, MaxPerSource: 1 << 20}
	for i := range 5000 {
		if _, _, r := unlimited.Enter("x"); r != EnterOK {
			t.Fatalf("unlimited total refused carrier %d: %v", i, r)
		}
	}
}

// TestPendingCarriersStall checks that stalled carriers keep their slots,
// that a source holds at most MaxPerSource - 1 of them and the listener
// half of MaxTotal, and that a new carrier at a full total evicts the
// oldest stalled one.
func TestPendingCarriersStall(t *testing.T) {
	p := PendingCarriers{MaxTotal: 6, MaxPerSource: 3}
	var evicted []int
	stall := func(src string, n int) (*PendingSlot, bool) {
		sl, _, r := p.Enter(src)
		if r != EnterOK {
			t.Fatalf("Enter %d: %v", n, r)
		}
		return sl, p.Stall(sl, func() { evicted = append(evicted, n) })
	}
	x1, ok1 := stall("x", 1)
	_, ok2 := stall("x", 2)
	x3, ok3 := stall("x", 3)
	if !ok1 || !ok2 || ok3 {
		t.Fatalf("stalls of a source with a limit of 3: %v %v %v", ok1, ok2, ok3)
	}
	p.Leave(x3)
	_, ok4 := stall("y", 4)
	_, ok5 := stall("z", 5)
	if !ok4 || ok5 {
		t.Fatalf("stalls up to half of the total: %v %v", ok4, ok5)
	}
	// Total: x1 x2 y4 (stalled) and z5: 4 of 6.
	p.Enter("a")
	p.Enter("b")
	sl, ev, r := p.Enter("c")
	if r != EnterOK || sl == nil || ev == nil {
		t.Fatalf("a full total with stalled carriers: %v", r)
	}
	ev()
	if len(evicted) != 1 || evicted[0] != 1 || p.Held("x") != 1 || p.Total() != 6 {
		t.Fatalf("evicted %v, x holds %d, total %d", evicted, p.Held("x"), p.Total())
	}
	p.Leave(x1) // evicted already: no effect
	if p.Total() != 6 {
		t.Fatalf("Leave of an evicted slot changed the total to %d", p.Total())
	}
	one := PendingCarriers{MaxPerSource: 1}
	s1, _, _ := one.Enter("x")
	if one.Stall(s1, nil) {
		t.Fatal("a source with a limit of 1 stalled a carrier")
	}
}
