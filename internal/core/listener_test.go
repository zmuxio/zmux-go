package core

import "testing"

// A carrier that gives up withdraws only itself; one a partner already
// took stays the session's.
func TestSplitPairingWithdraw(t *testing.T) {
	var sp SplitPairing
	a, b, c := [16]byte{1}, [16]byte{2}, "x"
	if r, _ := sp.Offer(a, true, "s1", "A-up"); r != PairWait {
		t.Fatal(r)
	}
	if r, _ := sp.Offer(b, true, "s2", "B-up"); r != PairWait {
		t.Fatal(r)
	}
	if !sp.Withdraw(a, "A-up") {
		t.Fatal("own carrier not withdrawn")
	}
	if r, got := sp.Offer(b, false, "s2", "B-down"); r != PairDone || got != "B-up" {
		t.Fatalf("the other pairing was disturbed: %v %v", r, got)
	}
	if sp.Withdraw(b, "B-up") {
		t.Fatal("a paired carrier was withdrawn")
	}
	if r, _ := sp.Offer(b, true, "s3", c); r != PairReject {
		t.Fatal("a used token paired again")
	}
	sp.Forget(b)
	if len(sp.used) != 0 || len(sp.wait) != 0 || len(sp.bySource) != 0 {
		t.Fatalf("state %v %v %v", sp.used, sp.wait, sp.bySource)
	}
}

// Pending-carrier limits: IPv6 sources count per /64; a full listener evicts its
// oldest pending carrier, a full source refuses.
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
	p := PendingCarriers{MaxTotal: 2, MaxPerSource: 2}
	var closed []string
	admit := func(src, name string) (*PendingCarrier, bool) {
		c, ev, ok := p.Admit(src, func() { closed = append(closed, name) })
		if ev != nil {
			ev()
		}
		return c, ok
	}
	a, _ := admit("x", "a")
	admit("x", "b")
	if _, ok := admit("x", "c"); ok {
		t.Fatal("a third carrier of a full source was admitted")
	}
	if _, ok := admit("y", "d"); !ok || len(closed) != 1 || closed[0] != "a" {
		t.Fatalf("full listener: admitted %v, closed %v", ok, closed)
	}
	p.Release(a) // evicted already: no effect
	if _, ok := admit("x", "e"); !ok || len(closed) != 2 || closed[1] != "b" {
		t.Fatalf("after eviction: admitted %v, closed %v", ok, closed)
	}
}

// TestPendingCarriersStall checks that a source holds at most
// MaxPerSource - 1 stalled carriers, and that they take no slot of the
// total.
func TestPendingCarriersStall(t *testing.T) {
	p := PendingCarriers{MaxTotal: 1, MaxPerSource: 3}
	for i, want := range []bool{true, true, false} {
		if got := p.Stall("x"); got != want {
			t.Fatalf("stall %d of a source with a limit of 3: %v", i+1, got)
		}
	}
	evicted := false
	if _, ev, ok := p.Admit("y", func() { evicted = true }); !ok || ev != nil {
		t.Fatal("a stalled carrier took a slot of the total")
	}
	p.Unstall("x")
	if !p.Stall("x") {
		t.Fatal("Unstall did not give the place back")
	}
	if evicted {
		t.Fatal("evicted")
	}
	one := PendingCarriers{MaxPerSource: 1}
	if one.Stall("x") {
		t.Fatal("a source with a limit of 1 stalled a carrier")
	}
}
