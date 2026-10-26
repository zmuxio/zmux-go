package core

import "testing"

// A carrier that gives up withdraws only itself; one a partner already
// took stays the session's.
func TestSplitPairingWithdraw(t *testing.T) {
	var sp SplitPairing
	a, b, c := [16]byte{1}, [16]byte{2}, "x"
	if r, _ := sp.Offer(a, true, "s1", "A-up", 0); r != PairWait {
		t.Fatal(r)
	}
	if r, _ := sp.Offer(b, true, "s2", "B-up", 0); r != PairWait {
		t.Fatal(r)
	}
	if !sp.Withdraw(a, "A-up") {
		t.Fatal("own carrier not withdrawn")
	}
	if r, got := sp.Offer(b, false, "s2", "B-down", 0); r != PairDone || got != "B-up" {
		t.Fatalf("the other pairing was disturbed: %v %v", r, got)
	}
	if sp.Withdraw(b, "B-up") {
		t.Fatal("a paired carrier was withdrawn")
	}
	if r, _ := sp.Offer(b, true, "s3", c, 0); r != PairReject {
		t.Fatal("a used token paired again")
	}
	sp.Forget(b)
	if len(sp.used) != 0 || len(sp.wait) != 0 || len(sp.bySource) != 0 {
		t.Fatalf("state %v %v %v", sp.used, sp.wait, sp.bySource)
	}
}
