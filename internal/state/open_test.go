package state

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestInitialWindowsFollowRoleAndDirection(t *testing.T) {
	t.Parallel()
	settings := wire.Settings{
		InitialMaxStreamDataBidiLocallyOpened: 11,
		InitialMaxStreamDataBidiPeerOpened:    22,
		InitialMaxStreamDataUni:               33,
	}

	if got := InitialSendWindow(wire.RoleResponder, settings, 1); got != 22 {
		t.Fatalf("local-opened bidi send window = %d, want 22", got)
	}
	if got := InitialSendWindow(wire.RoleResponder, settings, 4); got != 11 {
		t.Fatalf("peer-opened bidi send window = %d, want 11", got)
	}
	if got := InitialSendWindow(wire.RoleResponder, settings, 2); got != 0 {
		t.Fatalf("peer-opened uni send window = %d, want 0", got)
	}
	if got := InitialSendWindow(wire.RoleResponder, settings, 3); got != 33 {
		t.Fatalf("local-opened uni send window = %d, want 33", got)
	}

	if got := InitialReceiveWindow(wire.RoleResponder, settings, 1); got != 11 {
		t.Fatalf("local-opened bidi recv window = %d, want 11", got)
	}
	if got := InitialReceiveWindow(wire.RoleResponder, settings, 4); got != 22 {
		t.Fatalf("peer-opened bidi recv window = %d, want 22", got)
	}
	if got := InitialReceiveWindow(wire.RoleResponder, settings, 2); got != 33 {
		t.Fatalf("peer-opened uni recv window = %d, want 33", got)
	}
	if got := InitialReceiveWindow(wire.RoleResponder, settings, 3); got != 0 {
		t.Fatalf("local-opened uni recv window = %d, want 0", got)
	}
}

func TestOpenAdmissionHelpers(t *testing.T) {
	t.Parallel()
	if !LocalOpenRefusedByGoAway(12, true, 8, 99) {
		t.Fatal("expected bidi local open above peer GOAWAY watermark to be refused")
	}
	if LocalOpenRefusedByGoAway(8, true, 8, 99) {
		t.Fatal("expected bidi local open at watermark to be allowed")
	}
	if got := ProjectedLocalOpenID(9, 3); got != 21 {
		t.Fatalf("projected local open id = %d, want 21", got)
	}
	if got := MaxStreamIDForClass(9); got != 9+((wire.MaxVarint62-9)/4)*4 {
		t.Fatalf("max stream id for class = %d, want %d", got, 9+((wire.MaxVarint62-9)/4)*4)
	}
	last := MaxStreamIDForClass(9)
	if got := ProjectedLocalOpenID(last, 1); got != wire.MaxVarint62+1 {
		t.Fatalf("projected local open id past class max = %d, want %d sentinel", got, wire.MaxVarint62+1)
	}
	if got := ProjectedLocalOpenID(last-4, 1); got != last {
		t.Fatalf("projected local open id at last valid slot = %d, want %d", got, last)
	}
	if got := ProvisionalAvailableCount(9, 21); got != 4 {
		t.Fatalf("available provisional count = %d, want 4", got)
	}
	if got := ProvisionalAvailableCount(25, 21); got != 0 {
		t.Fatalf("available provisional count above watermark = %d, want 0", got)
	}
	if got := AdmissionSoftCap(128); got != 32 {
		t.Fatalf("admission soft cap = %d, want 32", got)
	}
	if got := AdmissionHardCap(128); got != 64 {
		t.Fatalf("admission hard cap = %d, want 64", got)
	}
	if got := ProvisionalSoftCap(true, 8); got != 16 {
		t.Fatalf("provisional soft cap = %d, want 16 minimum", got)
	}
	if got := ProvisionalHardCap(true, 8); got != 32 {
		t.Fatalf("provisional hard cap = %d, want 32 minimum", got)
	}
}

func TestPeerOpenAndActiveCountHelpers(t *testing.T) {
	t.Parallel()
	if !PeerOpenRefusedByGoAway(13, 12, 99) {
		t.Fatal("expected peer bidi stream above local GOAWAY watermark to be refused")
	}
	if got := ExpectedNextPeerStreamID(3, 5, 3); got != 3 {
		t.Fatalf("expected next peer uni id = %d, want 3", got)
	}
	if got := ExpectedNextPeerStreamID(4, 4, 3); got != 4 {
		t.Fatalf("expected next peer bidi id = %d, want 4", got)
	}
	if !PeerStreamWithinLimit(true, 1, 0, 2, 9) {
		t.Fatal("expected bidi peer stream under limit to be allowed")
	}
	if PeerStreamWithinLimit(false, 0, 2, 9, 2) {
		t.Fatal("expected uni peer stream at limit to be refused")
	}
	bidi, uni := DecrementActivePeerCount(true, 1, 7)
	if bidi != 0 || uni != 7 {
		t.Fatalf("decrement active bidi = (%d,%d), want (0,7)", bidi, uni)
	}
}

func TestProvisionalExpired(t *testing.T) {
	t.Parallel()
	now := time.Now()
	if ProvisionalExpired(true, now.Add(-time.Hour), now) {
		t.Fatal("committed stream should not be provisional-expired")
	}
	if ProvisionalExpired(false, time.Time{}, now) {
		t.Fatal("zero creation time should not be provisional-expired")
	}
	if !ProvisionalExpired(false, now.Add(-ProvisionalOpenMaxAge-time.Millisecond), now) {
		t.Fatal("expected old provisional stream to expire")
	}
}
