package zmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// receiveWindowPart opens a stream and returns the server's receiving side of
// it, on a session whose configured window cap is fixed at 64 KiB.
func receiveWindowPart(t *testing.T) *Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	scfg := mpConfig()
	scfg.InitialStreamReceiveWindow = 32 << 10
	scfg.MaxStreamReceiveWindow = 64 << 10
	e := newMPEnv(t, mpConfig(), scfg)
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	as, err := e.server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return as
}

func wantReceiveWindow(t *testing.T, when string, as *Stream, initial, limit uint64) {
	t.Helper()
	if in, lim := as.ReceiveWindow(); in != initial || lim != limit {
		t.Fatalf("%s: receive window reads %d %d, want %d %d", when, in, lim, initial, limit)
	}
}

// A limit of 0 makes the cap automatic, whatever the session's cap is and
// whatever the stream's limit was before; it never keeps the previous one.
func TestSetReceiveWindowZeroLimitIsAutomatic(t *testing.T) {
	leakcheck.Check(t)
	as := receiveWindowPart(t)
	wantReceiveWindow(t, "session's", as, 32<<10, 64<<10)
	if err := as.SetReceiveWindow(48<<10, 128<<10); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "fixed", as, 48<<10, 128<<10)
	if err := as.SetReceiveWindow(48<<10, 0); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "automatic after a fixed cap", as, 48<<10, 0)
	if err := as.SetReceiveWindow(64<<10, 96<<10); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "fixed again", as, 64<<10, 96<<10)
}

// A nonzero limit below initial is an invalid argument and changes nothing.
func TestSetReceiveWindowLimitBelowInitialIsInvalid(t *testing.T) {
	leakcheck.Check(t)
	as := receiveWindowPart(t)
	if err := as.SetReceiveWindow(48<<10, 96<<10); err != nil {
		t.Fatal(err)
	}
	before := as.Stats().RecvWindow
	for _, c := range [][2]uint64{{128 << 10, 64 << 10}, {1 << 20, 1}} {
		if err := as.SetReceiveWindow(c[0], c[1]); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("SetReceiveWindow(%d, %d) = %v, want ErrInvalidConfig", c[0], c[1], err)
		}
	}
	wantReceiveWindow(t, "after the rejected calls", as, 48<<10, 96<<10)
	if w := as.Stats().RecvWindow; w != before {
		t.Fatalf("a rejected call moved the window from %d to %d", before, w)
	}
	// Equal values are valid.
	if err := as.SetReceiveWindow(64<<10, 64<<10); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "equal", as, 64<<10, 64<<10)
}

// Credit already advertised is never withdrawn: an initial above the window in
// force raises it, one below changes only what the getter reads.
func TestSetReceiveWindowNeverWithdrawsCredit(t *testing.T) {
	leakcheck.Check(t)
	as := receiveWindowPart(t)
	if err := as.SetReceiveWindow(1<<20, 0); err != nil {
		t.Fatal(err)
	}
	if w := as.Stats().RecvWindow; w != 1<<20 {
		t.Fatalf("a higher initial left the window at %d", w)
	}
	if err := as.SetReceiveWindow(32<<10, 64<<10); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "lowered", as, 32<<10, 64<<10)
	if w := as.Stats().RecvWindow; w != 1<<20 {
		t.Fatalf("a lower initial withdrew credit: window %d", w)
	}
}

// An initial below 4 KiB (the floor of the configured initial window) is an
// invalid argument that changes nothing; 4096 is accepted.
func TestSetReceiveWindowInitialBelowTheFloorIsInvalid(t *testing.T) {
	leakcheck.Check(t)
	as := receiveWindowPart(t)
	if err := as.SetReceiveWindow(48<<10, 96<<10); err != nil {
		t.Fatal(err)
	}
	for _, initial := range []uint64{0, 1, 4095} {
		if err := as.SetReceiveWindow(initial, 0); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("SetReceiveWindow(%d, 0) = %v, want ErrInvalidConfig", initial, err)
		}
	}
	wantReceiveWindow(t, "after the rejected calls", as, 48<<10, 96<<10)
	if err := as.SetReceiveWindow(4096, 0); err != nil {
		t.Fatal(err)
	}
	wantReceiveWindow(t, "the floor", as, 4096, 0)
}
