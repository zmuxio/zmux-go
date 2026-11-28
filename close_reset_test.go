package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// A reset made just before a close reaches the peer: the close sends it
// first, and returns soon after it was sent, not at its bound.
func TestCloseSendsQueuedReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		cfg := &Config{ClockDomain: NewClockDomain(timeshim.Real)}
		c, s := pair(t, a, b, cfg, cfg)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte{7}); err != nil {
			t.Fatal(err)
		}
		r, err := s.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r.CancelWrite(9)
		start := time.Now()
		_ = s.CloseWithError(0, "")
		if d := time.Since(start); d != 0 {
			t.Fatalf("close took %v", d)
		}
		_, err = st.Read(make([]byte, 16))
		var se *StreamError
		if !errors.As(err, &se) || se.ErrorCode != 9 {
			t.Fatalf("read after the close: %v", err)
		}
	})
}
