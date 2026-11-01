package zmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

func TestPreambleTooLarge(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	c, s := pair(t, a, b, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tl *PreambleTooLargeError
	if err := st.SetPreamble(make([]byte, 5000)); !errors.As(err, &tl) || tl.Max != 4096 {
		t.Fatalf("SetPreamble = %v, want max 4096", err)
	}
	if err := st.AddPreambleTLV(0x1000, make([]byte, 5000)); !errors.As(err, &tl) {
		t.Fatalf("AddPreambleTLV = %v", err)
	}
	if err := st.SetPreamble([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	in, err := s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(in.Preamble()) != "hello" {
		t.Fatalf("preamble %q", in.Preamble())
	}
}
