package zmux

import (
	"errors"
	"math"
	"testing"
	"time"
)

// A negative DeadTimeout is off (-1 in the core) for a pipe and for the
// session, and survives Conn.Liveness; a positive one keeps its 1 s
// floor and any larger value, the largest included.
func TestDeadTimeoutOff(t *testing.T) {
	for _, d := range []time.Duration{-1, -time.Hour, math.MinInt64} {
		pl := PipeLiveness{DeadTimeout: d}
		cl, err := pl.core()
		if err != nil {
			t.Fatalf("pipe %v: %v", d, err)
		}
		if cl.DeadTimeout != -1 {
			t.Fatalf("pipe %v: %v", d, cl.DeadTimeout)
		}
		l := Liveness{DeadTimeout: d}
		sl, err := l.core()
		if err != nil {
			t.Fatalf("session %v: %v", d, err)
		}
		if sl.DeadTimeout != -1 {
			t.Fatalf("session %v: %v", d, sl.DeadTimeout)
		}
	}
	for _, d := range []time.Duration{time.Second, math.MaxInt64} {
		pl := PipeLiveness{DeadTimeout: d}
		cl, err := pl.core()
		if err != nil {
			t.Fatalf("pipe %v: %v", d, err)
		}
		if cl.DeadTimeout != int64(d) {
			t.Fatalf("pipe %v: %v", d, cl.DeadTimeout)
		}
	}
	pl := PipeLiveness{DeadTimeout: time.Second - 1}
	if _, err := pl.core(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("below 1 s: %v", err)
	}
}

// A stream carrier (StreamPipe) reports its own failure, and so do the
// views zmux puts over it: the replay of a prefix and Sealed records.
func TestStreamPipeReportsFailure(t *testing.T) {
	sp := streamPipe(&Stream{}, nil)
	if !carrierInfo(sp, tcpInfo{}, false).OwnFailure {
		t.Fatal("StreamPipe does not report its own failure")
	}
	if !reportsFailure(withPrefix(sp, []byte("x"))) {
		t.Fatal("the prefix replay hides it")
	}
	sealed := SealRecords(sp, nopSealer{}, nil, RecordSealed, nil)
	if !reportsFailure(sealed) {
		t.Fatal("Sealed records hide it")
	}
	if carrierInfo(PipeIO{R: &msgHalf{}, W: nopWriter{}}, tcpInfo{}, false).OwnFailure {
		t.Fatal("a plain carrier reports its own failure")
	}
}

// A split carrier reports its own failure only when every half does: a
// plain half beside a stream keeps stall-based failure, through the views.
func TestSplitCarrierReportsFailure(t *testing.T) {
	a, b := streamPipe(&Stream{}, nil), streamPipe(&Stream{}, nil)
	cases := []struct {
		name string
		p    PipeIO
		want bool
	}{
		{"plain writer", PipeIO{R: a.R, W: nopWriter{}}, false},
		{"plain reader", PipeIO{R: &msgHalf{}, W: a.W}, false},
		{"both report", PipeIO{R: a.R, W: b.W}, true},
		{"writer only", PipeIO{W: a.W}, true},
	}
	for _, c := range cases {
		views := map[string]PipeIO{
			"direct": c.p,
			"prefix": withPrefix(c.p, []byte("x")),
			"sealed": SealRecords(c.p, nopSealer{}, nil, RecordSealed, nil),
		}
		if c.p.R == nil {
			delete(views, "prefix") // a prefix is read data
		}
		for v, p := range views {
			if got := carrierInfo(p, tcpInfo{}, false).OwnFailure; got != c.want {
				t.Errorf("%s, %s: OwnFailure %v, want %v", c.name, v, got, c.want)
			}
		}
	}
}

type nopSealer struct{}

func (nopSealer) Overhead() int                      { return 16 }
func (nopSealer) Seal(dst, p []byte) ([]byte, error) { return append(dst, p...), nil }

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriter) Close() error                { return nil }
