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
		if err != nil || cl.DeadTimeout != -1 {
			t.Fatalf("pipe %v: %v, %v", d, cl.DeadTimeout, err)
		}
		l := Liveness{DeadTimeout: d}
		sl, err := l.core()
		if err != nil || sl.DeadTimeout != -1 {
			t.Fatalf("session %v: %v, %v", d, sl.DeadTimeout, err)
		}
	}
	for _, d := range []time.Duration{time.Second, math.MaxInt64} {
		pl := PipeLiveness{DeadTimeout: d}
		if cl, err := pl.core(); err != nil || cl.DeadTimeout != int64(d) {
			t.Fatalf("pipe %v: %v, %v", d, cl.DeadTimeout, err)
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

type nopSealer struct{}

func (nopSealer) Overhead() int                      { return 16 }
func (nopSealer) Seal(dst, p []byte) ([]byte, error) { return append(dst, p...), nil }

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriter) Close() error                { return nil }
