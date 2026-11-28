package zmuxh3

import (
	"errors"
	"net"
	"slices"
	"testing"
	"time"
)

func TestWebTransportSendHalfBatchesAndEndsOnce(t *testing.T) {
	s := &recordedWTWriter{}
	w := &wtWriter[*recordedWTWriter]{s: s}
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteBuffers([][]byte{[]byte("sec"), []byte("ond")}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(s.writes, []string{"first", "second"}) {
		t.Fatalf("stream writes %q", s.writes)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w.Abort(nil)
	if s.closed != 1 {
		t.Fatalf("stream ended %d times", s.closed)
	}
	if _, err := w.Write([]byte("closed")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close returned %v", err)
	}
}

type recordedWTWriter struct {
	writes []string
	closed int
}

func (s *recordedWTWriter) Write(p []byte) (int, error) {
	s.writes = append(s.writes, string(p))
	return len(p), nil
}

func (s *recordedWTWriter) Close() error {
	s.closed++
	return nil
}

func (s *recordedWTWriter) CancelWrite(StreamErrorCode)      { s.closed++ }
func (s *recordedWTWriter) SetWriteDeadline(time.Time) error { return nil }
