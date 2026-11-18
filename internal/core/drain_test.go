package core

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// drainSession is a session whose only pipe neither carries
// CONNECTION_CLOSE nor reads, with peer bidirectional streams 1 (accepted)
// and 5 (offered, not accepted) holding data.
func drainSession(t *testing.T, accept bool) (*Session, *Stream) {
	t.Helper()
	s := capacitySession()
	for _, id := range []uint64{1, 5} {
		data := []byte("unread")
		if _, err := s.onStream(&wire.Frame{Kind: wire.KindStream, StreamID: id, Data: data}, &Buf{B: data}); err != nil {
			t.Fatal(err)
		}
	}
	var st *Stream
	if accept {
		var ok bool
		if st, ok = s.AcceptStream(true); !ok {
			t.Fatal("no stream to accept")
		}
	}
	p := s.pipes[0]
	p.in, p.out = nil, nil
	return s, st
}

// After the peer's CONNECTION_CLOSE, CLOSED waits for the application to
// read what an accepted stream holds, also without a pipe to send
// CONNECTION_CLOSE on; a stream not yet accepted is discarded and accept
// fails; the read ends the wait.
func TestReadDrainAfterPeerClose(t *testing.T) {
	s, st := drainSession(t, true)
	s.onClose(&wire.Frame{App: true, Code: 0x2a})
	if s.state != StateClosing || s.drain.left != 1 {
		t.Fatalf("state %v, %d streams to read", s.state, s.drain.left)
	}
	if _, ok := s.AcceptStream(true); ok {
		t.Fatal("accept took a stream after the close")
	}
	buf := make([]byte, 16)
	if n, _, _ := s.Read(st, buf); n != 6 {
		t.Fatalf("read %d bytes", n)
	}
	if s.state != StateClosed {
		t.Fatalf("state %v after the last read", s.state)
	}
}

// The wait ends at the linger timeout without a read.
func TestReadDrainLinger(t *testing.T) {
	s, _ := drainSession(t, true)
	s.onClose(&wire.Frame{App: true, Code: 0x2a})
	s.HandleTimeout(s.now.Add(s.lingerTimeout()))
	if s.state != StateClosed {
		t.Fatalf("state %v after the linger timeout", s.state)
	}
}

// Streams offered but not accepted are not waited for.
func TestReadDrainOnlyAccepted(t *testing.T) {
	s, _ := drainSession(t, false)
	s.onClose(&wire.Frame{App: true, Code: 0x2a})
	if s.state != StateClosed {
		t.Fatalf("state %v with only unaccepted streams", s.state)
	}
}

// The peer's idle close without CONNECTION_CLOSE does not wait for reads.
func TestReadDrainNotOnIdleClose(t *testing.T) {
	s, _ := drainSession(t, true)
	s.closeSilently(&SessionError{Kind: CloseIdle, Remote: true}, true)
	s.checkLingerDone()
	if s.state != StateClosed {
		t.Fatalf("state %v after the peer's idle close", s.state)
	}
}

// drainEnded is a session whose accepted peer stream 1 received only its
// end, a FIN or a reset, and no byte; the pipe carries nothing.
func drainEnded(t *testing.T, reset bool) (*Session, *Stream) {
	t.Helper()
	s := capacitySession()
	if reset {
		if _, err := s.onStream(&wire.Frame{Kind: wire.KindStream, StreamID: 1, Data: []byte("x")}, &Buf{B: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	} else if _, err := s.onStream(&wire.Frame{Kind: wire.KindStream, StreamID: 1, Fin: true}, nil); err != nil {
		t.Fatal(err)
	}
	st, ok := s.AcceptStream(true)
	if !ok {
		t.Fatal("no stream to accept")
	}
	if reset {
		buf := make([]byte, 1)
		if n, _, _ := s.Read(st, buf); n != 1 {
			t.Fatalf("read %d bytes", n)
		}
		if _, e := s.onReset(&wire.Frame{Kind: wire.KindResetStream, StreamID: 1, Code: 9, FinalSize: 1}, false); e != nil {
			t.Fatal(e)
		}
	}
	p := s.pipes[0]
	p.in, p.out = nil, nil
	return s, st
}

// The read drain keeps a stream's end, not only its bytes (SPEC.md Section
// 6.5): a FIN or a reset received before the peer's close holds CLOSED
// until the application reads it, and the read reports it, not the
// session's error; the linger timeout still bounds the wait.
func TestReadDrainKeepsReceivedEnd(t *testing.T) {
	for _, reset := range []bool{false, true} {
		s, st := drainEnded(t, reset)
		s.onClose(&wire.Frame{App: true, Code: 0x2a})
		if s.state != StateClosing || s.drain.left != 1 {
			t.Fatalf("reset=%v: state %v, %d streams to read", reset, s.state, s.drain.left)
		}
		n, fin, err := s.Read(st, make([]byte, 16))
		var se *StreamError
		switch {
		case reset && (n != 0 || fin || !errors.As(err, &se) || se.Kind != StreamReset || se.Code != 9):
			t.Fatalf("reset: read = %d, %v, %v", n, fin, err)
		case !reset && (n != 0 || !fin || err != nil):
			t.Fatalf("fin: read = %d, %v, %v", n, fin, err)
		}
		if s.state != StateClosed {
			t.Fatalf("reset=%v: state %v after the end was read", reset, s.state)
		}
	}
}

// An unread end waits no longer than the linger timeout.
func TestReadDrainEndLinger(t *testing.T) {
	for _, reset := range []bool{false, true} {
		s, _ := drainEnded(t, reset)
		s.onClose(&wire.Frame{App: true, Code: 0x2a})
		s.HandleTimeout(s.now.Add(s.lingerTimeout()))
		if s.state != StateClosed {
			t.Fatalf("reset=%v: state %v after the linger timeout", reset, s.state)
		}
	}
}
