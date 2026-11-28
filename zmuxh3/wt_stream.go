package zmuxh3

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

type wtReceive interface {
	io.Reader
	StreamID() quic.StreamID
	SetReadDeadline(time.Time) error
	CancelRead(quic.StreamErrorCode)
	ReleaseStreamCredit()
}

type wtSend interface {
	io.WriteCloser
	StreamID() quic.StreamID
	Context() context.Context
	SetWriteDeadline(time.Time) error
	SetReliableBoundary()
	CancelWrite(quic.StreamErrorCode)
}

const (
	wtReading uint32 = 1 << iota
	wtWriting
)

// Stream is a data stream of a WebTransport session. A unidirectional
// stream exposes only its receive or send half.
type Stream struct {
	s      *Session
	r      wtReceive
	w      wtSend
	halves atomic.Uint32
}

// trackStream is called with conn.mu held.
func (s *Session) trackStream(r wtReceive, w wtSend) *Stream {
	st := &Stream{s: s, r: r, w: w}
	var halves uint32
	if r != nil {
		halves |= wtReading
	}
	if w != nil {
		halves |= wtWriting
	}
	st.halves.Store(halves)
	s.streams[st] = struct{}{}
	return st
}

func (st *Stream) ended(half uint32) {
	if st.halves.And(^half)&^half == 0 {
		st.s.conn.mu.Lock()
		delete(st.s.streams, st)
		st.s.conn.mu.Unlock()
	}
}

func (st *Stream) reset(code quic.StreamErrorCode) {
	halves := st.halves.Swap(0)
	if halves&wtReading != 0 {
		st.r.CancelRead(code)
		st.r.ReleaseStreamCredit()
	}
	if halves&wtWriting != 0 {
		st.w.CancelWrite(code)
	}
}

// StreamID returns the data stream's QUIC stream ID.
func (st *Stream) StreamID() quic.StreamID {
	switch {
	case st == nil:
		return -1
	case st.w != nil:
		return st.w.StreamID()
	default:
		return st.r.StreamID()
	}
}

// Context ends when the send half closes or is canceled. For receive-only
// streams it is the session's context.
func (st *Stream) Context() context.Context {
	switch {
	case st == nil:
		return closedWTContext
	case st.w != nil:
		return st.w.Context()
	default:
		return st.s.Context()
	}
}

func (st *Stream) Read(p []byte) (int, error) {
	if st == nil || st.r == nil {
		return 0, net.ErrClosed
	}
	n, err := st.r.Read(p)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		st.ended(wtReading)
	}
	return n, wtStreamError(err)
}

func (st *Stream) Write(p []byte) (int, error) {
	if st == nil || st.w == nil {
		return 0, net.ErrClosed
	}
	n, err := st.w.Write(p)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		st.ended(wtWriting)
	}
	return n, wtStreamError(err)
}

// Close sends FIN on the write half of the stream.
func (st *Stream) Close() error {
	if st == nil || st.w == nil {
		return net.ErrClosed
	}
	err := st.w.Close()
	st.ended(wtWriting)
	return wtStreamError(err)
}

// CancelRead stops the peer's sending half with the given application code.
func (st *Stream) CancelRead(code StreamErrorCode) {
	if st != nil && st.r != nil {
		st.r.CancelRead(wtWireError(code))
		st.ended(wtReading)
	}
}

// CancelWrite resets the sending half with the given application code.
func (st *Stream) CancelWrite(code StreamErrorCode) {
	if st != nil && st.w != nil {
		st.w.CancelWrite(wtWireError(code))
		st.ended(wtWriting)
	}
}

// SetReadDeadline applies a deadline to subsequent and blocked reads.
func (st *Stream) SetReadDeadline(deadline time.Time) error {
	if st == nil || st.r == nil {
		return net.ErrClosed
	}
	return st.r.SetReadDeadline(deadline)
}

// SetWriteDeadline applies a deadline to subsequent and blocked writes.
func (st *Stream) SetWriteDeadline(deadline time.Time) error {
	if st == nil || st.w == nil {
		return net.ErrClosed
	}
	return st.w.SetWriteDeadline(deadline)
}

// SetDeadline applies a deadline to both halves of a bidirectional stream.
func (st *Stream) SetDeadline(deadline time.Time) error {
	return errors.Join(st.SetReadDeadline(deadline), st.SetWriteDeadline(deadline))
}
