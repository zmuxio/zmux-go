package zmuxh3

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zmuxio/zmux-go/internal/labels"
)

type wtControl interface {
	io.ReadWriteCloser
	StreamID() quic.StreamID
	Context() context.Context
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SendDatagram([]byte) error
	ReceiveDatagram(context.Context) ([]byte, error)
}

// Session carries a group of zmux pipes and lanes over one WebTransport
// Extended CONNECT. Its context's cause reports how the session ended.
type Session struct {
	conn             *wtConn
	control          wtControl
	id               quic.StreamID
	ctx              context.Context
	cancel           context.CancelCauseFunc
	controlDone      chan struct{}
	draining         chan struct{}
	drainOnce        sync.Once
	state            quic.ConnectionState
	datagramOverhead int

	// conn.mu protects the queues and prevents new streams from being
	// attached after shutdown starts.
	streams                   map[*Stream]struct{}
	incomingBidi, incomingUni []*Stream
	changed                   chan struct{}
}

var closedWTContext = func() context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(net.ErrClosed)
	return ctx
}()

// Context ends when the CONNECT stream or the underlying connection ends.
func (s *Session) Context() context.Context {
	if s == nil {
		return closedWTContext
	}
	return s.ctx
}

// RemoteAddr returns the remote QUIC address.
func (s *Session) RemoteAddr() net.Addr {
	if s == nil {
		return nil
	}
	return s.conn.qc.RemoteAddr()
}

// Draining is closed when the peer requests a graceful session shutdown.
// Existing pipes can finish and the session remains usable until it closes.
func (s *Session) Draining() <-chan struct{} {
	if s == nil {
		return closedWTContext.Done()
	}
	return s.draining
}

func (s *Session) start() {
	stop := context.AfterFunc(s.control.Context(), func() {
		reset, ok := errors.AsType[*quic.StreamError](context.Cause(s.control.Context()))
		if !ok || !reset.Remote {
			return
		}
		if reset.ErrorCode == wtSessionGone {
			// STOP_SENDING can arrive ahead of the peer's close capsule.
			if s.ctx.Err() == nil {
				_ = s.control.SetReadDeadline(time.Now().Add(s.conn.timeout))
			}
		} else if s.finish(&http3.Error{ErrorCode: http3.ErrCode(reset.ErrorCode), Remote: true}) {
			s.control.CancelRead(wtSessionGone)
		}
	})
	labels.Go(s.ctx, "wt-capsules", func() {
		defer stop()
		s.readCapsules()
	})
}

func (s *Session) readCapsules() {
	defer close(s.controlDone)
	for {
		kind, closed, err := readWTCapsule(s.control)
		switch {
		case errors.Is(err, io.EOF):
			if s.finish(&SessionError{Remote: true}) {
				_ = s.control.Close()
			}
			return
		case err != nil:
			code := wtSessionGone
			malformed := errors.Is(err, errWTCapsule)
			if malformed {
				code = quic.StreamErrorCode(http3.ErrCodeMessageError)
			}
			if s.finish(err) || malformed {
				s.control.CancelWrite(code)
			}
			s.control.CancelRead(code)
			return
		case kind == wtClose:
			if s.finish(closed) {
				_ = s.control.Close()
			}
			// A close capsule must be the last CONNECT payload. Bound the
			// wait for its accompanying FIN while permitting a reordered FIN.
			var extra [1]byte
			_ = s.control.SetReadDeadline(time.Now().Add(s.conn.timeout))
			if n, _ := io.ReadFull(s.control, extra[:]); n != 0 {
				code := quic.StreamErrorCode(http3.ErrCodeMessageError)
				s.control.CancelRead(code)
				s.control.CancelWrite(code)
			} else {
				s.control.CancelRead(wtSessionGone)
			}
			return
		case kind == wtDrain:
			s.drainOnce.Do(func() { close(s.draining) })
		}
	}
}

func (s *Session) finish(err error) bool {
	s.conn.mu.Lock()
	if s.streams == nil {
		s.conn.mu.Unlock()
		return false
	}
	streams := s.streams
	s.streams = nil
	s.incomingBidi, s.incomingUni = nil, nil
	s.cancel(err)
	s.conn.mu.Unlock()
	for stream := range streams {
		stream.reset(wtSessionGone)
	}
	return true
}

// CloseWithError ends this session and aborts its data streams. It sends the
// application code and UTF-8 reason on CONNECT, followed by FIN.
func (s *Session) CloseWithError(code SessionErrorCode, reason string) error {
	if s == nil {
		return net.ErrClosed
	}
	reason = wtReason(reason)
	if !s.finish(&SessionError{ErrorCode: code, Message: reason}) {
		return nil
	}
	deadline := time.Now().Add(lingerTimeout(s.conn.qc.ConnectionStats().SmoothedRTT))
	_ = s.control.SetWriteDeadline(deadline)
	_ = s.control.SetReadDeadline(deadline)
	if _, err := s.control.Write(wtAppendClose(nil, code, reason)); err != nil {
		wtCancelControl(s.control)
		return err
	}
	return s.control.Close()
}

// SendDatagram sends an unreliable payload on the CONNECT stream's HTTP
// Datagram channel. HTTP/3 adds the RFC 9297 Quarter Stream ID.
func (s *Session) SendDatagram(p []byte) error {
	if err := context.Cause(s.Context()); err != nil {
		return err
	}
	// Skip HTTP/3's framing allocation for DatagramMTU's oversized probe.
	var err error
	if len(p) >= 1<<16 {
		err = s.conn.qc.SendDatagram(p)
	} else {
		err = s.control.SendDatagram(p)
	}
	if big, ok := errors.AsType[*quic.DatagramTooLargeError](err); ok {
		return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: max(0, big.MaxDatagramPayloadSize-int64(s.datagramOverhead))}
	}
	return err
}

// ReceiveDatagram waits for an HTTP Datagram and returns its payload.
func (s *Session) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if err := context.Cause(s.Context()); err != nil {
		return nil, err
	}
	if ctx != s.ctx {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		defer cancel()
	}
	p, err := s.control.ReceiveDatagram(ctx)
	if cause := context.Cause(s.ctx); cause != nil {
		return nil, cause
	}
	return p, err
}

func (s *Session) wakeAccept() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// AcceptStream waits for a peer-initiated bidirectional stream.
func (s *Session) AcceptStream(ctx context.Context) (*Stream, error) {
	return s.accept(ctx, false)
}

// AcceptUniStream waits for a peer-initiated receive-only stream.
func (s *Session) AcceptUniStream(ctx context.Context) (*Stream, error) {
	return s.accept(ctx, true)
}

func (s *Session) accept(ctx context.Context, uni bool) (*Stream, error) {
	if s == nil {
		return nil, net.ErrClosed
	}
	for {
		s.conn.mu.Lock()
		err := context.Cause(s.ctx)
		if err == nil {
			err = context.Cause(ctx)
		}
		if err != nil {
			s.conn.mu.Unlock()
			return nil, err
		}
		queue := &s.incomingBidi
		if uni {
			queue = &s.incomingUni
		}
		if len(*queue) != 0 {
			st := (*queue)[0]
			(*queue)[0] = nil
			*queue = (*queue)[1:]
			s.conn.mu.Unlock()
			st.r.ReleaseStreamCredit()
			return st, nil
		}
		wait := s.changed
		s.conn.mu.Unlock()
		select {
		case <-wait:
		case <-s.ctx.Done():
		case <-ctx.Done():
		}
	}
}

// OpenStreamSync opens a bidirectional stream, waiting for QUIC stream credit.
func (s *Session) OpenStreamSync(ctx context.Context) (*Stream, error) {
	return s.open(ctx, false)
}

// OpenUniStreamSync opens a send-only stream, waiting for QUIC stream credit.
func (s *Session) OpenUniStreamSync(ctx context.Context) (*Stream, error) {
	return s.open(ctx, true)
}

func (s *Session) open(ctx context.Context, uni bool) (*Stream, error) {
	if err := context.Cause(s.Context()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	defer stop()

	var r wtReceive
	var w wtSend
	kind := uint64(wtUni)
	if uni {
		send, err := s.conn.qc.OpenUniStreamSync(ctx)
		if err != nil {
			return nil, err
		}
		w = send
	} else {
		bidi, err := s.conn.qc.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
		r, w = bidi, bidi
		kind = wtBidi
	}

	var prefix [16]byte
	header := quicvarint.Append(prefix[:0], kind)
	header = quicvarint.Append(header, uint64(s.id))
	interrupt := context.AfterFunc(ctx, func() { w.CancelWrite(wtWireError(carrierFailed)) })
	_, err := w.Write(header)
	if err == nil && s.state.SupportsStreamResetPartialDelivery.Local && s.state.SupportsStreamResetPartialDelivery.Remote {
		w.SetReliableBoundary()
	}
	interrupt()

	s.conn.mu.Lock()
	if err == nil {
		err = context.Cause(s.ctx)
	}
	if err == nil {
		err = context.Cause(ctx)
	}
	if err == nil {
		st := s.trackStream(r, w)
		s.conn.mu.Unlock()
		return st, nil
	}
	s.conn.mu.Unlock()
	code := wtWireError(carrierFailed)
	if s.ctx.Err() != nil {
		code = wtSessionGone
	}
	if r != nil {
		r.CancelRead(code)
	}
	w.CancelWrite(code)
	return nil, err
}
