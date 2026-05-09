package quicmux

import (
	"context"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

type bidiAcceptResult struct {
	stream zmux.Stream
	err    error
}

type uniAcceptResult struct {
	stream zmux.RecvStream
	err    error
}

type quicAcceptLoopResult[T any] interface {
	acceptResult() (T, error)
}

func (r bidiAcceptResult) acceptResult() (zmux.Stream, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.stream, nil
}

func (r uniAcceptResult) acceptResult() (zmux.RecvStream, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.stream, nil
}

func acceptStreamFromLoop[T any, R quicAcceptLoopResult[T]](ctx context.Context, conn SessionConn, ch <-chan R) (T, error) {
	var zero T
	ctx = defaultContext(ctx)
	connCtx := conn.Context()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-connCtx.Done():
		if err := translateWaitError(context.Cause(connCtx)); err != nil {
			return zero, err
		}
		return zero, zmux.ErrSessionClosed
	case result := <-ch:
		return result.acceptResult()
	}
}

func (s *quicSession) ensureBidiAcceptLoop() <-chan bidiAcceptResult {
	if s == nil || s.conn == nil {
		return nil
	}
	s.bidiOnce.Do(func() {
		s.bidiCh = make(chan bidiAcceptResult, acceptedPreludeResultQueueCap)
		s.startInternal(func() {
			s.acceptBidiLoop()
		})
	})
	return s.bidiCh
}

func (s *quicSession) ensureUniAcceptLoop() <-chan uniAcceptResult {
	if s == nil || s.conn == nil {
		return nil
	}
	s.uniOnce.Do(func() {
		s.uniCh = make(chan uniAcceptResult, acceptedPreludeResultQueueCap)
		s.startInternal(func() {
			s.acceptUniLoop()
		})
	})
	return s.uniCh
}

func (s *quicSession) acceptBidiLoop() {
	runAcceptedStreamLoop(s, s.conn.AcceptStream, publishBidiAcceptError, discardAcceptedRawBidiStream, (*quicSession).prepareAcceptedBidiStream)
}

func (s *quicSession) acceptUniLoop() {
	runAcceptedStreamLoop(s, s.conn.AcceptUniStream, publishUniAcceptError, discardAcceptedRawUniStream, (*quicSession).prepareAcceptedUniStream)
}

func runAcceptedStreamLoop[Raw any](
	s *quicSession,
	accept func(context.Context) (Raw, error),
	publishErr func(*quicSession, error),
	discard func(Raw),
	prepare func(*quicSession, Raw),
) {
	for {
		stream, err := accept(context.Background())
		if err != nil {
			publishErr(s, translateError(err))
			return
		}
		if !s.acquirePrepareSlot() {
			discard(stream)
			return
		}
		if !s.startInternal(func() {
			defer s.releasePrepareSlot()
			prepare(s, stream)
		}) {
			s.releasePrepareSlot()
			discard(stream)
		}
	}
}

func (s *quicSession) startInternal(fn func()) bool {
	if s == nil || s.conn == nil || fn == nil {
		return false
	}
	s.internalMu.Lock()
	if s.conn.Context().Err() != nil {
		s.internalMu.Unlock()
		return false
	}
	if s.internalActive == 0 {
		s.internalDone = make(chan struct{})
	}
	s.internalActive++
	s.internalMu.Unlock()
	go func() {
		defer s.finishInternal()
		fn()
	}()
	return true
}

func (s *quicSession) finishInternal() {
	if s == nil {
		return
	}
	s.internalMu.Lock()
	if s.internalActive > 0 {
		s.internalActive--
	}
	if s.internalActive == 0 && s.internalDone != nil {
		close(s.internalDone)
		s.internalDone = nil
	}
	s.internalMu.Unlock()
}

func (s *quicSession) waitInternal(ctx context.Context) error {
	s.internalMu.Lock()
	done := s.internalDone
	s.internalMu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (s *quicSession) acquirePrepareSlot() bool {
	if s == nil || s.conn == nil || s.prepareSem == nil {
		return false
	}
	select {
	case s.prepareSem <- struct{}{}:
		return true
	case <-s.conn.Context().Done():
		return false
	}
}

func (s *quicSession) releasePrepareSlot() {
	if s == nil || s.prepareSem == nil {
		return
	}
	select {
	case <-s.prepareSem:
	default:
	}
}

type acceptedBidiDiscarder interface {
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	Close() error
}

type acceptedUniDiscarder interface {
	CancelRead(quic.StreamErrorCode)
}

func discardAcceptedBidiStream(stream acceptedBidiDiscarder) {
	if stream == nil {
		return
	}
	code := quic.StreamErrorCode(zmux.CodeCancelled)
	stream.CancelRead(code)
	stream.CancelWrite(code)
	_ = stream.Close()
}

func discardAcceptedUniStream(stream acceptedUniDiscarder) {
	if stream == nil {
		return
	}
	stream.CancelRead(quic.StreamErrorCode(zmux.CodeCancelled))
}

func discardAcceptedRawBidiStream(stream *quic.Stream) {
	discardAcceptedBidiStream(stream)
}

func discardAcceptedRawUniStream(stream *quic.ReceiveStream) {
	discardAcceptedUniStream(stream)
}

type acceptedPreparedStream interface {
	activate(*quicActiveStreamCounters, quicActiveStreamKind)
	CloseWithError(uint64, string) error
}

func prepareAcceptedStream[Raw any, Wrapped acceptedPreparedStream](
	s *quicSession,
	stream Raw,
	wrap func(SessionConn, Raw, time.Duration) (Wrapped, error),
	kind quicActiveStreamKind,
	publish func(*quicSession, Wrapped) bool,
) {
	wrapped, err := wrap(s.conn, stream, s.acceptedPreludeReadTimeout)
	if err != nil {
		return
	}
	wrapped.activate(&s.active, kind)
	if !publish(s, wrapped) {
		_ = wrapped.CloseWithError(uint64(zmux.CodeCancelled), "")
	}
}

func publishAcceptedBidiStream(s *quicSession, stream *quicStream) bool {
	return s.publishBidiAcceptResult(bidiAcceptResult{stream: stream})
}

func publishAcceptedUniStream(s *quicSession, stream *quicRecvStream) bool {
	return s.publishUniAcceptResult(uniAcceptResult{stream: stream})
}

func publishBidiAcceptError(s *quicSession, err error) {
	_ = s.publishBidiAcceptResult(bidiAcceptResult{err: err})
}

func publishUniAcceptError(s *quicSession, err error) {
	_ = s.publishUniAcceptResult(uniAcceptResult{err: err})
}

func (s *quicSession) prepareAcceptedBidiStream(stream *quic.Stream) {
	if s == nil || stream == nil {
		return
	}
	prepareAcceptedStream(s, stream, newAcceptedBidiStream, quicActiveStreamPeerBidi, publishAcceptedBidiStream)
}

func (s *quicSession) prepareAcceptedUniStream(stream *quic.ReceiveStream) {
	if s == nil || stream == nil {
		return
	}
	prepareAcceptedStream(s, stream, newAcceptedRecvStream, quicActiveStreamPeerUni, publishAcceptedUniStream)
}

func (s *quicSession) publishBidiAcceptResult(result bidiAcceptResult) bool {
	if s == nil || s.conn == nil || s.bidiCh == nil {
		return false
	}
	select {
	case s.bidiCh <- result:
		return true
	case <-s.conn.Context().Done():
		return false
	}
}

func (s *quicSession) publishUniAcceptResult(result uniAcceptResult) bool {
	if s == nil || s.conn == nil || s.uniCh == nil {
		return false
	}
	select {
	case s.uniCh <- result:
		return true
	case <-s.conn.Context().Done():
		return false
	}
}
