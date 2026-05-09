package quicmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

const acceptedPreludeResultQueueCap = 32
const defaultAcceptedPreludeMaxConcurrent = 8
const maxAcceptedPreludeMaxConcurrent = 1024

// DefaultAcceptedPreludeReadTimeout bounds how long the adapter will wait for
// an accepted QUIC stream to produce its zmux adapter prelude.
const DefaultAcceptedPreludeReadTimeout = 5 * time.Second

var defaultAcceptedPreludeMaxConcurrentValue atomic.Int64

func init() {
	defaultAcceptedPreludeMaxConcurrentValue.Store(defaultAcceptedPreludeMaxConcurrent)
}

// DefaultAcceptedPreludeMaxConcurrent returns the package default parse limit.
func DefaultAcceptedPreludeMaxConcurrent() int {
	if current := int(defaultAcceptedPreludeMaxConcurrentValue.Load()); current > 0 {
		if current > maxAcceptedPreludeMaxConcurrent {
			return maxAcceptedPreludeMaxConcurrent
		}
		return current
	}
	return 1
}

// SetDefaultAcceptedPreludeMaxConcurrent updates the default parse limit.
// Non-positive values restore the built-in default.
func SetDefaultAcceptedPreludeMaxConcurrent(max int) {
	if max <= 0 {
		max = defaultAcceptedPreludeMaxConcurrent
	} else if max > maxAcceptedPreludeMaxConcurrent {
		max = maxAcceptedPreludeMaxConcurrent
	}
	defaultAcceptedPreludeMaxConcurrentValue.Store(int64(max))
}

// SessionOptions configures adapter-local behavior.
type SessionOptions struct {
	// AcceptedPreludeReadTimeout bounds how long the adapter will wait for each
	// accepted QUIC stream prelude before dropping that stream. Zero uses the
	// default. Negative values disable the adapter-managed timeout.
	AcceptedPreludeReadTimeout time.Duration
	// AcceptedPreludeMaxConcurrent bounds how many accepted QUIC stream
	// adapter preludes this wrapped session may parse concurrently. Zero uses
	// the current package default.
	AcceptedPreludeMaxConcurrent int
}

// SessionConn is the quic-go connection shape required by the adapter.
type SessionConn interface {
	AcceptStream(ctx context.Context) (*quic.Stream, error)
	AcceptUniStream(ctx context.Context) (*quic.ReceiveStream, error)
	OpenStreamSync(ctx context.Context) (*quic.Stream, error)
	OpenUniStreamSync(ctx context.Context) (*quic.SendStream, error)
	CloseWithError(code quic.ApplicationErrorCode, desc string) error
	Context() context.Context
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// WrapSession exposes a quic-go connection as a zmux Session.
func WrapSession(conn SessionConn) zmux.Session {
	return WrapSessionWithOptions(conn, SessionOptions{})
}

// WrapSessionWithOptions exposes a quic-go connection as a zmux Session.
func WrapSessionWithOptions(conn SessionConn, opts SessionOptions) zmux.Session {
	if conn == nil {
		return &quicSession{}
	}
	return &quicSession{
		conn:                       conn,
		acceptedPreludeReadTimeout: normalizeAcceptedPreludeReadTimeout(opts.AcceptedPreludeReadTimeout),
		prepareSem:                 make(chan struct{}, normalizeAcceptedPreludeMaxConcurrent(opts.AcceptedPreludeMaxConcurrent)),
	}
}

func normalizeAcceptedPreludeReadTimeout(timeout time.Duration) time.Duration {
	switch {
	case timeout < 0:
		return 0
	case timeout == 0:
		return DefaultAcceptedPreludeReadTimeout
	default:
		return timeout
	}
}

func normalizeAcceptedPreludeMaxConcurrent(max int) int {
	if max > 0 {
		if max > maxAcceptedPreludeMaxConcurrent {
			return maxAcceptedPreludeMaxConcurrent
		}
		return max
	}
	return DefaultAcceptedPreludeMaxConcurrent()
}

type quicSession struct {
	conn                       SessionConn
	acceptedPreludeReadTimeout time.Duration
	prepareSem                 chan struct{}
	active                     quicActiveStreamCounters
	internalMu                 sync.Mutex
	internalActive             int
	internalDone               chan struct{}
	bidiOnce                   sync.Once
	uniOnce                    sync.Once
	bidiCh                     chan bidiAcceptResult
	uniCh                      chan uniAcceptResult
}

type quicActiveStreamKind uint8

const (
	quicActiveStreamNone quicActiveStreamKind = iota
	quicActiveStreamLocalBidi
	quicActiveStreamLocalUni
	quicActiveStreamPeerBidi
	quicActiveStreamPeerUni
)

type quicActiveStreamCounters struct {
	localBidi atomic.Uint64
	localUni  atomic.Uint64
	peerBidi  atomic.Uint64
	peerUni   atomic.Uint64
}

func (c *quicActiveStreamCounters) add(kind quicActiveStreamKind) {
	if c == nil {
		return
	}
	switch kind {
	case quicActiveStreamLocalBidi:
		c.localBidi.Add(1)
	case quicActiveStreamLocalUni:
		c.localUni.Add(1)
	case quicActiveStreamPeerBidi:
		c.peerBidi.Add(1)
	case quicActiveStreamPeerUni:
		c.peerUni.Add(1)
	default:
	}
}

func (c *quicActiveStreamCounters) done(kind quicActiveStreamKind) {
	if c == nil {
		return
	}
	switch kind {
	case quicActiveStreamLocalBidi:
		decrementAtomicUint64(&c.localBidi)
	case quicActiveStreamLocalUni:
		decrementAtomicUint64(&c.localUni)
	case quicActiveStreamPeerBidi:
		decrementAtomicUint64(&c.peerBidi)
	case quicActiveStreamPeerUni:
		decrementAtomicUint64(&c.peerUni)
	default:
	}
}

func (c *quicActiveStreamCounters) snapshot() zmux.ActiveStreamStats {
	if c == nil {
		return zmux.ActiveStreamStats{}
	}
	localBidi := c.localBidi.Load()
	localUni := c.localUni.Load()
	peerBidi := c.peerBidi.Load()
	peerUni := c.peerUni.Load()
	return zmux.ActiveStreamStats{
		LocalBidi: localBidi,
		LocalUni:  localUni,
		PeerBidi:  peerBidi,
		PeerUni:   peerUni,
		Total: saturatingAddUint64(
			saturatingAddUint64(localBidi, localUni),
			saturatingAddUint64(peerBidi, peerUni),
		),
	}
}

func decrementAtomicUint64(counter *atomic.Uint64) {
	for {
		current := counter.Load()
		if current == 0 {
			return
		}
		if counter.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func saturatingAddUint64(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

func (s *quicSession) AcceptStream(ctx context.Context) (zmux.Stream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	return acceptStreamFromLoop[zmux.Stream, bidiAcceptResult](ctx, s.conn, s.ensureBidiAcceptLoop())
}

func (s *quicSession) AcceptUniStream(ctx context.Context) (zmux.RecvStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	return acceptStreamFromLoop[zmux.RecvStream, uniAcceptResult](ctx, s.conn, s.ensureUniAcceptLoop())
}

func (s *quicSession) OpenStream(ctx context.Context) (zmux.Stream, error) {
	return s.OpenStreamWithOptions(ctx, zmux.OpenOptions{})
}

func (s *quicSession) OpenUniStream(ctx context.Context) (zmux.SendStream, error) {
	return s.OpenUniStreamWithOptions(ctx, zmux.OpenOptions{})
}

func (s *quicSession) OpenStreamWithOptions(ctx context.Context, opts zmux.OpenOptions) (zmux.Stream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	stream, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, translateError(err)
	}
	wrapped := &quicStream{stream: stream}
	initLocalStreamBase(&wrapped.quicStreamBase, s.conn, stream, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(ctx, stream); err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeInternal))
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeInternal))
		_ = stream.Close()
		return nil, err
	}
	wrapped.activate(&s.active, quicActiveStreamLocalBidi)
	return wrapped, nil
}

func (s *quicSession) OpenUniStreamWithOptions(ctx context.Context, opts zmux.OpenOptions) (zmux.SendStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	stream, err := s.conn.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, translateError(err)
	}
	wrapped := &quicSendStream{stream: stream}
	initLocalStreamBase(&wrapped.quicStreamBase, s.conn, nil, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(ctx, stream); err != nil {
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeInternal))
		_ = stream.Close()
		return nil, err
	}
	wrapped.activate(&s.active, quicActiveStreamLocalUni)
	return wrapped, nil
}

func (s *quicSession) OpenAndSend(ctx context.Context, p []byte) (zmux.Stream, int, error) {
	return s.OpenAndSendWithOptions(ctx, zmux.OpenOptions{}, p)
}

func (s *quicSession) OpenAndSendWithOptions(ctx context.Context, opts zmux.OpenOptions, p []byte) (zmux.Stream, int, error) {
	stream, err := s.OpenStreamWithOptions(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	n, writeErr := stream.Write(p)
	return stream, n, writeErr
}

func (s *quicSession) OpenUniAndSend(ctx context.Context, p []byte) (zmux.SendStream, int, error) {
	return s.OpenUniAndSendWithOptions(ctx, zmux.OpenOptions{}, p)
}

func (s *quicSession) OpenUniAndSendWithOptions(ctx context.Context, opts zmux.OpenOptions, p []byte) (zmux.SendStream, int, error) {
	stream, err := s.OpenUniStreamWithOptions(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	n, writeErr := stream.WriteFinal(p)
	return stream, n, writeErr
}

func (s *quicSession) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	var closeErr error
	if s.conn.Context().Err() != nil {
		closeErr = translateWaitError(context.Cause(s.conn.Context()))
	} else {
		closeErr = translateError(s.conn.CloseWithError(0, ""))
	}
	waitErr := s.waitClosedAndInternal(context.Background())
	return errors.Join(closeErr, waitErr)
}

func (s *quicSession) CloseWithError(err error) {
	if s == nil || s.conn == nil {
		return
	}
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	_ = s.conn.CloseWithError(quic.ApplicationErrorCode(code), reason)
}

func (s *quicSession) Wait(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.waitClosedAndInternal(ctx)
}

func (s *quicSession) waitClosedAndInternal(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.Context().Done():
		err = translateWaitError(context.Cause(s.conn.Context()))
	}
	if waitErr := s.waitInternal(ctx); waitErr != nil {
		return waitErr
	}
	return err
}

func (s *quicSession) Closed() bool {
	if s == nil || s.conn == nil {
		return true
	}
	return s.conn.Context().Err() != nil
}

func (s *quicSession) State() zmux.SessionState {
	if s == nil || s.conn == nil {
		return zmux.SessionStateInvalid
	}
	if s.conn.Context().Err() != nil {
		return zmux.SessionStateClosed
	}
	return zmux.SessionStateReady
}

func (s *quicSession) Stats() zmux.SessionStats {
	state := s.State()
	stats := zmux.SessionStats{State: state}
	if s != nil && state == zmux.SessionStateReady {
		stats.ActiveStreams = s.active.snapshot()
	}
	return stats
}
