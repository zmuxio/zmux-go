package quicmux

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/wire"
)

const quicmuxStreamPreludeMaxPayload = 16 << 10

const quicmuxOpenCaps = wire.CapabilityOpenMetadata | wire.CapabilityPriorityHints | wire.CapabilityStreamGroups

// DefaultAcceptedPreludeReadTimeout bounds how long the adapter will wait for
// an accepted QUIC stream to produce its zmux adapter prelude.
const DefaultAcceptedPreludeReadTimeout = 5 * time.Second

// SessionOptions configures adapter-local behavior that does not exist on the
// stable zmux Session surface itself.
type SessionOptions struct {
	// AcceptedPreludeReadTimeout bounds how long AcceptStream / AcceptUniStream
	// will wait for the peer's adapter prelude. Zero uses the default. Negative
	// values disable the adapter-managed timeout.
	AcceptedPreludeReadTimeout time.Duration
}

// SessionConn is the quic-go connection shape required by the adapter.
//
// *quic.Conn satisfies this interface directly, so callers can pass accepted
// or dialed QUIC connections to WrapSession without any extra shim.
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

// WrapSession exposes a quic-go session / connection through the
// repository-default zmux Session interface.
func WrapSession(conn SessionConn) zmux.Session {
	return WrapSessionWithOptions(conn, SessionOptions{})
}

// WrapSessionWithOptions exposes a quic-go session / connection through the
// repository-default zmux Session interface with adapter-local options.
func WrapSessionWithOptions(conn SessionConn, opts SessionOptions) zmux.Session {
	if conn == nil {
		return &quicSession{}
	}
	return &quicSession{
		conn:                       conn,
		acceptedPreludeReadTimeout: normalizeAcceptedPreludeReadTimeout(opts.AcceptedPreludeReadTimeout),
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

type quicSession struct {
	conn                       SessionConn
	acceptedPreludeReadTimeout time.Duration
	bidiOnce                   sync.Once
	uniOnce                    sync.Once
	bidiCh                     chan bidiAcceptResult
	uniCh                      chan uniAcceptResult
	bidiGate                   chan struct{}
	uniGate                    chan struct{}
}

type bidiAcceptResult struct {
	stream *quic.Stream
	err    error
}

type uniAcceptResult struct {
	stream *quic.ReceiveStream
	err    error
}

func (s *quicSession) AcceptStream(ctx context.Context) (zmux.Stream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	ch, gate := s.ensureBidiAcceptLoop()
	if err := acquireAcceptGate(ctx, s.conn.Context(), gate); err != nil {
		return nil, err
	}
	defer releaseAcceptGate(gate)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.conn.Context().Done():
		if err := translateWaitError(context.Cause(s.conn.Context())); err != nil {
			return nil, err
		}
		return nil, zmux.ErrSessionClosed
	case result := <-ch:
		if result.err != nil {
			return nil, result.err
		}
		if result.stream == nil {
			return nil, zmux.ErrSessionClosed
		}
		return newAcceptedBidiStream(s.conn, result.stream, s.acceptedPreludeReadTimeout)
	}
}

func (s *quicSession) AcceptUniStream(ctx context.Context) (zmux.RecvStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	ch, gate := s.ensureUniAcceptLoop()
	if err := acquireAcceptGate(ctx, s.conn.Context(), gate); err != nil {
		return nil, err
	}
	defer releaseAcceptGate(gate)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.conn.Context().Done():
		if err := translateWaitError(context.Cause(s.conn.Context())); err != nil {
			return nil, err
		}
		return nil, zmux.ErrSessionClosed
	case result := <-ch:
		if result.err != nil {
			return nil, result.err
		}
		if result.stream == nil {
			return nil, zmux.ErrSessionClosed
		}
		return newAcceptedRecvStream(s.conn, result.stream, s.acceptedPreludeReadTimeout)
	}
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
	stream, err := s.conn.OpenStreamSync(defaultContext(ctx))
	if err != nil {
		return nil, translateError(err)
	}
	wrapped := &quicStream{stream: stream}
	initLocalStreamBase(&wrapped.quicStreamBase, s.conn, stream, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(); err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeInternal))
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeInternal))
		_ = stream.Close()
		return nil, err
	}
	return wrapped, nil
}

func (s *quicSession) OpenUniStreamWithOptions(ctx context.Context, opts zmux.OpenOptions) (zmux.SendStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	stream, err := s.conn.OpenUniStreamSync(defaultContext(ctx))
	if err != nil {
		return nil, translateError(err)
	}
	wrapped := &quicSendStream{stream: stream}
	initLocalStreamBase(&wrapped.quicStreamBase, s.conn, nil, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(); err != nil {
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeInternal))
		_ = stream.Close()
		return nil, err
	}
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
		return zmux.ErrSessionClosed
	}
	return translateError(s.conn.CloseWithError(0, ""))
}

func (s *quicSession) Abort(err error) {
	if s == nil || s.conn == nil {
		return
	}
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	_ = s.conn.CloseWithError(quic.ApplicationErrorCode(code), reason)
}

func (s *quicSession) Wait(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return zmux.ErrSessionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.Context().Done():
		return translateWaitError(context.Cause(s.conn.Context()))
	}
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
	return zmux.SessionStats{
		State: s.State(),
	}
}

func (s *quicSession) ensureBidiAcceptLoop() (<-chan bidiAcceptResult, chan struct{}) {
	if s == nil || s.conn == nil {
		return nil, nil
	}
	s.bidiOnce.Do(func() {
		s.bidiCh = make(chan bidiAcceptResult, 32)
		s.bidiGate = make(chan struct{}, 1)
		s.bidiGate <- struct{}{}
		go s.acceptBidiLoop()
	})
	return s.bidiCh, s.bidiGate
}

func (s *quicSession) ensureUniAcceptLoop() (<-chan uniAcceptResult, chan struct{}) {
	if s == nil || s.conn == nil {
		return nil, nil
	}
	s.uniOnce.Do(func() {
		s.uniCh = make(chan uniAcceptResult, 32)
		s.uniGate = make(chan struct{}, 1)
		s.uniGate <- struct{}{}
		go s.acceptUniLoop()
	})
	return s.uniCh, s.uniGate
}

func (s *quicSession) acceptBidiLoop() {
	for {
		stream, err := s.conn.AcceptStream(context.Background())
		if err != nil {
			s.publishBidiAcceptResult(bidiAcceptResult{err: translateError(err)})
			return
		}
		s.publishBidiAcceptResult(bidiAcceptResult{stream: stream})
	}
}

func (s *quicSession) acceptUniLoop() {
	for {
		stream, err := s.conn.AcceptUniStream(context.Background())
		if err != nil {
			s.publishUniAcceptResult(uniAcceptResult{err: translateError(err)})
			return
		}
		s.publishUniAcceptResult(uniAcceptResult{stream: stream})
	}
}

func (s *quicSession) publishBidiAcceptResult(result bidiAcceptResult) {
	if s == nil || s.conn == nil || s.bidiCh == nil {
		return
	}
	select {
	case s.bidiCh <- result:
	case <-s.conn.Context().Done():
	}
}

func (s *quicSession) publishUniAcceptResult(result uniAcceptResult) {
	if s == nil || s.conn == nil || s.uniCh == nil {
		return
	}
	select {
	case s.uniCh <- result:
	case <-s.conn.Context().Done():
	}
}

func acquireAcceptGate(ctx context.Context, connCtx context.Context, gate chan struct{}) error {
	if gate == nil {
		return zmux.ErrSessionClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-connCtx.Done():
		if err := translateWaitError(context.Cause(connCtx)); err != nil {
			return err
		}
		return zmux.ErrSessionClosed
	case <-gate:
		return nil
	}
}

func releaseAcceptGate(gate chan struct{}) {
	if gate == nil {
		return
	}
	gate <- struct{}{}
}

type quicStreamBase struct {
	conn          SessionConn
	reader        io.Reader
	preludeWriter io.Writer
	sendPrelude   bool

	writeMu sync.Mutex
	metaMu  sync.Mutex
	termMu  sync.Mutex

	prelude       []byte
	preludeSent   bool
	preludeFrozen bool
	preludeOffset int
	priority      uint64
	prioritySet   bool
	group         uint64
	groupEncoded  bool
	openInfo      []byte

	localReadClosed  atomic.Bool
	localWriteClosed atomic.Bool
	localReadErr     error
	localWriteErr    error
}

func initLocalStreamBase(base *quicStreamBase, conn SessionConn, reader io.Reader, writer io.Writer, opts zmux.OpenOptions) {
	if base == nil {
		return
	}
	*base = quicStreamBase{
		conn:          conn,
		reader:        reader,
		preludeWriter: writer,
		sendPrelude:   true,
		openInfo:      cloneBytes(opts.OpenInfo),
	}
	if opts.InitialPriority != nil {
		base.priority = *opts.InitialPriority
		base.prioritySet = true
	}
	if opts.InitialGroup != nil {
		base.group = *opts.InitialGroup
		base.groupEncoded = true
	}
}

func initAcceptedStreamBase(base *quicStreamBase, conn SessionConn, reader io.Reader, meta acceptedStreamMetadata) {
	if base == nil {
		return
	}
	*base = quicStreamBase{
		conn:         conn,
		reader:       reader,
		sendPrelude:  false,
		preludeSent:  true,
		priority:     meta.priority,
		prioritySet:  meta.prioritySet,
		group:        meta.group,
		groupEncoded: meta.groupEncoded,
		openInfo:     cloneBytes(meta.openInfo),
	}
}

func (b *quicStreamBase) localAddr() net.Addr {
	if b == nil || b.conn == nil {
		return nil
	}
	return b.conn.LocalAddr()
}

func (b *quicStreamBase) remoteAddr() net.Addr {
	if b == nil || b.conn == nil {
		return nil
	}
	return b.conn.RemoteAddr()
}

func (b *quicStreamBase) readFrom(src io.Reader, p []byte) (int, error) {
	if src == nil {
		return 0, zmux.ErrSessionClosed
	}
	if b != nil {
		if err := b.loadLocalReadErr(); err != nil {
			return 0, err
		}
	}
	if b != nil && b.localReadClosed.Load() {
		return 0, zmux.ErrReadClosed
	}
	n, err := src.Read(p)
	if err != nil && b != nil {
		if localErr := b.loadLocalReadErr(); localErr != nil {
			return n, localErr
		}
	}
	if err != nil && b != nil && b.localReadClosed.Load() {
		return n, zmux.ErrReadClosed
	}
	return n, translateReadError(err)
}

func (b *quicStreamBase) OpenInfo() []byte {
	if b == nil {
		return nil
	}
	b.metaMu.Lock()
	defer b.metaMu.Unlock()
	return cloneBytes(b.openInfo)
}

func (b *quicStreamBase) Metadata() zmux.StreamMetadata {
	if b == nil {
		return zmux.StreamMetadata{}
	}
	b.metaMu.Lock()
	defer b.metaMu.Unlock()

	var group *uint64
	if b.groupEncoded && b.group != 0 {
		value := b.group
		group = &value
	}

	return zmux.StreamMetadata{
		Priority: b.priority,
		Group:    group,
		OpenInfo: cloneBytes(b.openInfo),
	}
}

func (b *quicStreamBase) UpdateMetadata(update zmux.MetadataUpdate) error {
	if b == nil {
		return zmux.ErrSessionClosed
	}
	if b.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	if update.Priority == nil && update.Group == nil {
		return zmux.ErrEmptyMetadataUpdate
	}

	b.metaMu.Lock()
	if !b.sendPrelude || b.preludeSent || b.preludeFrozen {
		b.metaMu.Unlock()
		return errors.Join(zmux.ErrAdapterUnsupported, zmux.ErrPriorityUpdateUnavailable)
	}
	if update.Priority != nil {
		b.priority = *update.Priority
		b.prioritySet = true
	}
	if update.Group != nil {
		b.group = *update.Group
		b.groupEncoded = true
	}
	b.metaMu.Unlock()

	// In the adapter, a pre-data UpdateMetadata acts as the peer-visible open
	// advisory point. Once emitted, later updates are intentionally unsupported.
	return b.ensureOpenPrelude()
}

func (b *quicStreamBase) maybeSendOpenPreludeOnOpen() error {
	if b == nil {
		return zmux.ErrSessionClosed
	}
	b.metaMu.Lock()
	needs := b.sendPrelude && !b.preludeSent && b.hasPeerVisibleOpenMetadataLocked()
	b.metaMu.Unlock()
	if !needs {
		return nil
	}
	return b.ensureOpenPrelude()
}

func (b *quicStreamBase) ensureOpenPrelude() error {
	if b == nil || !b.sendPrelude || b.preludeWriter == nil {
		return nil
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	prelude, err := b.prepareOpenPrelude()
	if err != nil {
		return err
	}
	if prelude == nil {
		return nil
	}
	for b.preludeOffset < len(prelude) {
		n, err := b.preludeWriter.Write(prelude[b.preludeOffset:])
		if n > 0 {
			b.preludeOffset += n
		}
		if err != nil {
			return translateError(err)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	b.metaMu.Lock()
	b.preludeSent = true
	b.metaMu.Unlock()
	return nil
}

func (b *quicStreamBase) hasPeerVisibleOpenMetadataLocked() bool {
	return b.prioritySet || b.groupEncoded || len(b.openInfo) > 0
}

func (b *quicStreamBase) prepareOpenPrelude() ([]byte, error) {
	if b == nil {
		return nil, zmux.ErrSessionClosed
	}
	b.metaMu.Lock()
	defer b.metaMu.Unlock()
	if b.preludeSent {
		return nil, nil
	}
	if !b.preludeFrozen {
		prefix, err := wire.BuildOpenMetadataPrefix(
			quicmuxOpenCaps,
			uint64PtrIf(b.prioritySet, b.priority),
			uint64PtrIf(b.groupEncoded, b.group),
			b.openInfo,
			quicmuxStreamPreludeMaxPayload,
		)
		if err != nil {
			return nil, err
		}
		prelude, err := buildStreamPrelude(prefix)
		if err != nil {
			return nil, err
		}
		b.prelude = prelude
		b.preludeFrozen = true
	}
	return b.prelude, nil
}

func (b *quicStreamBase) loadLocalReadErr() error {
	if b == nil {
		return nil
	}
	b.termMu.Lock()
	defer b.termMu.Unlock()
	return b.localReadErr
}

func (b *quicStreamBase) loadLocalWriteErr() error {
	if b == nil {
		return nil
	}
	b.termMu.Lock()
	defer b.termMu.Unlock()
	return b.localWriteErr
}

func (b *quicStreamBase) storeLocalReadErr(err error) {
	if b == nil || err == nil {
		return
	}
	b.termMu.Lock()
	b.localReadErr = err
	b.termMu.Unlock()
}

func (b *quicStreamBase) storeLocalWriteErr(err error) {
	if b == nil || err == nil {
		return
	}
	b.termMu.Lock()
	b.localWriteErr = err
	b.termMu.Unlock()
}

type quicStream struct {
	quicStreamBase
	stream *quic.Stream
}

func newAcceptedBidiStream(conn SessionConn, stream *quic.Stream, timeout time.Duration) (*quicStream, error) {
	reader := bufio.NewReader(stream)
	if timeout > 0 {
		_ = stream.SetReadDeadline(time.Now().Add(timeout))
		defer func() {
			_ = stream.SetReadDeadline(time.Time{})
		}()
	}
	meta, err := readAcceptedStreamMetadata(reader)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeProtocol))
		_ = stream.Close()
		return nil, err
	}
	wrapped := &quicStream{stream: stream}
	initAcceptedStreamBase(&wrapped.quicStreamBase, conn, reader, meta)
	return wrapped, nil
}

func (s *quicStream) Read(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	return s.readFrom(s.reader, p)
}

func (s *quicStream) Write(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	if err := s.loadLocalWriteErr(); err != nil {
		return 0, err
	}
	if s.localWriteClosed.Load() {
		return 0, zmux.ErrWriteClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return 0, err
	}
	if s.localWriteClosed.Load() {
		return 0, zmux.ErrWriteClosed
	}
	n, err := s.stream.Write(p)
	if err != nil {
		if localErr := s.loadLocalWriteErr(); localErr != nil {
			return n, localErr
		}
	}
	if err != nil && s.localWriteClosed.Load() {
		return n, zmux.ErrWriteClosed
	}
	return n, translateError(err)
}

func (s *quicStream) Close() error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	errWrite := s.CloseWrite()
	if errors.Is(errWrite, zmux.ErrWriteClosed) || errors.Is(errWrite, zmux.ErrReadClosed) {
		errWrite = nil
	}
	errRead := s.CloseRead()
	if errors.Is(errRead, zmux.ErrReadClosed) || errors.Is(errRead, zmux.ErrWriteClosed) {
		errRead = nil
	}
	return errors.Join(errWrite, errRead)
}

func (s *quicStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicStream) LocalAddr() net.Addr {
	return s.localAddr()
}

func (s *quicStream) RemoteAddr() net.Addr {
	return s.remoteAddr()
}

func (s *quicStream) WriteFinal(p []byte) (int, error) {
	n, err := s.Write(p)
	if err != nil {
		return n, err
	}
	return n, s.CloseWrite()
}

func (s *quicStream) WritevFinal(parts ...[]byte) (int, error) {
	var total int
	for _, part := range parts {
		n, err := s.Write(part)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, s.CloseWrite()
}

func (s *quicStream) SetDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetDeadline(t))
}

func (s *quicStream) SetReadDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetReadDeadline(t))
}

func (s *quicStream) SetWriteDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetWriteDeadline(t))
}

func (s *quicStream) CloseRead() error {
	return s.CloseReadWithCode(uint64(zmux.CodeCancelled))
}

func (s *quicStream) CloseReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if s.localReadClosed.Load() {
		return zmux.ErrReadClosed
	}
	s.localReadClosed.Store(true)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CloseWrite() error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if s.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	if s.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	s.localWriteClosed.Store(true)
	if err := translateError(s.stream.Close()); err != nil {
		return err
	}
	return nil
}

func (s *quicStream) Reset(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code}
	s.localWriteClosed.Store(true)
	s.storeLocalWriteErr(appErr)
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeNoError))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicStream) CloseWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.localReadClosed.Store(true)
	s.localWriteClosed.Store(true)
	s.storeLocalReadErr(appErr)
	s.storeLocalWriteErr(appErr)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

type quicSendStream struct {
	quicStreamBase
	stream *quic.SendStream
}

func (s *quicSendStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicSendStream) LocalAddr() net.Addr {
	return s.localAddr()
}

func (s *quicSendStream) RemoteAddr() net.Addr {
	return s.remoteAddr()
}

func (s *quicSendStream) Write(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	if err := s.loadLocalWriteErr(); err != nil {
		return 0, err
	}
	if s.localWriteClosed.Load() {
		return 0, zmux.ErrWriteClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return 0, err
	}
	if s.localWriteClosed.Load() {
		return 0, zmux.ErrWriteClosed
	}
	n, err := s.stream.Write(p)
	if err != nil {
		if localErr := s.loadLocalWriteErr(); localErr != nil {
			return n, localErr
		}
	}
	if err != nil && s.localWriteClosed.Load() {
		return n, zmux.ErrWriteClosed
	}
	return n, translateError(err)
}

func (s *quicSendStream) WriteFinal(p []byte) (int, error) {
	n, err := s.Write(p)
	if err != nil {
		return n, err
	}
	return n, s.CloseWrite()
}

func (s *quicSendStream) WritevFinal(parts ...[]byte) (int, error) {
	var total int
	for _, part := range parts {
		n, err := s.Write(part)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, s.CloseWrite()
}

func (s *quicSendStream) CloseWrite() error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if s.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	if s.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	s.localWriteClosed.Store(true)
	if err := translateError(s.stream.Close()); err != nil {
		return err
	}
	return nil
}

func (s *quicSendStream) Reset(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code}
	s.localWriteClosed.Store(true)
	s.storeLocalWriteErr(appErr)
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicSendStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeNoError))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicSendStream) CloseWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.localWriteClosed.Store(true)
	s.storeLocalWriteErr(appErr)
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicSendStream) Close() error {
	err := s.CloseWrite()
	if errors.Is(err, zmux.ErrWriteClosed) {
		return nil
	}
	return err
}

func (s *quicSendStream) SetDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetWriteDeadline(t))
}

func (s *quicSendStream) SetWriteDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetWriteDeadline(t))
}

type quicRecvStream struct {
	quicStreamBase
	stream *quic.ReceiveStream
}

func newAcceptedRecvStream(conn SessionConn, stream *quic.ReceiveStream, timeout time.Duration) (*quicRecvStream, error) {
	reader := bufio.NewReader(stream)
	if timeout > 0 {
		_ = stream.SetReadDeadline(time.Now().Add(timeout))
		defer func() {
			_ = stream.SetReadDeadline(time.Time{})
		}()
	}
	meta, err := readAcceptedStreamMetadata(reader)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		return nil, err
	}
	wrapped := &quicRecvStream{stream: stream}
	initAcceptedStreamBase(&wrapped.quicStreamBase, conn, reader, meta)
	return wrapped, nil
}

func (s *quicRecvStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicRecvStream) LocalAddr() net.Addr {
	return s.localAddr()
}

func (s *quicRecvStream) RemoteAddr() net.Addr {
	return s.remoteAddr()
}

func (s *quicRecvStream) Read(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	return s.readFrom(s.reader, p)
}

func (s *quicRecvStream) CloseRead() error {
	return s.CloseReadWithCode(uint64(zmux.CodeCancelled))
}

func (s *quicRecvStream) CloseReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if s.localReadClosed.Load() {
		return zmux.ErrReadClosed
	}
	s.localReadClosed.Store(true)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicRecvStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeNoError))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicRecvStream) CloseWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.localReadClosed.Store(true)
	s.storeLocalReadErr(appErr)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicRecvStream) Close() error {
	err := s.CloseRead()
	if errors.Is(err, zmux.ErrReadClosed) {
		return nil
	}
	return err
}

func (s *quicRecvStream) SetDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetReadDeadline(t))
}

func (s *quicRecvStream) SetReadDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return translateError(s.stream.SetReadDeadline(t))
}

type acceptedStreamMetadata struct {
	priority     uint64
	prioritySet  bool
	group        uint64
	groupEncoded bool
	openInfo     []byte
}

func readAcceptedStreamMetadata(reader *bufio.Reader) (acceptedStreamMetadata, error) {
	if reader == nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("missing stream prelude reader", io.ErrUnexpectedEOF)
	}

	metadataLen, prefixLen, err := wire.ReadVarint(reader)
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream prelude length", err)
	}
	if metadataLen == 0 {
		return acceptedStreamMetadata{}, nil
	}
	if metadataLen+uint64(prefixLen) > quicmuxStreamPreludeMaxPayload {
		return acceptedStreamMetadata{}, zmux.ErrOpenMetadataTooLarge
	}

	payload := make([]byte, int(metadataLen))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream metadata", err)
	}

	tlvs, err := wire.ParseTLVs(payload)
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("parse stream metadata tlvs", err)
	}
	parsed, ok, err := wire.ParseStreamMetadataTLVs(tlvs)
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("parse stream metadata", err)
	}
	if !ok {
		return acceptedStreamMetadata{}, nil
	}

	return acceptedStreamMetadata{
		priority:     parsed.Priority,
		prioritySet:  parsed.HasPriority,
		group:        parsed.Group,
		groupEncoded: parsed.HasGroup,
		openInfo:     cloneBytes(parsed.OpenInfo),
	}, nil
}

func buildStreamPrelude(openPrefix []byte) ([]byte, error) {
	if len(openPrefix) != 0 {
		return openPrefix, nil
	}
	return wire.EncodeVarint(0)
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	return append([]byte(nil), src...)
}

func uint64PtrIf(ok bool, v uint64) *uint64 {
	if !ok {
		return nil
	}
	value := v
	return &value
}

func protocolPreludeErr(op string, err error) error {
	if err == nil {
		return &zmux.ApplicationError{Code: uint64(zmux.CodeProtocol), Reason: "quicmux: " + op}
	}
	return &zmux.ApplicationError{Code: uint64(zmux.CodeProtocol), Reason: fmt.Sprintf("quicmux: %s: %v", op, err)}
}

func translateReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	return translateError(err)
}

func translateWaitError(err error) error {
	if err == nil {
		return nil
	}
	if appErr, ok := findError[*quic.ApplicationError](err); ok && appErr.ErrorCode == 0 && appErr.ErrorMessage == "" {
		return nil
	}
	return translateError(err)
}

func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	if appErr, ok := findError[*quic.ApplicationError](err); ok {
		if appErr.ErrorCode == 0 && appErr.ErrorMessage == "" {
			return errors.Join(zmux.ErrSessionClosed, err)
		}
		return &zmux.ApplicationError{Code: uint64(appErr.ErrorCode), Reason: appErr.ErrorMessage}
	}

	if streamErr, ok := findError[*quic.StreamError](err); ok {
		return &zmux.ApplicationError{Code: uint64(streamErr.ErrorCode)}
	}

	if _, ok := findError[quic.StreamLimitReachedError](err); ok {
		return errors.Join(zmux.ErrOpenLimited, err)
	}

	if _, ok := findError[*quic.IdleTimeoutError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.HandshakeTimeoutError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.StatelessResetError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.VersionNegotiationError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if errors.Is(err, quic.Err0RTTRejected) || errors.Is(err, net.ErrClosed) {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	return err
}

func mappedApplicationError(err error, defaultCode uint64) (uint64, string) {
	if err == nil {
		return defaultCode, ""
	}
	if appErr, ok := findError[*zmux.ApplicationError](err); ok {
		return appErr.Code, appErr.Reason
	}
	return defaultCode, err.Error()
}

func findError[T any](err error) (T, bool) {
	var zero T
	if err == nil {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findError[T](child); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findError[T](wrapped.Unwrap())
	}
	return zero, false
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

var (
	_ zmux.Session    = (*quicSession)(nil)
	_ zmux.Stream     = (*quicStream)(nil)
	_ zmux.SendStream = (*quicSendStream)(nil)
	_ zmux.RecvStream = (*quicRecvStream)(nil)
)
