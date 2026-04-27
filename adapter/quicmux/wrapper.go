package quicmux

import (
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
const metadataPayloadPoolInitCap = 128
const metadataPayloadPoolMaxRetainedCap = 2 << 10
const acceptedPreludeResultQueueCap = 32
const defaultAcceptedPreludeMaxConcurrent = 8
const maxAcceptedPreludeMaxConcurrent = 1024

const quicmuxOpenCaps = wire.CapabilityOpenMetadata | wire.CapabilityPriorityHints | wire.CapabilityStreamGroups

// DefaultAcceptedPreludeReadTimeout bounds how long the adapter will wait for
// an accepted QUIC stream to produce its zmux adapter prelude.
const DefaultAcceptedPreludeReadTimeout = 5 * time.Second

var defaultAcceptedPreludeMaxConcurrentValue atomic.Int64

var metadataPayloadPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, metadataPayloadPoolInitCap)
		return &buf
	},
}

var emptyStreamPrelude = []byte{0}

func init() {
	defaultAcceptedPreludeMaxConcurrentValue.Store(defaultAcceptedPreludeMaxConcurrent)
}

// DefaultAcceptedPreludeMaxConcurrent returns the package default upper bound
// for concurrently parsing accepted QUIC stream adapter preludes when a
// session does not override SessionOptions.AcceptedPreludeMaxConcurrent.
func DefaultAcceptedPreludeMaxConcurrent() int {
	if current := int(defaultAcceptedPreludeMaxConcurrentValue.Load()); current > 0 {
		if current > maxAcceptedPreludeMaxConcurrent {
			return maxAcceptedPreludeMaxConcurrent
		}
		return current
	}
	return 1
}

// SetDefaultAcceptedPreludeMaxConcurrent updates the package default upper
// bound for concurrently parsing accepted QUIC stream adapter preludes. Values
// less than or equal to zero restore the built-in default.
func SetDefaultAcceptedPreludeMaxConcurrent(max int) {
	if max <= 0 {
		max = defaultAcceptedPreludeMaxConcurrent
	} else if max > maxAcceptedPreludeMaxConcurrent {
		max = maxAcceptedPreludeMaxConcurrent
	}
	defaultAcceptedPreludeMaxConcurrentValue.Store(int64(max))
}

// SessionOptions configures adapter-local behavior that does not exist on the
// stable zmux Session surface itself.
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
	bidiOnce                   sync.Once
	uniOnce                    sync.Once
	bidiCh                     chan bidiAcceptResult
	uniCh                      chan uniAcceptResult
}

type bidiAcceptResult struct {
	stream zmux.Stream
	err    error
}

type uniAcceptResult struct {
	stream zmux.RecvStream
	err    error
}

func (s *quicSession) AcceptStream(ctx context.Context) (zmux.Stream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	ch := s.ensureBidiAcceptLoop()
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
		return result.stream, nil
	}
}

func (s *quicSession) AcceptUniStream(ctx context.Context) (zmux.RecvStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	ctx = defaultContext(ctx)
	ch := s.ensureUniAcceptLoop()
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
		return result.stream, nil
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
	if s.conn.Context().Err() != nil {
		return translateWaitError(context.Cause(s.conn.Context()))
	}
	return translateError(s.conn.CloseWithError(0, ""))
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

func (s *quicSession) ensureBidiAcceptLoop() <-chan bidiAcceptResult {
	if s == nil || s.conn == nil {
		return nil
	}
	s.bidiOnce.Do(func() {
		s.bidiCh = make(chan bidiAcceptResult, acceptedPreludeResultQueueCap)
		go s.acceptBidiLoop()
	})
	return s.bidiCh
}

func (s *quicSession) ensureUniAcceptLoop() <-chan uniAcceptResult {
	if s == nil || s.conn == nil {
		return nil
	}
	s.uniOnce.Do(func() {
		s.uniCh = make(chan uniAcceptResult, acceptedPreludeResultQueueCap)
		go s.acceptUniLoop()
	})
	return s.uniCh
}

func (s *quicSession) acceptBidiLoop() {
	for {
		stream, err := s.conn.AcceptStream(context.Background())
		if err != nil {
			s.publishBidiAcceptResult(bidiAcceptResult{err: translateError(err)})
			return
		}
		if !s.acquirePrepareSlot() {
			discardAcceptedBidiStream(stream)
			return
		}
		go func() {
			defer s.releasePrepareSlot()
			s.prepareAcceptedBidiStream(stream)
		}()
	}
}

func (s *quicSession) acceptUniLoop() {
	for {
		stream, err := s.conn.AcceptUniStream(context.Background())
		if err != nil {
			s.publishUniAcceptResult(uniAcceptResult{err: translateError(err)})
			return
		}
		if !s.acquirePrepareSlot() {
			discardAcceptedUniStream(stream)
			return
		}
		go func() {
			defer s.releasePrepareSlot()
			s.prepareAcceptedUniStream(stream)
		}()
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

func (s *quicSession) prepareAcceptedBidiStream(stream *quic.Stream) {
	if s == nil || stream == nil {
		return
	}
	wrapped, err := newAcceptedBidiStream(s.conn, stream, s.acceptedPreludeReadTimeout)
	if err != nil {
		return
	}
	s.publishBidiAcceptResult(bidiAcceptResult{stream: wrapped})
}

func (s *quicSession) prepareAcceptedUniStream(stream *quic.ReceiveStream) {
	if s == nil || stream == nil {
		return
	}
	wrapped, err := newAcceptedRecvStream(s.conn, stream, s.acceptedPreludeReadTimeout)
	if err != nil {
		return
	}
	s.publishUniAcceptResult(uniAcceptResult{stream: wrapped})
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

type writeDeadlineSetter interface {
	SetWriteDeadline(time.Time) error
}

type singleByteReader struct {
	reader io.Reader
	buf    [1]byte
}

func (r *singleByteReader) ReadByte() (byte, error) {
	if r == nil || r.reader == nil {
		return 0, io.ErrUnexpectedEOF
	}
	if _, err := io.ReadFull(r.reader, r.buf[:]); err != nil {
		return 0, err
	}
	return r.buf[0], nil
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
		openInfo:     meta.openInfo,
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
	if b != nil && b.localReadClosed.Load() {
		if err := b.loadLocalReadErr(); err != nil {
			return 0, err
		}
		return 0, zmux.ErrReadClosed
	}
	n, err := src.Read(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortBuffer
	}
	if err != nil && b != nil && b.localReadClosed.Load() {
		if localErr := b.loadLocalReadErr(); localErr != nil {
			return n, localErr
		}
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
	return b.preferLocalWriteError(b.ensureOpenPrelude())
}

func (b *quicStreamBase) maybeSendOpenPreludeOnOpen(ctx context.Context, deadlineSetter writeDeadlineSetter) error {
	if b == nil {
		return zmux.ErrSessionClosed
	}
	b.metaMu.Lock()
	needs := b.sendPrelude && !b.preludeSent && b.hasPeerVisibleOpenMetadataLocked()
	b.metaMu.Unlock()
	if !needs {
		return nil
	}
	return b.ensureOpenPreludeWithContext(ctx, deadlineSetter)
}

func (b *quicStreamBase) ensureOpenPrelude() error {
	if b == nil || !b.sendPrelude || b.preludeWriter == nil {
		return nil
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return b.ensureOpenPreludeLocked()
}

func (b *quicStreamBase) ensureOpenPreludeLocked() error {
	if b == nil || !b.sendPrelude || b.preludeWriter == nil {
		return nil
	}
	prelude, err := b.prepareOpenPrelude()
	if err != nil {
		return err
	}
	if prelude == nil {
		return nil
	}
	for b.preludeOffset < len(prelude) {
		remaining := prelude[b.preludeOffset:]
		n, err := b.preludeWriter.Write(remaining)
		if n < 0 || n > len(remaining) {
			return io.ErrShortWrite
		}
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
	b.prelude = nil
	b.metaMu.Unlock()
	return nil
}

func (b *quicStreamBase) ensureOpenPreludeWithContext(ctx context.Context, deadlineSetter writeDeadlineSetter) error {
	restore := installContextWriteDeadline(ctx, deadlineSetter)
	defer restore()
	return b.ensureOpenPrelude()
}

func (b *quicStreamBase) writePayload(writer io.Writer, p []byte) (int, error) {
	if writer == nil {
		return 0, zmux.ErrSessionClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if b.localWriteClosed.Load() {
		if err := b.loadLocalWriteErr(); err != nil {
			return 0, err
		}
		return 0, zmux.ErrWriteClosed
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if err := b.preferLocalWriteError(b.ensureOpenPreludeLocked()); err != nil {
		return 0, err
	}
	if b.localWriteClosed.Load() {
		if err := b.loadLocalWriteErr(); err != nil {
			return 0, err
		}
		return 0, zmux.ErrWriteClosed
	}
	n, err := writer.Write(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	if err != nil && b.localWriteClosed.Load() {
		if localErr := b.loadLocalWriteErr(); localErr != nil {
			return n, localErr
		}
		return n, zmux.ErrWriteClosed
	}
	return n, translateError(err)
}

func (b *quicStreamBase) closeWrite(closer interface{ Close() error }) error {
	if closer == nil {
		return zmux.ErrSessionClosed
	}
	if b.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	if err := b.preferLocalWriteError(b.ensureOpenPreludeLocked()); err != nil {
		return err
	}
	if b.localWriteClosed.Load() {
		return zmux.ErrWriteClosed
	}
	if !b.markLocalWriteClosed(nil) {
		return zmux.ErrWriteClosed
	}
	if err := translateError(closer.Close()); err != nil {
		return err
	}
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

func (b *quicStreamBase) markLocalWriteClosed(err error) bool {
	if b == nil {
		return false
	}
	b.termMu.Lock()
	defer b.termMu.Unlock()
	if b.localWriteClosed.Load() {
		return false
	}
	if err != nil {
		b.localWriteErr = err
	}
	b.localWriteClosed.Store(true)
	return true
}

func (b *quicStreamBase) preferLocalWriteError(err error) error {
	if err == nil || b == nil || !b.localWriteClosed.Load() {
		return err
	}
	if localErr := b.loadLocalWriteErr(); localErr != nil {
		return localErr
	}
	return zmux.ErrWriteClosed
}

type quicStream struct {
	quicStreamBase
	stream *quic.Stream
}

func newAcceptedBidiStream(conn SessionConn, stream *quic.Stream, timeout time.Duration) (*quicStream, error) {
	if timeout > 0 {
		_ = stream.SetReadDeadline(time.Now().Add(timeout))
		defer func() {
			_ = stream.SetReadDeadline(time.Time{})
		}()
	}
	meta, err := readAcceptedStreamMetadata(stream)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeProtocol))
		_ = stream.Close()
		return nil, err
	}
	wrapped := &quicStream{stream: stream}
	initAcceptedStreamBase(&wrapped.quicStreamBase, conn, stream, meta)
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
	return s.writePayload(s.stream, p)
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
	return s.CancelRead(uint64(zmux.CodeCancelled))
}

func (s *quicStream) CancelRead(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if s.localReadClosed.Load() {
		return zmux.ErrReadClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
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
	return s.closeWrite(s.stream)
}

func (s *quicStream) CancelWrite(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code}
	if !s.markLocalWriteClosed(appErr) {
		if err := s.loadLocalWriteErr(); err != nil {
			return err
		}
		return zmux.ErrWriteClosed
	}
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CloseWithError(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.storeLocalReadErr(appErr)
	s.localReadClosed.Store(true)
	cancelWrite := s.markLocalWriteClosed(appErr)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	if cancelWrite {
		s.stream.CancelWrite(quic.StreamErrorCode(code))
	}
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
	return s.writePayload(s.stream, p)
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
	return s.closeWrite(s.stream)
}

func (s *quicSendStream) CancelWrite(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code}
	if !s.markLocalWriteClosed(appErr) {
		if err := s.loadLocalWriteErr(); err != nil {
			return err
		}
		return zmux.ErrWriteClosed
	}
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicSendStream) CloseWithError(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	if s.markLocalWriteClosed(appErr) {
		s.stream.CancelWrite(quic.StreamErrorCode(code))
	}
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
	if timeout > 0 {
		_ = stream.SetReadDeadline(time.Now().Add(timeout))
		defer func() {
			_ = stream.SetReadDeadline(time.Time{})
		}()
	}
	meta, err := readAcceptedStreamMetadata(stream)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		return nil, err
	}
	wrapped := &quicRecvStream{stream: stream}
	initAcceptedStreamBase(&wrapped.quicStreamBase, conn, stream, meta)
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
	return s.CancelRead(uint64(zmux.CodeCancelled))
}

func (s *quicRecvStream) CancelRead(code uint64) error {
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

func (s *quicRecvStream) CloseWithError(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.storeLocalReadErr(appErr)
	s.localReadClosed.Store(true)
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

func readAcceptedStreamMetadata(reader io.Reader) (acceptedStreamMetadata, error) {
	if reader == nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("missing stream prelude reader", io.ErrUnexpectedEOF)
	}

	var (
		metadataLen uint64
		prefixLen   int
		err         error
	)
	if byteReader, ok := reader.(io.ByteReader); ok {
		metadataLen, prefixLen, err = wire.ReadVarint(byteReader)
	} else {
		byteReader := singleByteReader{reader: reader}
		metadataLen, prefixLen, err = wire.ReadVarint(&byteReader)
	}
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream prelude length", err)
	}
	if metadataLen == 0 {
		return acceptedStreamMetadata{}, nil
	}
	if metadataLen+uint64(prefixLen) > quicmuxStreamPreludeMaxPayload {
		return acceptedStreamMetadata{}, zmux.ErrOpenMetadataTooLarge
	}

	ptr := metadataPayloadPool.Get().(*[]byte)
	payload := *ptr
	if uint64(cap(payload)) < metadataLen {
		payload = make([]byte, int(metadataLen))
	} else {
		payload = payload[:int(metadataLen)]
	}
	defer func() {
		if cap(payload) > metadataPayloadPoolMaxRetainedCap {
			*ptr = make([]byte, 0, metadataPayloadPoolInitCap)
		} else {
			*ptr = payload[:0]
		}
		metadataPayloadPool.Put(ptr)
	}()
	if _, err := io.ReadFull(reader, payload); err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream metadata", err)
	}

	parsed, ok, err := wire.ParseStreamMetadataBytesView(payload)
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
	return emptyStreamPrelude, nil
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

const maxErrorUnwrapDepth = 64

func findError[T any](err error) (T, bool) {
	return findErrorDepth[T](err, 0)
}

func findErrorDepth[T any](err error, depth int) (T, bool) {
	var zero T
	if err == nil || depth > maxErrorUnwrapDepth {
		return zero, false
	}
	if target, ok := any(err).(T); ok {
		return target, true
	}
	if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range wrapped.Unwrap() {
			if target, ok := findErrorDepth[T](child, depth+1); ok {
				return target, true
			}
		}
		return zero, false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return findErrorDepth[T](wrapped.Unwrap(), depth+1)
	}
	return zero, false
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func installContextWriteDeadline(ctx context.Context, deadlineSetter writeDeadlineSetter) func() {
	if ctx == nil || deadlineSetter == nil {
		return func() {}
	}
	if ctx.Done() == nil {
		return func() {}
	}
	if ctx.Err() != nil {
		_ = deadlineSetter.SetWriteDeadline(time.Now())
		return func() {
			_ = deadlineSetter.SetWriteDeadline(time.Time{})
		}
	}

	clearOnReturn := false
	if deadline, ok := ctx.Deadline(); ok {
		_ = deadlineSetter.SetWriteDeadline(deadline)
		clearOnReturn = true
	}
	var (
		mu     sync.Mutex
		cond   = sync.NewCond(&mu)
		done   bool
		firing bool
		fired  bool
	)
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		if done {
			mu.Unlock()
			return
		}
		firing = true
		mu.Unlock()
		_ = deadlineSetter.SetWriteDeadline(time.Now())
		mu.Lock()
		firing = false
		fired = true
		cond.Broadcast()
		mu.Unlock()
	})
	return func() {
		stopped := stop()
		mu.Lock()
		done = true
		for firing && !fired {
			cond.Wait()
		}
		mu.Unlock()
		if stopped {
			if clearOnReturn {
				_ = deadlineSetter.SetWriteDeadline(time.Time{})
			}
			return
		}
		_ = deadlineSetter.SetWriteDeadline(time.Time{})
	}
}

var (
	_ zmux.Session    = (*quicSession)(nil)
	_ zmux.Stream     = (*quicStream)(nil)
	_ zmux.SendStream = (*quicSendStream)(nil)
	_ zmux.RecvStream = (*quicRecvStream)(nil)
)
