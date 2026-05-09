package quicmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/wire"
)

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
	active           *quicActiveStreamCounters
	activeKind       quicActiveStreamKind
	activeTracked    atomic.Bool
}

type writeDeadlineSetter interface {
	SetWriteDeadline(time.Time) error
}

type quicWriteCloser interface {
	io.Writer
	Close() error
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

func (b *quicStreamBase) activate(active *quicActiveStreamCounters, kind quicActiveStreamKind) {
	if b == nil || active == nil || kind == quicActiveStreamNone {
		return
	}
	b.active = active
	b.activeKind = kind
	if b.activeTracked.CompareAndSwap(false, true) {
		active.add(kind)
		b.maybeFinishActive()
	}
}

func (b *quicStreamBase) maybeFinishActive() {
	if b == nil || !b.activeTracked.Load() {
		return
	}
	switch b.activeKind {
	case quicActiveStreamLocalBidi, quicActiveStreamPeerBidi:
		if b.localReadClosed.Load() && b.localWriteClosed.Load() {
			b.finishActive()
		}
	case quicActiveStreamLocalUni:
		if b.localWriteClosed.Load() {
			b.finishActive()
		}
	case quicActiveStreamPeerUni:
		if b.localReadClosed.Load() {
			b.finishActive()
		}
	default:
	}
}

func (b *quicStreamBase) finishActive() {
	if b == nil || b.active == nil || b.activeKind == quicActiveStreamNone {
		return
	}
	if b.activeTracked.CompareAndSwap(true, false) {
		b.active.done(b.activeKind)
	}
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
	translated := translateReadError(err)
	if err != nil && b != nil {
		if b.localReadClosed.Load() {
			if localErr := b.loadLocalReadErr(); localErr != nil {
				return n, localErr
			}
			return n, zmux.ErrReadClosed
		}
		if quicAdapterTerminalError(translated) {
			b.markLocalReadClosed(translated)
		}
	}
	return n, translated
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

	// Pre-data metadata is the adapter's peer-visible open advisory point.
	if err := b.preferLocalWriteError(b.ensureOpenPrelude()); err != nil {
		if quicAdapterTerminalError(err) {
			b.markLocalWriteClosed(err)
		}
		return err
	}
	return nil
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
		if quicAdapterTerminalError(err) {
			b.markLocalWriteClosed(err)
		}
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
	translated := translateError(err)
	if err != nil {
		if b.localWriteClosed.Load() {
			if localErr := b.loadLocalWriteErr(); localErr != nil {
				return n, localErr
			}
			return n, zmux.ErrWriteClosed
		}
		if quicAdapterTerminalError(translated) {
			b.markLocalWriteClosed(translated)
		}
		return n, translated
	}
	if n == 0 {
		return 0, io.ErrNoProgress
	}
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, translated
}

func (b *quicStreamBase) writevFinal(writer quicWriteCloser, parts [][]byte) (int, error) {
	total, ok := quicWritevTotalLen(parts)
	if !ok {
		return 0, errWritevPayloadTooLarge
	}
	if total == 0 {
		return 0, b.closeWrite(writer)
	}
	if len(parts) == 1 {
		n, err := b.writePayload(writer, parts[0])
		if err != nil {
			return n, err
		}
		return n, b.closeWrite(writer)
	}
	if total <= quicWritevCoalesceMaxBytes {
		p := coalesceQuicWritevParts(parts, total)
		n, err := b.writePayload(writer, p)
		if err != nil {
			return n, err
		}
		return n, b.closeWrite(writer)
	}

	var written int
	for _, part := range parts {
		n, err := b.writePayload(writer, part)
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, b.closeWrite(writer)
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
		if quicAdapterTerminalError(err) {
			b.markLocalWriteClosed(err)
		}
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

func (b *quicStreamBase) storeLocalWriteErr(err error) {
	if b == nil || err == nil {
		return
	}
	b.termMu.Lock()
	b.localWriteErr = err
	b.termMu.Unlock()
}

func (b *quicStreamBase) markLocalReadClosed(err error) bool {
	if b == nil {
		return false
	}
	b.termMu.Lock()
	if b.localReadClosed.Load() {
		b.termMu.Unlock()
		return false
	}
	if err != nil {
		b.localReadErr = err
	}
	b.localReadClosed.Store(true)
	b.termMu.Unlock()
	b.maybeFinishActive()
	return true
}

func (b *quicStreamBase) markLocalWriteClosed(err error) bool {
	if b == nil {
		return false
	}
	b.termMu.Lock()
	if b.localWriteClosed.Load() {
		b.termMu.Unlock()
		return false
	}
	if err != nil {
		b.localWriteErr = err
	}
	b.localWriteClosed.Store(true)
	b.termMu.Unlock()
	b.maybeFinishActive()
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
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	return s.writevFinal(s.stream, parts)
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
		if quicAdapterTerminalError(err) {
			s.markLocalReadClosed(err)
		}
		return err
	}
	if s.localReadClosed.Load() {
		return zmux.ErrReadClosed
	}
	s.markLocalReadClosed(nil)
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
	s.markLocalReadClosed(appErr)
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
	if s == nil || s.stream == nil {
		return 0, zmux.ErrSessionClosed
	}
	return s.writevFinal(s.stream, parts)
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
	s.markLocalReadClosed(nil)
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicRecvStream) CloseWithError(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	appErr := &zmux.ApplicationError{Code: code, Reason: reason}
	s.markLocalReadClosed(appErr)
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
