package quicmux

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	quic "github.com/quic-go/quic-go"
	zmux "github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/wire"
)

const quicmuxStreamPreludeMaxPayload = 16 << 10

const quicmuxOpenCaps = wire.CapabilityOpenMetadata | wire.CapabilityPriorityHints | wire.CapabilityStreamGroups

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
	if conn == nil {
		return nil
	}
	return &quicSession{conn: conn}
}

type quicSession struct {
	conn SessionConn
}

func (s *quicSession) AcceptStream(ctx context.Context) (zmux.Stream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	stream, err := s.conn.AcceptStream(defaultContext(ctx))
	if err != nil {
		return nil, translateError(err)
	}
	wrapped, err := newAcceptedBidiStream(s.conn, stream)
	if err != nil {
		return nil, err
	}
	return wrapped, nil
}

func (s *quicSession) AcceptUniStream(ctx context.Context) (zmux.RecvStream, error) {
	if s == nil || s.conn == nil {
		return nil, zmux.ErrSessionClosed
	}
	stream, err := s.conn.AcceptUniStream(defaultContext(ctx))
	if err != nil {
		return nil, translateError(err)
	}
	wrapped, err := newAcceptedRecvStream(s.conn, stream)
	if err != nil {
		return nil, err
	}
	return wrapped, nil
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
	wrapped := newLocalBidiStream(s.conn, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(); err != nil {
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
	wrapped := newLocalSendStream(s.conn, stream, opts)
	if err := wrapped.maybeSendOpenPreludeOnOpen(); err != nil {
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
		return translateError(context.Cause(s.conn.Context()))
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

type quicStreamBase struct {
	conn          SessionConn
	reader        io.Reader
	preludeWriter io.Writer
	sendPrelude   bool

	writeMu sync.Mutex
	metaMu  sync.Mutex

	preludeSent  bool
	priority     uint64
	prioritySet  bool
	group        uint64
	groupEncoded bool
	openInfo     []byte
}

func newLocalStreamBase(conn SessionConn, reader io.Reader, writer io.Writer, opts zmux.OpenOptions) quicStreamBase {
	base := quicStreamBase{
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
	return base
}

func newAcceptedStreamBase(conn SessionConn, reader io.Reader, meta acceptedStreamMetadata) quicStreamBase {
	return quicStreamBase{
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
	n, err := src.Read(p)
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
	if update.Priority == nil && update.Group == nil {
		return zmux.ErrEmptyMetadataUpdate
	}

	b.metaMu.Lock()
	if !b.sendPrelude || b.preludeSent {
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

	b.metaMu.Lock()
	defer b.metaMu.Unlock()
	if b.preludeSent {
		return nil
	}

	prefix, err := wire.BuildOpenMetadataPrefix(
		quicmuxOpenCaps,
		uint64PtrIf(b.prioritySet, b.priority),
		uint64PtrIf(b.groupEncoded, b.group),
		b.openInfo,
		quicmuxStreamPreludeMaxPayload,
	)
	if err != nil {
		return err
	}
	prelude, err := buildStreamPrelude(prefix)
	if err != nil {
		return err
	}
	if err := writeAll(b.preludeWriter, prelude); err != nil {
		return translateError(err)
	}
	b.preludeSent = true
	return nil
}

func (b *quicStreamBase) hasPeerVisibleOpenMetadataLocked() bool {
	return b.prioritySet || b.groupEncoded || len(b.openInfo) > 0
}

type quicStream struct {
	quicStreamBase
	stream *quic.Stream
}

func newLocalBidiStream(conn SessionConn, stream *quic.Stream, opts zmux.OpenOptions) *quicStream {
	return &quicStream{
		quicStreamBase: newLocalStreamBase(conn, stream, stream, opts),
		stream:         stream,
	}
}

func newAcceptedBidiStream(conn SessionConn, stream *quic.Stream) (*quicStream, error) {
	reader := bufio.NewReader(stream)
	meta, err := readAcceptedStreamMetadata(reader)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		stream.CancelWrite(quic.StreamErrorCode(zmux.CodeProtocol))
		return nil, err
	}
	return &quicStream{
		quicStreamBase: newAcceptedStreamBase(conn, reader, meta),
		stream:         stream,
	}, nil
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
	if err := s.ensureOpenPrelude(); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.stream.Write(p)
	return n, translateError(err)
}

func (s *quicStream) Close() error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	return errors.Join(s.CloseWrite(), s.CloseRead())
}

func (s *quicStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicStream) ID() uint64 {
	return s.StreamID()
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

func (s *quicStream) CancelRead() error {
	return s.CloseRead()
}

func (s *quicStream) CloseReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CancelReadWithCode(code uint64) error {
	return s.CloseReadWithCode(code)
}

func (s *quicStream) CloseWrite() error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return translateError(s.stream.Close())
}

func (s *quicStream) Reset(code uint64) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicStream) CancelWrite() error {
	return s.CancelWriteWithCode(uint64(zmux.CodeCancelled))
}

func (s *quicStream) CancelWriteWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) ResetWrite() error {
	return s.CancelWrite()
}

func (s *quicStream) ResetWriteWithCode(code uint64) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicStream) ResetWithReason(code uint64, _ string) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicStream) Abort() error {
	return s.AbortWithErrorCode(uint64(zmux.CodeCancelled), "")
}

func (s *quicStream) AbortWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.AbortWithErrorCode(code, reason)
}

func (s *quicStream) AbortWithErrorCode(code uint64, _ string) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	s.stream.CancelRead(quic.StreamErrorCode(code))
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicStream) CloseWithErrorCode(code uint64, _ string) error {
	return s.AbortWithErrorCode(code, "")
}

type quicSendStream struct {
	quicStreamBase
	stream *quic.SendStream
}

func newLocalSendStream(conn SessionConn, stream *quic.SendStream, opts zmux.OpenOptions) *quicSendStream {
	return &quicSendStream{
		quicStreamBase: newLocalStreamBase(conn, nil, stream, opts),
		stream:         stream,
	}
}

func (s *quicSendStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicSendStream) ID() uint64 {
	return s.StreamID()
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
	if err := s.ensureOpenPrelude(); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	n, err := s.stream.Write(p)
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
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return translateError(s.stream.Close())
}

func (s *quicSendStream) Reset(code uint64) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicSendStream) CancelWrite() error {
	return s.CancelWriteWithCode(uint64(zmux.CodeCancelled))
}

func (s *quicSendStream) CancelWriteWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	if err := s.ensureOpenPrelude(); err != nil {
		return err
	}
	s.stream.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicSendStream) ResetWrite() error {
	return s.CancelWrite()
}

func (s *quicSendStream) ResetWriteWithCode(code uint64) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicSendStream) ResetWithReason(code uint64, _ string) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicSendStream) Abort() error {
	return s.AbortWithErrorCode(uint64(zmux.CodeCancelled), "")
}

func (s *quicSendStream) AbortWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.AbortWithErrorCode(code, reason)
}

func (s *quicSendStream) AbortWithErrorCode(code uint64, _ string) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicSendStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicSendStream) CloseWithErrorCode(code uint64, _ string) error {
	return s.CancelWriteWithCode(code)
}

func (s *quicSendStream) Close() error {
	return s.CloseWrite()
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

func newAcceptedRecvStream(conn SessionConn, stream *quic.ReceiveStream) (*quicRecvStream, error) {
	reader := bufio.NewReader(stream)
	meta, err := readAcceptedStreamMetadata(reader)
	if err != nil {
		stream.CancelRead(quic.StreamErrorCode(zmux.CodeProtocol))
		return nil, err
	}
	return &quicRecvStream{
		quicStreamBase: newAcceptedStreamBase(conn, reader, meta),
		stream:         stream,
	}, nil
}

func (s *quicRecvStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return uint64(s.stream.StreamID())
}

func (s *quicRecvStream) ID() uint64 {
	return s.StreamID()
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

func (s *quicRecvStream) CancelRead() error {
	return s.CloseRead()
}

func (s *quicRecvStream) CloseReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return zmux.ErrSessionClosed
	}
	s.stream.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicRecvStream) CancelReadWithCode(code uint64) error {
	return s.CloseReadWithCode(code)
}

func (s *quicRecvStream) Abort() error {
	return s.AbortWithErrorCode(uint64(zmux.CodeCancelled), "")
}

func (s *quicRecvStream) AbortWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.AbortWithErrorCode(code, reason)
}

func (s *quicRecvStream) AbortWithErrorCode(code uint64, _ string) error {
	return s.CloseReadWithCode(code)
}

func (s *quicRecvStream) CloseWithError(err error) error {
	code, reason := mappedApplicationError(err, uint64(zmux.CodeCancelled))
	return s.CloseWithErrorCode(code, reason)
}

func (s *quicRecvStream) CloseWithErrorCode(code uint64, _ string) error {
	return s.CloseReadWithCode(code)
}

func (s *quicRecvStream) Close() error {
	return s.CloseRead()
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

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if n > 0 {
			payload = payload[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
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

func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		if appErr.ErrorCode == 0 && appErr.ErrorMessage == "" {
			return errors.Join(zmux.ErrSessionClosed, err)
		}
		return &zmux.ApplicationError{Code: uint64(appErr.ErrorCode), Reason: appErr.ErrorMessage}
	}

	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		return &zmux.ApplicationError{Code: uint64(streamErr.ErrorCode)}
	}

	var limitErr quic.StreamLimitReachedError
	if errors.As(err, &limitErr) {
		return errors.Join(zmux.ErrOpenLimited, err)
	}

	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	var handshakeErr *quic.HandshakeTimeoutError
	if errors.As(err, &handshakeErr) {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	var statelessResetErr *quic.StatelessResetError
	if errors.As(err, &statelessResetErr) {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	var versionErr *quic.VersionNegotiationError
	if errors.As(err, &versionErr) {
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
	var appErr *zmux.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Code, appErr.Reason
	}
	return defaultCode, err.Error()
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
