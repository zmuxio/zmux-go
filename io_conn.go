package zmux

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type closeReadOnly interface {
	CloseRead() error
}

type closeWriteOnly interface {
	CloseWrite() error
}

type deadlineSetter interface {
	SetDeadline(time.Time) error
}

type localAddrProvider interface {
	LocalAddr() net.Addr
}

type remoteAddrProvider interface {
	RemoteAddr() net.Addr
}

type ioConnState struct {
	closeMu sync.Mutex

	reader io.Reader
	writer io.Writer

	readClosed  atomic.Bool
	writeClosed atomic.Bool
}

type ioReadHalf struct {
	state *ioConnState
}

type ioWriteHalf struct {
	state *ioConnState
}

type ioCloseSide uint8

const (
	ioCloseReadSide ioCloseSide = iota
	ioCloseWriteSide
)

// JoinIO adapts ordinary Go I/O halves into a net.Conn-compatible JoinedConn.
//
// Either half may be nil, matching JoinConn. The returned connection forwards
// deadline and address calls when the underlying half exposes compatible
// methods such as SetReadDeadline, SetWriteDeadline, LocalAddr, or RemoteAddr.
// Plain io.Reader/io.Writer values that do not expose deadline methods return
// errors.ErrUnsupported from deadline setters.
func JoinIO(reader io.Reader, writer io.Writer) *JoinedConn {
	state := &ioConnState{reader: reader, writer: writer}
	var readHalf ReadHalf
	if reader != nil {
		readHalf = &ioReadHalf{state: state}
	}
	var writeHalf WriteHalf
	if writer != nil {
		writeHalf = &ioWriteHalf{state: state}
	}
	return JoinConn(readHalf, writeHalf)
}

// NewIO establishes a native zmux session on separate reader and writer halves.
func NewIO(reader io.Reader, writer io.Writer, cfg *Config) (*Conn, error) {
	conn, err := newIOConn(reader, writer)
	if err != nil {
		return nil, err
	}
	return New(conn, cfg)
}

// ClientIO establishes a native initiator session on separate reader and writer halves.
func ClientIO(reader io.Reader, writer io.Writer, cfg *Config) (*Conn, error) {
	conn, err := newIOConn(reader, writer)
	if err != nil {
		return nil, err
	}
	return Client(conn, cfg)
}

// ServerIO establishes a native responder session on separate reader and writer halves.
func ServerIO(reader io.Reader, writer io.Writer, cfg *Config) (*Conn, error) {
	conn, err := newIOConn(reader, writer)
	if err != nil {
		return nil, err
	}
	return Server(conn, cfg)
}

// NewIOSession returns a stable Session backed by native zmux on separate
// reader and writer halves.
func NewIOSession(reader io.Reader, writer io.Writer, cfg *Config) (Session, error) {
	session, err := NewIO(reader, writer, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

// ClientIOSession returns a stable initiator Session backed by native zmux on
// separate reader and writer halves.
func ClientIOSession(reader io.Reader, writer io.Writer, cfg *Config) (Session, error) {
	session, err := ClientIO(reader, writer, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

// ServerIOSession returns a stable responder Session backed by native zmux on
// separate reader and writer halves.
func ServerIOSession(reader io.Reader, writer io.Writer, cfg *Config) (Session, error) {
	session, err := ServerIO(reader, writer, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

func newIOConn(reader io.Reader, writer io.Writer) (*JoinedConn, error) {
	if reader == nil || writer == nil {
		return nil, ErrNilConn
	}
	return JoinIO(reader, writer), nil
}

func (h *ioReadHalf) Read(p []byte) (int, error) {
	reader, err := h.state.readerForRead()
	if err != nil {
		return 0, err
	}
	return reader.Read(p)
}

func (h *ioReadHalf) CloseRead() error {
	return h.state.closeRead()
}

func (h *ioReadHalf) Close() error {
	return h.state.closeAll()
}

func (h *ioReadHalf) joinedCloseIdentity() any {
	if h == nil {
		return nil
	}
	return h.state
}

func (h *ioReadHalf) SetReadDeadline(t time.Time) error {
	reader, err := h.state.readerForRead()
	if err != nil {
		return err
	}
	if setter, ok := reader.(readDeadlineSetter); ok {
		return setter.SetReadDeadline(t)
	}
	if setter, ok := reader.(deadlineSetter); ok {
		return setter.SetDeadline(t)
	}
	return errors.ErrUnsupported
}

func (h *ioReadHalf) LocalAddr() net.Addr {
	return h.state.localAddr()
}

func (h *ioReadHalf) RemoteAddr() net.Addr {
	return h.state.remoteAddr()
}

func (h *ioWriteHalf) Write(p []byte) (int, error) {
	writer, err := h.state.writerForWrite()
	if err != nil {
		return 0, err
	}
	return writer.Write(p)
}

func (h *ioWriteHalf) CloseWrite() error {
	return h.state.closeWrite()
}

func (h *ioWriteHalf) Close() error {
	return h.state.closeAll()
}

func (h *ioWriteHalf) joinedCloseIdentity() any {
	if h == nil {
		return nil
	}
	return h.state
}

func (h *ioWriteHalf) SetWriteDeadline(t time.Time) error {
	writer, err := h.state.writerForWrite()
	if err != nil {
		return err
	}
	if setter, ok := writer.(writeDeadlineSetter); ok {
		return setter.SetWriteDeadline(t)
	}
	if setter, ok := writer.(deadlineSetter); ok {
		return setter.SetDeadline(t)
	}
	return errors.ErrUnsupported
}

func (h *ioWriteHalf) LocalAddr() net.Addr {
	return h.state.localAddr()
}

func (h *ioWriteHalf) RemoteAddr() net.Addr {
	return h.state.remoteAddr()
}

func (s *ioConnState) readerForRead() (io.Reader, error) {
	if s == nil {
		return nil, ErrSessionClosed
	}
	if s.readClosed.Load() {
		return nil, ErrReadClosed
	}
	if s.reader == nil {
		return nil, ErrStreamNotReadable
	}
	return s.reader, nil
}

func (s *ioConnState) writerForWrite() (io.Writer, error) {
	if s == nil {
		return nil, ErrSessionClosed
	}
	if s.writeClosed.Load() {
		return nil, ErrWriteClosed
	}
	if s.writer == nil {
		return nil, ErrStreamNotWritable
	}
	return s.writer, nil
}

func (s *ioConnState) closeRead() error {
	return s.closeDirectional(ioCloseReadSide)
}

func (s *ioConnState) closeWrite() error {
	return s.closeDirectional(ioCloseWriteSide)
}

func (s *ioConnState) closeDirectional(side ioCloseSide) error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	reader := s.reader
	writer := s.writer
	shouldClose := false
	switch side {
	case ioCloseReadSide:
		if !s.readClosed.Load() {
			shouldClose = true
			s.readClosed.Store(true)
			if readerCloseIsFull(reader) && sameJoinedHalf(reader, writer) {
				s.writeClosed.Store(true)
			}
		}
	case ioCloseWriteSide:
		if !s.writeClosed.Load() {
			shouldClose = true
			s.writeClosed.Store(true)
			if writerCloseIsFull(writer) && sameJoinedHalf(reader, writer) {
				s.readClosed.Store(true)
			}
		}
	}
	s.closeMu.Unlock()
	if !shouldClose {
		return nil
	}
	if side == ioCloseReadSide {
		return closeReadDirectional(reader)
	}
	return closeWriteDirectional(writer)
}

func (s *ioConnState) closeAll() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	reader := s.reader
	writer := s.writer
	closeRead := !s.readClosed.Load() && reader != nil
	closeWrite := !s.writeClosed.Load() && writer != nil
	s.readClosed.Store(true)
	s.writeClosed.Store(true)
	s.closeMu.Unlock()

	var errs []error
	readClosedFully := false
	if closeRead {
		var err error
		readClosedFully, err = closeReadFull(reader)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if closeWrite && (!readClosedFully || !sameJoinedHalf(reader, writer)) {
		if err := closeWriteFull(writer); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *ioConnState) localAddr() net.Addr {
	if addr := localAddrOf(s.reader); addr != nil {
		return addr
	}
	if addr := localAddrOf(s.writer); addr != nil {
		return addr
	}
	return streamAddr{endpoint: "local"}
}

func (s *ioConnState) remoteAddr() net.Addr {
	if addr := remoteAddrOf(s.reader); addr != nil {
		return addr
	}
	if addr := remoteAddrOf(s.writer); addr != nil {
		return addr
	}
	return streamAddr{endpoint: "remote"}
}

func readerCloseIsFull(reader io.Reader) bool {
	if reader == nil {
		return false
	}
	if _, ok := reader.(closeReadOnly); ok {
		return false
	}
	_, ok := reader.(io.Closer)
	return ok
}

func writerCloseIsFull(writer io.Writer) bool {
	if writer == nil {
		return false
	}
	if _, ok := writer.(closeWriteOnly); ok {
		return false
	}
	_, ok := writer.(io.Closer)
	return ok
}

func closeReadDirectional(reader io.Reader) error {
	if reader == nil {
		return nil
	}
	if closer, ok := reader.(closeReadOnly); ok {
		return closer.CloseRead()
	}
	if closer, ok := reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func closeWriteDirectional(writer io.Writer) error {
	if writer == nil {
		return nil
	}
	if closer, ok := writer.(closeWriteOnly); ok {
		return closer.CloseWrite()
	}
	if closer, ok := writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func closeReadFull(reader io.Reader) (bool, error) {
	if reader == nil {
		return false, nil
	}
	if closer, ok := reader.(io.Closer); ok {
		return true, closer.Close()
	}
	if closer, ok := reader.(closeReadOnly); ok {
		return false, closer.CloseRead()
	}
	return false, nil
}

func closeWriteFull(writer io.Writer) error {
	if writer == nil {
		return nil
	}
	if closer, ok := writer.(io.Closer); ok {
		return closer.Close()
	}
	if closer, ok := writer.(closeWriteOnly); ok {
		return closer.CloseWrite()
	}
	return nil
}

func localAddrOf(v any) net.Addr {
	if provider, ok := v.(localAddrProvider); ok {
		return provider.LocalAddr()
	}
	return nil
}

func remoteAddrOf(v any) net.Addr {
	if provider, ok := v.(remoteAddrProvider); ok {
		return provider.RemoteAddr()
	}
	return nil
}

var (
	_ ReadHalf            = (*ioReadHalf)(nil)
	_ WriteHalf           = (*ioWriteHalf)(nil)
	_ io.Closer           = (*ioReadHalf)(nil)
	_ io.Closer           = (*ioWriteHalf)(nil)
	_ joinedCloseIdentity = (*ioReadHalf)(nil)
	_ joinedCloseIdentity = (*ioWriteHalf)(nil)
)
