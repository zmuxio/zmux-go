package zmux

import (
	"context"
	"io"
	"net"
	"time"
)

// Stream is the stable bidirectional stream interface.
type Stream interface {
	net.Conn
	StreamID() uint64
	OpenInfo() []byte
	Metadata() StreamMetadata
	UpdateMetadata(update MetadataUpdate) error
	WriteFinal(p []byte) (int, error)
	WritevFinal(parts ...[]byte) (int, error)
	CloseRead() error
	CancelRead(code uint64) error
	CloseWrite() error
	CancelWrite(code uint64) error
	CloseWithError(code uint64, reason string) error
}

// SendStream is the stable send-only stream interface.
type SendStream interface {
	io.Writer
	io.Closer
	StreamID() uint64
	OpenInfo() []byte
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Metadata() StreamMetadata
	UpdateMetadata(update MetadataUpdate) error
	WriteFinal(p []byte) (int, error)
	WritevFinal(parts ...[]byte) (int, error)
	CloseWrite() error
	CancelWrite(code uint64) error
	CloseWithError(code uint64, reason string) error
	SetDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// RecvStream is the stable receive-only stream interface.
type RecvStream interface {
	io.Reader
	io.Closer
	StreamID() uint64
	OpenInfo() []byte
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Metadata() StreamMetadata
	CloseRead() error
	CancelRead(code uint64) error
	CloseWithError(code uint64, reason string) error
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
}

// Session is the stable session interface used by native zmux and adapters.
type Session interface {
	io.Closer
	AcceptStream(ctx context.Context) (Stream, error)
	AcceptUniStream(ctx context.Context) (RecvStream, error)
	OpenStream(ctx context.Context) (Stream, error)
	OpenUniStream(ctx context.Context) (SendStream, error)
	OpenStreamWithOptions(ctx context.Context, opts OpenOptions) (Stream, error)
	OpenUniStreamWithOptions(ctx context.Context, opts OpenOptions) (SendStream, error)
	OpenAndSend(ctx context.Context, p []byte) (Stream, int, error)
	OpenAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (Stream, int, error)
	OpenUniAndSend(ctx context.Context, p []byte) (SendStream, int, error)
	OpenUniAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (SendStream, int, error)
	CloseWithError(err error)
	Wait(ctx context.Context) error
	Closed() bool
	State() SessionState
	Stats() SessionStats
}

// NativeSession is the native zmux session interface.
//
// It repeats open and accept methods with native stream return types because Go
// does not support covariant interface return types.
type NativeSession interface {
	AcceptStream(ctx context.Context) (NativeStream, error)
	AcceptUniStream(ctx context.Context) (NativeRecvStream, error)
	OpenStream(ctx context.Context) (NativeStream, error)
	OpenUniStream(ctx context.Context) (NativeSendStream, error)
	OpenStreamWithOptions(ctx context.Context, opts OpenOptions) (NativeStream, error)
	OpenUniStreamWithOptions(ctx context.Context, opts OpenOptions) (NativeSendStream, error)
	OpenAndSend(ctx context.Context, p []byte) (NativeStream, int, error)
	OpenAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (NativeStream, int, error)
	OpenUniAndSend(ctx context.Context, p []byte) (NativeSendStream, int, error)
	OpenUniAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (NativeSendStream, int, error)
	Close() error
	CloseWithError(err error)
	Wait(ctx context.Context) error
	Closed() bool
	State() SessionState
	Stats() SessionStats
	Ping(ctx context.Context, echo []byte) (time.Duration, error)
	GoAway(lastAcceptedBidi, lastAcceptedUni uint64) error
	GoAwayWithError(lastAcceptedBidi, lastAcceptedUni, code uint64, reason string) error
	PeerGoAwayError() *ApplicationError
	PeerCloseError() *ApplicationError
	LocalPreface() Preface
	PeerPreface() Preface
	Negotiated() Negotiated
}

// NativeStream is the native bidirectional stream interface.
type NativeStream interface {
	Stream
	OpenedLocally() bool
	Bidirectional() bool
	ReadClosed() bool
	WriteClosed() bool
}

// NativeSendStream is the native send-only stream interface.
type NativeSendStream interface {
	SendStream
	OpenedLocally() bool
	Bidirectional() bool
	WriteClosed() bool
}

// NativeRecvStream is the native receive-only stream interface.
type NativeRecvStream interface {
	RecvStream
	OpenedLocally() bool
	Bidirectional() bool
	ReadClosed() bool
}

// AsSession wraps a native *Conn as a stable Session.
func AsSession(conn *Conn) Session {
	if conn == nil {
		return nativeSession{}
	}
	return nativeSession{conn: conn}
}

// NewSession returns a stable Session backed by native zmux.
func NewSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := New(conn, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

// ClientSession returns a stable initiator Session backed by native zmux.
func ClientSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := Client(conn, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

// ServerSession returns a stable responder Session backed by native zmux.
func ServerSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := Server(conn, cfg)
	if err != nil {
		return nil, err
	}
	return nativeSession{conn: session}, nil
}

type nativeSession struct {
	conn *Conn
}

func (s nativeSession) AcceptStream(ctx context.Context) (Stream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.AcceptStream(ctx)
}

func (s nativeSession) AcceptUniStream(ctx context.Context) (RecvStream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.AcceptUniStream(ctx)
}

func (s nativeSession) OpenStream(ctx context.Context) (Stream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.OpenStream(ctx)
}

func (s nativeSession) OpenUniStream(ctx context.Context) (SendStream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.OpenUniStream(ctx)
}

func (s nativeSession) OpenStreamWithOptions(ctx context.Context, opts OpenOptions) (Stream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.OpenStreamWithOptions(ctx, opts)
}

func (s nativeSession) OpenUniStreamWithOptions(ctx context.Context, opts OpenOptions) (SendStream, error) {
	if s.conn == nil {
		return nil, ErrSessionClosed
	}
	return s.conn.OpenUniStreamWithOptions(ctx, opts)
}

func (s nativeSession) OpenAndSend(ctx context.Context, p []byte) (Stream, int, error) {
	if s.conn == nil {
		return nil, 0, ErrSessionClosed
	}
	return s.conn.OpenAndSend(ctx, p)
}

func (s nativeSession) OpenAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (Stream, int, error) {
	if s.conn == nil {
		return nil, 0, ErrSessionClosed
	}
	return s.conn.OpenAndSendWithOptions(ctx, opts, p)
}

func (s nativeSession) OpenUniAndSend(ctx context.Context, p []byte) (SendStream, int, error) {
	if s.conn == nil {
		return nil, 0, ErrSessionClosed
	}
	return s.conn.OpenUniAndSend(ctx, p)
}

func (s nativeSession) OpenUniAndSendWithOptions(ctx context.Context, opts OpenOptions, p []byte) (SendStream, int, error) {
	if s.conn == nil {
		return nil, 0, ErrSessionClosed
	}
	return s.conn.OpenUniAndSendWithOptions(ctx, opts, p)
}

func (s nativeSession) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s nativeSession) CloseWithError(err error) {
	if s.conn == nil {
		return
	}
	s.conn.CloseWithError(err)
}

func (s nativeSession) Wait(ctx context.Context) error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Wait(ctx)
}

func (s nativeSession) Closed() bool {
	if s.conn == nil {
		return true
	}
	return s.conn.Closed()
}

func (s nativeSession) State() SessionState {
	if s.conn == nil {
		return SessionStateInvalid
	}
	return s.conn.State()
}

func (s nativeSession) Stats() SessionStats {
	if s.conn == nil {
		return SessionStats{State: SessionStateInvalid}
	}
	return s.conn.Stats()
}

var (
	_ Session          = nativeSession{}
	_ NativeSession    = (*Conn)(nil)
	_ Stream           = (*nativeStream)(nil)
	_ NativeStream     = (*nativeStream)(nil)
	_ SendStream       = (*nativeSendStream)(nil)
	_ NativeSendStream = (*nativeSendStream)(nil)
	_ RecvStream       = (*nativeRecvStream)(nil)
	_ NativeRecvStream = (*nativeRecvStream)(nil)
)
