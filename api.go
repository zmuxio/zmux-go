package zmux

import (
	"context"
	"io"
	"net"
	"time"
)

// Stream defines the repository-default bidirectional stream surface.
//
// Native zmux implements every method directly. Adapters may provide a
// best-effort mapping for methods that don't have a protocol-native equivalent.
type Stream interface {
	net.Conn
	StreamID() uint64
	OpenInfo() []byte
	Metadata() StreamMetadata
	UpdateMetadata(update MetadataUpdate) error
	WriteFinal(p []byte) (int, error)
	WritevFinal(parts ...[]byte) (int, error)
	CloseRead() error
	CloseWrite() error
	Reset(code uint64) error
	Abort() error
}

// SendStream defines the repository-default send-only stream surface.
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
	Reset(code uint64) error
	SetDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// RecvStream defines the repository-default receive-only stream surface.
type RecvStream interface {
	io.Reader
	io.Closer
	StreamID() uint64
	OpenInfo() []byte
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Metadata() StreamMetadata
	CloseRead() error
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
}

// Session defines the repository-default session surface used by native zmux
// and transport adapters.
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
	Wait(ctx context.Context) error
	Closed() bool
	State() SessionState
	Stats() SessionStats
}

// AsSession exposes a native *Conn through the stable Session interface.
func AsSession(conn *Conn) Session {
	if conn == nil {
		return nativeSession{}
	}
	return nativeSession{conn: conn}
}

// NewSession establishes a native zmux session and returns it through the
// stable Session interface.
func NewSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := New(conn, cfg)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	conn = nil
	return nativeSession{conn: session}, nil
}

// ClientSession establishes an initiator-native session and returns it through
// the stable Session interface.
func ClientSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := Client(conn, cfg)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	conn = nil
	return nativeSession{conn: session}, nil
}

// ServerSession establishes a responder-native session and returns it through
// the stable Session interface.
func ServerSession(conn io.ReadWriteCloser, cfg *Config) (Session, error) {
	session, err := Server(conn, cfg)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, err
	}
	conn = nil
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
		return ErrSessionClosed
	}
	return s.conn.Close()
}

func (s nativeSession) Wait(ctx context.Context) error {
	if s.conn == nil {
		return ErrSessionClosed
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
		return SessionStats{}
	}
	return s.conn.Stats()
}

var (
	_ Session    = nativeSession{}
	_ Stream     = (*nativeStream)(nil)
	_ SendStream = (*nativeSendStream)(nil)
	_ RecvStream = (*nativeRecvStream)(nil)
)
