# zmux-go

`zmux-go` implements the `zmux v1` single-link stream multiplexing protocol in Go.

It provides:

- a native API on `*zmux.Conn`
- stable transport-agnostic interfaces: `Session`, `Stream`, `SendStream`, `RecvStream`
- native-only interfaces: `NativeSession`, `NativeStream`, `NativeSendStream`, `NativeRecvStream`
- a QUIC adapter in `github.com/zmuxio/zmux-go/adapter/quicmux`

## Constructors

Use the native constructors when you want the full zmux API:

```go
conn, err := zmux.New(rwc, cfg)
client, err := zmux.Client(rwc, cfg)
server, err := zmux.Server(rwc, cfg)
```

Use `New` when the transport does not already fix the initiator/responder role.
Use `Client` or `Server` when the role is already known.

Use the stable constructors when you want the transport-agnostic surface:

```go
session, err := zmux.NewSession(rwc, cfg)
session, err := zmux.ClientSession(rwc, cfg)
session, err := zmux.ServerSession(rwc, cfg)
```

You can also expose an existing native connection through the stable session interface:

```go
native, err := zmux.New(rwc, cfg)
session := zmux.AsSession(native)
```

## Stable Interfaces

`Session` is the stable session surface:

```go
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
```

`Stream` is the stable bidirectional stream surface:

```go
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
```

`SendStream` and `RecvStream` are the stable unidirectional stream surfaces:

```go
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
```

## Native Interfaces

Native constructors return `*zmux.Conn`, which satisfies `NativeSession`.

`NativeSession` repeats open/accept methods with native stream return types and adds native session controls:

```go
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
```

`NativeStream`, `NativeSendStream`, and `NativeRecvStream` extend the stable stream interfaces with local/native
queries:

```go
type NativeStream interface {
	Stream
	OpenedLocally() bool
	Bidirectional() bool
	ReadClosed() bool
	WriteClosed() bool
}

type NativeSendStream interface {
	SendStream
	OpenedLocally() bool
	Bidirectional() bool
	WriteClosed() bool
}

type NativeRecvStream interface {
	RecvStream
	OpenedLocally() bool
	Bidirectional() bool
	ReadClosed() bool
}
```

These interfaces are specific to native zmux. Adapters only need to implement the stable surfaces.

## Basic Use

Open and use a bidirectional stream:

```go
ctx := context.Background()

stream, err := session.OpenStream(ctx)
if err != nil {
	return err
}
defer stream.Close()

if _, err := stream.Write([]byte("ping")); err != nil {
	return err
}

buf := make([]byte, 4096)
n, err := stream.Read(buf)
```

Accept the peer side:

```go
stream, err := session.AcceptStream(ctx)
if err != nil {
	return err
}
defer stream.Close()
```

Use unidirectional streams when only one side writes:

```go
send, err := session.OpenUniStream(ctx)
if err != nil {
	return err
}
defer send.Close()

if _, err := send.Write([]byte("fire-and-forget")); err != nil {
	return err
}

recv, err := session.AcceptUniStream(ctx)
if err != nil {
	return err
}
defer recv.Close()
```

Open and send in one call:

```go
stream, n, err := session.OpenAndSend(ctx, []byte("hello"))
send, n, err := session.OpenUniAndSend(ctx, []byte("hello"))
```

Use `OpenAndSendWithOptions` or `OpenUniAndSendWithOptions` when the opener also carries metadata.

## Open Metadata

Use `OpenOptions` to send opener metadata:

```go
priority := uint64(7)
group := uint64(2)

stream, err := session.OpenStreamWithOptions(ctx, zmux.OpenOptions{
	OpenInfo:        []byte("ssh"),
	InitialPriority: &priority,
	InitialGroup:    &group,
})
```

Read opener metadata from the receiving side:

```go
info := stream.OpenInfo()
meta := stream.Metadata()
```

Update local metadata before the stream becomes peer-visible:

```go
priority := uint64(9)
err := stream.UpdateMetadata(zmux.MetadataUpdate{
	Priority: &priority,
})
```

If metadata can no longer be represented on the wire, the call returns an error such as
`zmux.ErrPriorityUpdateUnavailable` or `zmux.ErrAdapterUnsupported`.

## Stream Close And Cancel

Stable stream methods mean:

- `WriteFinal` / `WritevFinal`: write the final payload and finish the write side
- `CloseWrite`: graceful write-side finish
- `CancelWrite(code)`: abort the local write side
- `CloseRead`: stop the local read side with the default cancel code
- `CancelRead(code)`: stop the local read side with an explicit code
- `CloseWithError(code, reason)`: abort the whole stream
- `Close`: local helper that closes both existing halves

Payload buffers passed to `Write`, `WriteFinal`, `WritevFinal`, `OpenAndSend`,
`OpenAndSendWithOptions`, `OpenUniAndSend`, and `OpenUniAndSendWithOptions` are not
retained after the call returns. Callers may reuse or mutate their input slices
immediately after the method returns.

Examples:

```go
if _, err := stream.WriteFinal([]byte("goodbye")); err != nil {
	return err
}

if err := stream.CloseRead(); err != nil {
	return err
}

if err := stream.CancelWrite(uint64(zmux.CodeCancelled)); err != nil {
	return err
}

if err := stream.CloseWithError(uint64(zmux.CodeInternal), "backend failed"); err != nil {
	return err
}
```

Native zmux treats whole-stream abort as a first-class operation on bidirectional and unidirectional streams, so
`CloseWithError` exists on `Stream`, `SendStream`, and `RecvStream`.

## Deadlines

`Stream` exposes the usual `net.Conn` deadlines:

```go
_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
```

`SendStream` exposes write deadlines. `RecvStream` exposes read deadlines.

## Session Lifecycle

Stable session methods mean:

- `Close()`: graceful session close
- `CloseWithError(err)`: abortive session close
- `Wait(ctx)`: wait for termination
- `Closed()`: report whether the session has terminated
- `State()`: return the public session state
- `Stats()`: return runtime counters and pressure snapshots
- `Stats().ActiveStreams`: report active stream counts split by local/peer and bidi/uni

Example:

```go
go func() {
	if err := session.Wait(context.Background()); err != nil {
		// reconnect / cleanup / report
	}
}()
```

## Native Session Helpers

On native `*zmux.Conn`, you also have:

- `Ping(ctx, echo)`
- `GoAway(lastAcceptedBidi, lastAcceptedUni)`
- `GoAwayWithError(...)`
- `PeerGoAwayError()`
- `PeerCloseError()`
- `LocalPreface()`
- `PeerPreface()`
- `Negotiated()`

Example:

```go
conn, err := zmux.New(rwc, cfg)
if err != nil {
	return err
}

if _, err := conn.Ping(ctx, []byte("probe")); err != nil {
	return err
}
```

## Errors

Use standard Go error inspection:

```go
if errors.Is(err, zmux.ErrSessionClosed) {
	// session is gone
}

var appErr *zmux.ApplicationError
if errors.As(err, &appErr) {
	// inspect appErr.Code / appErr.Reason
}

var structured *zmux.Error
if errors.As(err, &structured) {
	// inspect Scope / Operation / Source / Direction / TerminationKind
}
```

Common surface errors include:

- `zmux.ErrSessionClosed`
- `zmux.ErrReadClosed`
- `zmux.ErrWriteClosed`
- `zmux.ErrOpenLimited`
- `zmux.ErrOpenExpired`
- `zmux.ErrPriorityUpdateUnavailable`
- `zmux.ErrAdapterUnsupported`

`ApplicationError` carries peer-visible application close codes and reason text.

## Configuration

Pass `*zmux.Config` to control capabilities, settings, keepalive, close timeouts, queue budgets, memory budgets, and
event hooks:

```go
cfg := zmux.DefaultConfig()
cfg.GracefulCloseDrainTimeout = 100 * time.Millisecond
cfg.PrefacePadding = true
cfg.PrefacePaddingMinBytes = 16
cfg.PrefacePaddingMaxBytes = 256
cfg.PingPadding = true
cfg.EventHandler = func(ev zmux.Event) {
	// observe stream/session lifecycle
}

session, err := zmux.New(rwc, cfg)
```

Start from `DefaultConfig()` unless you intentionally want to override fields
that have non-default zero values such as `Role`.

Use `ConfigureDefaultConfig` during process initialization to adjust the
process-wide template used by `DefaultConfig()` and by constructors called with
nil config:

```go
zmux.ConfigureDefaultConfig(func(cfg *zmux.Config) {
	cfg.PrefacePadding = true
	cfg.PingPadding = true
})
```

The global template does not retain per-session random values such as
`TieBreakerNonce` or `Settings.PingPaddingKey`; each session still generates
fresh values when needed. Concurrent default-template updates are race-safe, but
the last completed update wins, so configure it during process initialization.
`ResetDefaultConfig()` restores the built-in template.

`DefaultConfig()` already enables a low-frequency directional-idle keepalive
probe plus a slower cap on how long the session may go without sending any
PING for RTT sampling. Set `KeepaliveInterval = 0` to disable the automatic
keepalive logic entirely, or lower it when you want faster liveness detection.

Set `PrefacePadding = true` to append one random ignored settings TLV to the
local establishment preface. This varies the TLS record length of the zmux
handshake without changing negotiated settings; the default random padding
value length is 16..256 bytes, and `PrefacePaddingMinBytes` /
`PrefacePaddingMaxBytes` can tune that range.

Set `PingPadding = true` to append random opaque bytes to local PING frames and
recognized PONG replies. This also advertises a per-session padding key in the
local preface; when disabled, the key is omitted. This does not change
`Ping(ctx, echo)` API behavior; the default padding range is 16..64 bytes.

```go
cfg := &zmux.Config{
	Role:                     zmux.RoleAuto,
	PrefacePadding:           true,
	PingPadding:              true,
	KeepaliveInterval:        30 * time.Second,
	KeepaliveMaxPingInterval: 2 * time.Minute,
	GracefulCloseDrainTimeout: 100 * time.Millisecond,
	EventHandler: func(ev zmux.Event) {
		// observe stream/session lifecycle
	},
}

session, err := zmux.New(rwc, cfg)
```

## JoinConn

If your transport exposes separate read and write halves, combine them first:

```go
joined := zmux.JoinConn(readHalf, writeHalf)
session, err := zmux.New(joined, nil)
```

## QUIC Adapter

The QUIC adapter lives in `github.com/zmuxio/zmux-go/adapter/quicmux`:

```go
import (
	quic "github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/adapter/quicmux"
)

var qconn *quic.Conn
session := quicmux.WrapSession(qconn)
```

It implements the stable `zmux.Session` surface and maps the supported subset of metadata and termination semantics onto
`quic-go`. See `adapter/quicmux/README.md` for adapter details.
