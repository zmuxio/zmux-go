# zmux-go

`zmux-go` is the Go implementation of the `zmux v1` single-link stream
multiplexing protocol.

It exposes:

- native constructors that return `*zmux.Conn`
- a stable transport-agnostic `zmux.Session` interface
- bidirectional, send-only, and receive-only stream surfaces
- a separate QUIC adapter submodule at
  `github.com/zmuxio/zmux-go/adapter/quicmux`

## Create A Session

Use `zmux.New`, `zmux.Client`, or `zmux.Server` with any
`io.ReadWriteCloser` transport.

```go
package main

import (
	"context"
	"net"

	zmux "github.com/zmuxio/zmux-go"
)

func main() {
	rawConn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}

	session, err := zmux.New(rawConn, nil)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	stream, err := session.OpenStream(context.Background())
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	if _, err := stream.Write([]byte("hello")); err != nil {
		panic(err)
	}
}
```

Use explicit role constructors when the transport already dictates which side is
the initiator or responder:

```go
client, err := zmux.Client(rawConn, nil)
server, err := zmux.Server(rawConn, nil)
```

## Stable Session Interface

If you want to code against the repository-default interface instead of native
`*Conn`, use:

```go
var session zmux.Session

session, err = zmux.NewSession(rawConn, nil)
session, err = zmux.ClientSession(rawConn, nil)
session, err = zmux.ServerSession(rawConn, nil)
```

To expose an existing native connection through the stable interface:

```go
native, err := zmux.New(rawConn, nil)
session := zmux.AsSession(native)
```

## Open And Accept Streams

Bidirectional streams behave like `net.Conn`.

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

Accept them on the peer side:

```go
stream, err := session.AcceptStream(ctx)
if err != nil {
	return err
}
defer stream.Close()
```

## Unidirectional Streams

Use `OpenUniStream` / `OpenUniStreamWithOptions` for local send-only streams and
`AcceptUniStream` for peer-opened receive-only streams.

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

## Open Helpers

If you want to open and immediately send payload bytes in one call:

```go
stream, n, err := session.OpenAndSend(ctx, []byte("hello"))
send, n, err := session.OpenUniAndSend(ctx, []byte("hello"))
```

The `WithOptions` variants combine open metadata with the initial payload:

```go
stream, n, err := session.OpenAndSendWithOptions(ctx, zmux.OpenOptions{
	OpenInfo: []byte("ssh"),
}, []byte("client hello"))
```

## Open Metadata And Initial Hints

Use `OpenOptions` when opening a stream with:

- `OpenInfo`
- `InitialPriority`
- `InitialGroup`

```go
priority := uint64(7)
group := uint64(2)

stream, err := session.OpenStreamWithOptions(ctx, zmux.OpenOptions{
	OpenInfo:        []byte("ssh"),
	InitialPriority: &priority,
	InitialGroup:    &group,
})
```

Once a stream exists, inspect the received opener metadata through:

```go
info := stream.OpenInfo()
meta := stream.Metadata()
```

## Update Metadata

Before a stream becomes peer-visible on the wire, callers can update the local
metadata view:

```go
priority := uint64(9)
err := stream.UpdateMetadata(zmux.MetadataUpdate{
	Priority: &priority,
})
```

If the stream or adapter cannot carry that update on the wire anymore, the call
returns an error such as `zmux.ErrPriorityUpdateUnavailable` or
`zmux.ErrAdapterUnsupported`.

## Graceful Stream Close

Use the canonical stream termination methods:

- `CloseWrite()` for send-half graceful finish
- `WriteFinal(...)` / `WritevFinal(...)` to send final bytes and finish in one call
- `CloseRead()` or `CloseReadWithCode(...)` to stop reading
- `Close()` to close both local halves when they exist

```go
if _, err := stream.WriteFinal([]byte("goodbye")); err != nil {
	return err
}

if err := stream.CloseRead(); err != nil {
	return err
}
```

`CloseReadWithCode(code)` lets you choose the read-stop application code:

```go
if err := stream.CloseReadWithCode(uint64(zmux.CodeCancelled)); err != nil {
	return err
}
```

## Abortive Stream Termination

Use:

- `Reset(code)` or `ResetWithReason(code, reason)` to abort the local write side
- `Abort()` to abort the whole stream with the default cancelled code
- `AbortWithError(err)` or `AbortWithErrorCode(code, reason)` to abort with a
  specific application error

```go
if err := stream.Reset(uint64(zmux.CodeCancelled)); err != nil {
	return err
}

if err := stream.AbortWithErrorCode(uint64(zmux.CodeInternal), "backend failed"); err != nil {
	return err
}
```

## Deadlines

Bidirectional streams expose the usual `net.Conn` deadlines:

```go
_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
```

`SendStream` exposes write deadlines, and `RecvStream` exposes read deadlines.

## Session Lifecycle

Useful session-level methods:

- `Close()` for graceful session shutdown
- `Abort(err)` for abortive session shutdown
- `Wait(ctx)` to block until the session is fully terminated
- `Closed()` to check whether shutdown has completed
- `State()` to read the current public session lifecycle state
- `Stats()` to inspect queue, flow-control, and runtime counters

```go
go func() {
	if err := session.Wait(context.Background()); err != nil {
		// reconnect / cleanup / report
	}
}()
```

## PING / GOAWAY / Peer Close State

Native `*zmux.Conn` also exposes protocol helpers:

- `Ping(ctx, echo)`
- `GoAway(lastAcceptedBidi, lastAcceptedUni)`
- `GoAwayWithError(...)`
- `PeerGoAwayError()`
- `PeerCloseError()`
- `LocalPreface()`
- `PeerPreface()`
- `Negotiated()`

These are available on the native connection type, not on the stable
`zmux.Session` interface.

## Errors

Use Go error inspection helpers instead of direct equality:

```go
if errors.Is(err, zmux.ErrSessionClosed) {
	// session is gone
}

var appErr *zmux.ApplicationError
if errors.As(err, &appErr) {
	// inspect appErr.Code / appErr.Reason
}
```

Common surface errors include:

- `zmux.ErrSessionClosed`
- `zmux.ErrReadClosed`
- `zmux.ErrWriteClosed`
- `zmux.ErrOpenLimited`
- `zmux.ErrPriorityUpdateUnavailable`

## Configuration

Pass a `*zmux.Config` to constructors to customize behavior such as:

- negotiated capabilities
- local settings / limits
- keepalive behavior
- queue budgets and backlog limits
- event handling
- memory and control buffering budgets

```go
cfg := &zmux.Config{
	KeepaliveInterval: 15 * time.Second,
	EventHandler: func(ev zmux.Event) {
		// observe stream/session lifecycle
	},
}

session, err := zmux.New(rawConn, cfg)
```

## Joined Read/Write Halves

If your transport exposes separate read and write halves, join them into a
single `net.Conn`-compatible object first:

```go
joined := zmux.JoinConn(readHalf, writeHalf)
session, err := zmux.New(joined, nil)
```

## QUIC Adapter

The QUIC adapter lives in the separate submodule
`github.com/zmuxio/zmux-go/adapter/quicmux`.

```go
import (
	quic "github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/adapter/quicmux"
)

var qconn *quic.Conn
session := quicmux.WrapSession(qconn)
```

The adapter exposes the same stable `zmux.Session` interface while translating
the supported subset of stream metadata and termination semantics onto
`quic-go`.
