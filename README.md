# zmux-go

`zmux-go` is a Go library that implements the `zmux v1` single-link stream
multiplexing protocol.

## Create a session

Wrap any `io.ReadWriteCloser` transport and start using multiplexed streams:

```go
package main

import (
	"context"
	"net"

	zmux "github.com/zmuxio/zmux-go"
)

func main() {
	rawConn, _ := net.Dial("tcp", "127.0.0.1:9000")
	session, _ := zmux.New(rawConn, nil)
	defer session.Close()

	stream, _ := session.OpenStream(context.Background())
	defer stream.Close()

	_, _ = stream.Write([]byte("hello"))
}
```

## Open and accept streams

Bidirectional streams use `*Stream` and behave like `net.Conn`:

```go
ctx := context.Background()

stream, err := session.OpenStream(ctx)
if err != nil {
	return err
}
defer stream.Close()

_, err = stream.Write([]byte("ping"))
if err != nil {
	return err
}

buf := make([]byte, 4096)
n, err := stream.Read(buf)
```

On the receiving side:

```go
stream, err := session.AcceptStream(ctx)
if err != nil {
	return err
}
defer stream.Close()
```

## Open streams with metadata

Use `OpenStreamWithOptions` or `OpenUniStreamWithOptions` when you want to send
open metadata, initial priority, or an explicit group:

```go
stream, err := session.OpenStreamWithOptions(ctx, zmux.OpenOptions{
	OpenInfo: []byte("ssh"),
})
if err != nil {
	return err
}
defer stream.Close()
```

## Unidirectional streams

- `OpenUniStream` / `OpenUniStreamWithOptions` return `*SendStream`
- `AcceptUniStream` returns `*RecvStream`

`*SendStream` is write-only and `*RecvStream` is read-only, while sharing the
same lifecycle and deadline model as `*Stream`.

## Session helpers

Useful connection-level entry points:

- `session.Close()` gracefully closes the session
- `session.Wait(ctx)` waits for terminal shutdown
- `session.Stats()` returns runtime and pressure diagnostics

## Joined halves

If your transport exposes separate read and write halves, adapt them into a
single `net.Conn`-compatible object with `JoinConn`:

```go
joined := zmux.JoinConn(readHalf, writeHalf)
session, err := zmux.New(joined, nil)
```

## Stable Session Interface

If you want to code against a transport-agnostic session surface, use the
repository-default `zmux.Session` interface:

```go
var session zmux.Session
session, err := zmux.ClientSession(rawConn, nil)
```

The native constructors (`New`, `Client`, `Server`) still return `*Conn`, and
`zmux.AsSession(conn)` exposes an existing native connection through the stable
interface.

## QUIC Adapter

The QUIC adapter lives in the separate submodule
`github.com/zmuxio/zmux-go/adapter/quicmux` so importing the main `zmux-go`
module does not pull in `quic-go`.
