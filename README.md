# zmux-go

Go implementation of zmux: multiplexed, flow-controlled streams and
datagrams over one or many reliable byte pipes.

zmux is wire-compatible with QMux (draft-ietf-quic-qmux-02): QUIC version 1
frames in size-prefixed records over TCP, TLS, WebSocket or any other
reliable carrier. With a plain QMux peer a session is a plain single-pipe
QMux session. Between zmux endpoints, transport parameters negotiate
multipath (many pipes, per-frame spreading, failover with reinjection),
lanes over multi-stream carriers, 0-RTT, resumption after every pipe is
lost, and the base extensions: roles, GOAWAY, stream preambles, priorities
and datagram flows.

## Modules

| Module | Purpose |
| --- | --- |
| `github.com/zmuxio/zmux-go` | core protocol, TCP/TLS/WebSocket/HTTP carriers |
| `github.com/zmuxio/zmux-go/zmuxquic` | QUIC carriers (quic-go) |
| `github.com/zmuxio/zmux-go/zmuxh3` | HTTP/3 and WebTransport carriers |

## Usage

```go
c, err := net.Dial("tcp", "127.0.0.1:4433")
if err != nil { ... }
conn, err := zmux.Dial(ctx, c, nil)
if err != nil { ... }
s, err := conn.OpenStreamSync(ctx)
s.Write([]byte("hello"))
s.Close()
```

The accepting side calls `zmux.Server` (or uses a `zmux.Listener`), then
`AcceptStream`. `Conn` and `Stream` follow the quic-go method set. See the
package documentation for sessions, pipes, multipath, lanes and nested
sessions.

## Testing

```
go test -race ./...
```

Run it in each module directory. The root `go.work` makes the sub-modules
build against the local root module; see RELEASING.md for the release
order.

## License

Apache-2.0
