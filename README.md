# zmux-go

[![CI](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zmuxio/zmux-go.svg)](https://pkg.go.dev/github.com/zmuxio/zmux-go)
[![Release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=v*&label=release)](https://github.com/zmuxio/zmux-go/tags)
[![Go version](https://img.shields.io/github/go-mod/go-version/zmuxio/zmux-go)](go.mod)
[![License](https://img.shields.io/github/license/zmuxio/zmux-go)](LICENSE)

[![zmuxquic docs](https://img.shields.io/badge/pkg.go.dev-zmuxquic-007d9c)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxquic)
[![zmuxquic release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=zmuxquic/*&label=zmuxquic)](https://github.com/zmuxio/zmux-go/tags)
[![zmuxh3 docs](https://img.shields.io/badge/pkg.go.dev-zmuxh3-007d9c)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxh3)
[![zmuxh3 release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=zmuxh3/*&label=zmuxh3)](https://github.com/zmuxio/zmux-go/tags)

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

## Client

```go
conn, err := zmux.Dial(ctx, "tcp", "example.com:4433", nil)
// or: zmux.DialTLS(ctx, "tcp", "example.com:4433", tlsConf, nil)
if err != nil {
	return err
}
defer conn.Close()

st, err := conn.OpenStreamSync(ctx)
if err != nil {
	return err
}
if _, err := st.Write([]byte("hello")); err != nil {
	return err
}
if err := st.Close(); err != nil { // FIN: the write side is done
	return err
}
reply, err := io.ReadAll(st)
```

`Client` starts a session on a `net.Conn` you already have. `Conn` and
`Stream` follow the quic-go method set; `zmux.StreamConn` turns a stream
into a `net.Conn`.

## Server

A `Listener` accepts sessions, and the pipes that later join them, from
any `net.Listener`:

```go
ln, err := net.Listen("tcp", ":4433")
if err != nil {
	return err
}
l := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
go l.Serve(ctx, ln) // for TLS: l.Serve(ctx, tls.NewListener(ln, tlsConf))

for {
	conn, err := l.Accept(ctx)
	if err != nil {
		return err
	}
	go func() {
		for {
			st, err := conn.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func() {
				io.Copy(st, st) // echo
				st.Close()
			}()
		}
	}()
}
```

`l.Shutdown(ctx, 0, "")` stops accepting and ends every session
gracefully. `zmux.Server` serves a single connection without multipath.

## Closing

`Conn.Close` ends the session at once and discards data not yet sent, unlike
`net.Conn.Close`. To let the peer read everything first:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := conn.Shutdown(ctx, 0, "")
```

## Errors

```go
_, err := st.Read(buf)
switch {
case errors.Is(err, zmux.ErrIdleTimeout):
	// the session timed out
case zmux.IsRetryable(err):
	// the peer did not process the stream: send it again
case errors.Is(err, net.ErrClosed):
	// the session ended for another reason
}
```

## Multipath

Both endpoints set `MaxPipes` above 1 and a join secret; the server uses a
`Listener`. The client adds pipes over other paths and follows them with
`NextEvent`:

```go
cfg := &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: psk}}
conn, err := zmux.Dial(ctx, "tcp", primaryAddr, cfg)
if err != nil {
	return err
}
nc, err := net.Dial("tcp", backupAddr)
if err != nil {
	return err
}
standby := true
p, err := conn.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{Label: "backup", SendStandby: &standby})
if err != nil {
	return err
}
for {
	ev, err := conn.NextEvent(ctx)
	if err != nil {
		return err
	}
	if ev.Kind == zmux.EventPipeFailed && ev.Pipe == p.ID() {
		log.Println("backup path failed")
	}
}
```

## WebSocket

A WebSocket connection carries one zmux record per binary message
(`FramingRecords`). The adapter below fits libraries whose connections read
and write whole messages:

```go
type wsConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
}

const binaryMessage = 2

type wsAdapter struct{ c wsConn }

func (a *wsAdapter) ReadMessage() ([]byte, error) {
	for {
		t, p, err := a.c.ReadMessage()
		if err != nil || t == binaryMessage {
			return p, err
		}
	}
}
func (a *wsAdapter) WriteMessage(p []byte) error { return a.c.WriteMessage(binaryMessage, p) }
func (a *wsAdapter) Read([]byte) (int, error)    { return 0, errors.New("message carrier") }
func (a *wsAdapter) Write([]byte) (int, error)   { return 0, errors.New("message carrier") }
func (a *wsAdapter) Close() error                { return a.c.Close() }

func wsPipe(c wsConn) zmux.PipeIO {
	a := &wsAdapter{c: c}
	return zmux.PipeIO{R: a, W: a, Framing: zmux.FramingRecords}
}

// client: zmux.Start(ctx, wsPipe(ws), zmux.RoleClient, cfg)
// server: l.HandlePipe(ctx, wsPipe(ws), zmux.JoinHint{})
```

A WebSocket library that offers a byte-stream `net.Conn` view works with
`zmux.Client` and `zmux.NetPipe` directly.

## HTTP exchanges

Where a long-lived HTTP request does not survive the path (CDNs and
reverse proxies that buffer whole bodies, HTTP/1.0 hops, request duration
limits, caches), an exchange carrier builds one zmux carrier out of a
sequence of short POST requests. The server mounts a handler in front of
its `Listener`:

```go
h := zmux.NewHTTPExchangeHandler(l, &zmux.HTTPExchangeConfig{PSK: psk})
defer h.Close()
http.Handle("/x/", h)
```

and the client opens the carrier with the same PSK:

```go
p, err := zmux.HTTPExchangePipe(ctx, client, "https://example.com/x/{instance}/{nonce}",
	&zmux.HTTPExchangeConfig{PSK: psk})
if err != nil {
	return err
}
conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg) // or conn.AddPipe(ctx, p, opts)
```

`{instance}` is the same in every request of a carrier (for load-balancer
stickiness) and `{nonce}` differs in every request (cache busting; without
it a query parameter is added). `DownURL` sends DOWN requests to another
template (scheme, host, port and path may all differ, through other
intermediaries); UP and CLOSE requests use the first. A carrier's state
lives in one server process: with several backends, route every request of
a carrier to the same one, for example by hashing `{instance}`. Both ends
should use the same piece sizes: a body over the receiver's limit fails the
carrier instead of being retried. Requests are numbered, idempotent and
authenticated, so lost, repeated, reordered and cached exchanges are
recovered; `HTTPExchangeConfig` documents the timers, sizes and limits and
their defaults. Keep the server's `HoldTime` below every intermediary's
response timeout, and the client transport's `MaxIdleConnsPerHost` at least
`DownRequests+UpRequests`.

The exchange MACs authenticate both ends (with a PSK) and bind each
response to its request, but they do not encrypt. When TLS is terminated by
a proxy or CDN, that intermediary can read the carrier.

## QUIC

```go
conn, err := zmuxquic.Dial(ctx, "example.com:4433", tlsConf, nil, nil)
```

On the server, `zmuxquic.Serve(ctx, ln, l)` hands the streams of a QUIC
listener to a `zmux.Listener`; `zmuxh3.Dial` and `zmuxh3.Serve` do the same
for WebTransport sessions.

## More

The package documentation and its examples cover datagrams, stream
preambles and priorities, early data and its retry, splicing streams
between sessions, lanes, nested sessions and the scheduler. The protocol
is specified in the zmux specification repository.

## Testing

```
go test -race ./...
```

Run it in each module directory. The root `go.work` makes the sub-modules
build against the local root module; see RELEASING.md for the release
order.

## License

Apache-2.0

### QUIC dial socket families

The `zmuxquic.Dial` and `NewPool` default dialer bind UDP using the resolved
destination's address family (`udp4` or `udp6`). This avoids a macOS dual-stack
wildcard ephemeral-port collision with a local IPv4 listener. Hostname-based
TLS verification and session-cache settings are preserved without modifying
the caller's TLS configuration. The adapter owns these sockets and releases
them after native connection shutdown, including 0-RTT rejection/continuation.
Existing QUIC connections passed to `Client` remain caller-owned.

Native QUIC 0-RTT stream creation is temporarily gated on handshake completion:
quic-go0.64.1 can race a stream's initial flow-control setup against peer
transport-parameter updates. This applies to default dialers and borrowed
connections passed to `OpenPipe`/`Client`; incoming early-data handling remains.
The workaround sacrifices native 0-RTT payload latency while preserving zmux
replayable/resumed startup, TLS validation, cancellation and socket ownership.
Remove the barrier after a dependency fix passes the preserved native
rejection reproducer under the race detector. No dependency fork is embedded.

To check whether an upstream update removes that limitation, explicitly run
`go test -race -tags=quicgo_upstream_repro ./zmuxquic -run TestQuicGoUpstreamEarlyStreamRejection -count=20`.
This dependency reproducer deliberately bypasses the adapter's handshake
barrier and currently detects the upstream race; it is not a supported
production path. The default rejection test exercises the guarded adapter.

The guarded outgoing stream's carrier verdict is accepted once its handshake
completes: its bytes were sent as 1-RTT and were not invalidated by a native
0-RTT rejection. This avoids replaying the newly created bootstrap twice.
When removing the temporary barrier, validate the verdict for this pipe's
initial bytes alongside stream opening. A connection-global `Used0RTT=false`
does not mean a pipe created as 1-RTT was rejected. Test accepted/rejected
resumption and remembered zmux parameters without a reusable TLS ticket.
