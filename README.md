# zmux-go

[![CI](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zmuxio/zmux-go.svg)](https://pkg.go.dev/github.com/zmuxio/zmux-go)
[![Release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=v*&label=release)](https://github.com/zmuxio/zmux-go/tags)
[![Go version](https://img.shields.io/github/go-mod/go-version/zmuxio/zmux-go)](go.mod)
[![License](https://img.shields.io/github/license/zmuxio/zmux-go)](LICENSE)

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
