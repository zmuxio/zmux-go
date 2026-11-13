# zmux-go

[![CI](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zmuxio/zmux-go.svg)](https://pkg.go.dev/github.com/zmuxio/zmux-go)
[![Release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=v*&label=release)](https://github.com/zmuxio/zmux-go/tags)
[![Go version](https://img.shields.io/github/go-mod/go-version/zmuxio/zmux-go)](go.mod)
[![License](https://img.shields.io/github/license/zmuxio/zmux-go)](LICENSE)

[![zmuxquic](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxquic%2F%40latest&query=%24.Version&label=zmuxquic&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxquic)
[![zmuxh3](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxh3%2F%40latest&query=%24.Version&label=zmuxh3&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxh3)
[![zmuxws](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxws%2F%40latest&query=%24.Version&label=zmuxws&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxws)
[![seal](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fseal%2F%40latest&query=%24.Version&label=seal&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/seal)

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
| `github.com/zmuxio/zmux-go` | core protocol, TCP/TLS carriers, any message carrier; packages `zmuxhttp` (HTTP carriers), `admit` (admission helpers such as `PrefixAuth`) and `carrier` (utilities for carrier adapters) |
| `github.com/zmuxio/zmux-go/zmuxquic` | QUIC carriers |
| `github.com/zmuxio/zmux-go/zmuxh3` | HTTP/3 and WebTransport carriers |
| `github.com/zmuxio/zmux-go/zmuxws` | WebSocket carriers |
| `github.com/zmuxio/zmux-go/seal` | secure carrier: end-to-end handshake and record protection (experimental) |

## Client

```go
conn, err := zmux.Dial(ctx, "tcp", "example.com:4433", nil)
// or: zmux.DialTLS(ctx, "tcp", "example.com:4433", tlsConf, nil)
if err != nil {
	return err
}
defer func() { _ = conn.Close() }()

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

`Client` starts a session on a `net.Conn` you already have. A connection or
`PipeIO` handed to `Client`, `Server`, `Start`, `AddPipe` or `HandlePipe`
belongs to zmux from then on, whether the call succeeds or fails: zmux
closes it, the caller never does. The `ctx` of `Dial`, `Client` and `Start`
bounds establishment only; `conn.Context()` is the session's own. `Conn` and
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
l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
if err != nil {
	return err // the whole configuration is checked here
}
defer func() { _ = l.Close() }()
go func() { _ = l.Serve(ctx, ln) }() // for TLS: l.Serve(ctx, tls.NewListener(ln, tlsConf))

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
				_, _ = io.Copy(st, st) // echo
				_ = st.Close()
			}()
		}
	}()
}
```

`l.Shutdown(ctx, 0, "")` stops accepting new sessions, ends every session
gracefully while still taking their joins and resumptions, and then
closes the listener; `l.Close()` refuses every carrier at once. Close every
listener you made: one that took a carrier runs a goroutine until then.
`zmux.Server` serves a single connection without multipath or resume, and
rejects `MaxPipes` above 1 and `ResumeTimeout` with `ErrInvalidConfig`.

In `Config` and the other configuration structs, the zero value of a field
selects its stated default, and a negative value turns the setting off, or
makes a limit unlimited, where the field has such a setting; other negative
values are invalid. A feature that is on by default has a `Switch` field
(`SwitchOff` turns it off). A value out of range is `ErrInvalidConfig`,
never corrected. `JoinTimeout` (10 s) bounds every join and cannot be
switched off; `HandshakeTimeout` bounds establishment only.
`ListenerConfig.MaxPending` (1024 by default) bounds the carriers a
listener holds before they belong to a session. `Config.Logger` (a `*slog.Logger`) receives the
warnings an application would otherwise not see.

## Closing

`Conn.CloseWithError(code, reason)` closes the session gracefully with an
application error code and reason, which the peer sees as
`*zmux.ApplicationError`; `Conn.Close` is `CloseWithError(0, "")`. It first
sends what the streams have written, as far as flow control and the
carriers allow, then hands CONNECTION_CLOSE to every usable carrier (written
to it; for a socket, taken into the kernel's send buffer). It returns at the
latest 1 s after the call, flushing included, so a process may exit as soon
as it returns; what was not handed over by then is discarded. Open streams
are not waited for; they fail with the session's error.
`Conn.Abort(code, reason)` ends the session at once with an application
error and discards unsent data. The first close or abort decides the code
and reason; an abort while a close is still flushing ends the flush at once.

`Wait` waits for the session's whole end: until the peer has read
CONNECTION_CLOSE and ended its side of every carrier, or the linger timeout
has expired. `Done` is its selectable form, so "delivered or given up after
d" is a `select`:

```go
_ = conn.CloseWithError(0x2a, "replaced")
select {
case <-conn.Done(): // the peer read the close (or the linger ended)
case <-time.After(5 * time.Second):
}
```

To let the peer read everything first, including what the open streams
still write:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := conn.Shutdown(ctx, 0, "")
```

## Errors

```go
_, err := st.Read(buf)
var app *zmux.ApplicationError
switch {
case zmux.IsRetryable(err):
	// the peer did not process the stream: send it again
case errors.Is(err, zmux.ErrIdleTimeout):
	// the session timed out
case errors.Is(err, zmux.ErrResumeTimeout), errors.Is(err, zmux.ErrPipesLost):
	// every pipe was lost (and not resumed in time); errors.Unwrap(err)
	// is the error of the last pipe lost
	log.Println("paths down:", errors.Unwrap(err))
case errors.As(err, &app) && app.Remote:
	// the peer closed the session with app.ErrorCode
case errors.Is(err, net.ErrClosed):
	// the session ended for another reason
}
```

Every session end matches `net.ErrClosed`; `ErrResumeFailed` is a rejoin the
peer refused. `ErrInvalidConfig` is a configuration error, reported before
any byte is written.

## Multipath

Both endpoints set `MaxPipes` above 1 and a join secret; the server uses a
`Listener`. Without a secret source `Dial` and `Start` fail with
`ErrInvalidConfig` before writing a byte. The client adds pipes over other
paths and follows them with `NextEvent`. The secret is `PipeAuth.PSK`, or
`PipeAuth.Exporter`: the exporter of the innermost end-to-end handshake of
the first pipe (`zmux.Exporter`: a carrier transform's own, or TLS that this
process terminates, its handshake done or still running), read when the
peer's parameters arrive. Joins need the first pipe known to end at the
peer: `PipeAuth.EndToEnd` declares it in advance (or `conn.DeclareEndToEnd`
later); `EndToEnd` is no secret source of its own. On a secure first pipe
(the `seal` module) the secure carrier is the source and `PipeAuth` sets
none.
`PipeAuth.DeferredPSK` sends the PSK flag but takes the key later, for
example once the application authenticated the peer over the session:
`conn.SetJoinSecret(psk)` (once, at least 16 bytes) derives the join keys.
Until then `AddPipe` fails with `ErrJoinPending`, whose message says what
is missing, the peer's joins wait at most `JoinTimeout`
(`ConnectionStats.JoinPending`) and the session cannot resume. The peer may hold the same bytes as a static
`PSK`. `ListenerConfig.SessionConfig` picks each new session's `Config`,
for example `Exporter` where `zmux.TerminatedTLS` reports TLS ended here
and `DeferredPSK` otherwise, or its `ShapingLimits`; `Conn.SetShapingLimits`
replaces them while the session runs.

```go
cfg := &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: psk, EndToEnd: true}}
conn, err := zmux.Dial(ctx, "tcp", primaryAddr, cfg)
if err != nil {
	return err
}
nc, err := net.Dial("tcp", backupAddr)
if err != nil {
	return err
}
p, err := conn.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{Label: "backup", SendStandby: new(true)})
if err != nil {
	return err
}
for {
	ev, err := conn.NextEvent(ctx)
	if err != nil {
		return err
	}
	if ev.Kind == zmux.EventPipeFailed && ev.PipeID == p.ID() {
		log.Println("backup path failed")
	}
}
```

The pointer fields of `PipeOptions` (`SendPriority`, `SendStandby`,
`Weight`) are nil to follow the peer's request or the default; a set value,
zero included, wins, so `Weight: new(uint64(0))` excludes the pipe from
spreading. `Direction` makes a pipe an uplink (`DirectionSendOnly`) or a
downlink (`DirectionReceiveOnly`). `Probation` is the base of a pipe's
probation (zero: the session's `Config.SchedulerParams.Probation`, whose
zero derives it from the RTT and the pipe's history; negative: off).
`Pipe.Update` changes a live pipe's settings in one call, each field a
`zmux.Set(v)` or a `zmux.Clear` that returns it to the peer's request or
the session's default:

```go
err := p.Update(zmux.PipeUpdate{SendStandby: zmux.Set(false), Weight: zmux.Clear[uint64]()})
```

`Config.Scheduler` picks how records spread over pipes:
`SchedulerAffinity()`, `SchedulerSessionAffinity()`, `SchedulerSpreadETA()`,
`SchedulerSpreadRoundRobin()`, `SchedulerSpreadRateWeighted()` or
`SchedulerCustom(pick)`. Every pipe has a `PipeID`, which events
(`Event.PipeID`) and statistics name. `Conn.ReplacePipes(ctx)` replaces every
pipe through `Config.Redial`, make before break, after a session secret may
have leaked. On the server, `zmux.AdmittedAs[T](conn)` (or a `*Pipe`)
returns the value the admission attached to the carrier. `PipeStats` reports per-pipe loss:
`BytesLost` and `RecordsLost` when a pipe fails, `BytesRetransmitted` and
`LossRate` from the carrier's own retransmissions; see the godoc for
their exact definitions.

### Reconnecting after an outage

With `ResumeTimeout` a session that loses every pipe suspends instead of
ending, and resumes when a pipe joins again within the grace period. Something
must dial: the `Redial` hook for a session on one pipe at a time, or
`MaintainPipes` with a `PipePool` per path for a session that keeps several.
Both wait out an exponential backoff from 1 s to 60 s
(`RedialMinBackoff`/`RedialMaxBackoff`, `PipePool.MinBackoff`/`MaxBackoff`)
and any join refusal backoff the peer started. A suspended session with
neither reports `EventResumeUnattended` and logs a warning.

```go
cfg := &zmux.Config{
	ResumeTimeout: 30 * time.Second,
	PipeAuth:      zmux.PipeAuth{PSK: psk, EndToEnd: true},
	Redial: func(ctx context.Context, need zmux.Direction) (zmux.PipeIO, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return zmux.PipeIO{}, err
		}
		return zmux.NetPipe(nc), nil
	},
}
conn, err := zmux.Dial(ctx, "tcp", addr, cfg)
if err != nil {
	return err
}
// The server's Listener uses the same ResumeTimeout and PipeAuth. Streams
// survive a carrier that fails and is redialled within 30 s.
```

`Config.Liveness` holds the keep-alive period and the failure-detection
and probe settings; `Conn.SetLiveness` replaces them as a whole while the
session runs (zero fields are the defaults, negative ones off, and
`Conn.Liveness` returns the values in effect). A pipe's own `PipeLiveness`
is set with `PipeOptions.Liveness`, or later with
`Pipe.Update(zmux.PipeUpdate{Liveness: zmux.Set(pl)})`, and dropped with
`zmux.Clear[zmux.PipeLiveness]()`.
A pipe already suspect keeps the dead timeout it turned suspect under.
`Conn.RaiseIncomingStreams` raises the incoming stream limits, for example
after the peer has authenticated; the limits only grow. `zmux.MaxStreamCredit`
(2^60) is the largest stream credit, the value of `MaxIncomingStreams`,
`MaxIncomingUniStreams` and `RaiseIncomingStreams` that grants as many
streams as the protocol counts.

## WebSocket

`zmuxws` carries one zmux record per binary message (`FramingRecords`),
so proxies that re-chunk the byte stream cannot break records. The
subprotocol is the application protocol identifier: `qmux-02` by default,
`qmux-02.<application>` or an application's own identifier, each also
accepted as its draft-01 alias.

```go
// server: upgrades go to l; other requests, and those the admission
// does not accept, to Next
h, err := zmuxws.NewHandler(l, &zmuxhttp.Options{Next: site})
if err != nil {
	return nil, err
}
http.Handle("/zmux", h)

// client
conn, err := zmuxws.Dial(ctx, "wss://example.com/zmux", nil, nil)
```

Nothing is answered before a request is accepted: a request that is not a
WebSocket upgrade, or offers no accepted subprotocol, goes to `Next`, and
the admission decides on the upgrade request's head. A client's
credential travels in the upgrade request: `Dial` presents
`Config.CarrierCredential` and `DialPipe` `DialOptions.CarrierCredential`,
for an `admit.PrefixAuth` in its `Zmux-Prefix-Auth` field, which
`admit.PrefixAuthVerifier.Decide` checks.
`DialPipe` opens a further pipe for `AddPipe`, and `zmuxws.ConnPipe` adapts a
WebSocket connection established otherwise. A close frame ends a pipe
gracefully; an aborted pipe closes the connection without one.

A carrier exposes the TLS this process terminates beneath it
(`ConnectionState`: the client's wss connection, the server's request
TLS, shared by the WebSocket streams of one HTTP/2 connection), so
`PipeAuth.Exporter` works over wss (`zmux.TerminatedTLS`
reports whether a carrier has such TLS). Over ws, or behind a proxy
that terminates TLS, there is none and both fail, unless a carrier
transform with its own handshake supplies the exporter. Only the exporter
binding or a client that pins the server proves the TLS end to end.

## Protected carriers (experimental)

The `seal` module runs a handshake on a session's first carrier and takes the session's
secret from it. Each carrier has a record mode (`PipeOptions.RecordMode`): `RecordSealed`
protects every record end to end where an intermediary terminates the carrier's TLS,
`RecordBound` leaves records to a carrier that protects them and binds its exporter, and
`RecordTrusted` leaves them to the deployment. Further
carriers join the session in any mode the acceptor allows. The wire format and the API are
experimental and may change.

```go
server := &seal.ServerConfig{Gate: gate, Key: nodeKey}
l, err := zmux.NewListener(&zmux.ListenerConfig{WrapPipe: server}) // validates server
if err != nil {
	return nil, err
}
defer func() { _ = l.Close() }()

ctx, cancel := context.WithTimeout(ctx, 10*time.Second) // a refused handshake gets no reply
defer cancel()
p, err := seal.Establish(ctx, carrier, &seal.ClientConfig{
	Anchor: seal.PinAnchor(seal.NodeKeyID(nodePub)),
	Handle: handle,
	PSK:    psk,
})
if err != nil {
	return nil, err
}
conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg)
```

`Gate` decides who may establish on the server and `Anchor` whom the client trusts. An
`Anchor` judges the peer's static key, its one opaque credential and the PSK identity, and
its `VerifyVerdict` can require a transcript signature, which makes the peer post-quantum
authenticated (`RequirePQAuth` insists on it). Both ends can exchange opaque application
data in the handshake through `AppData` callbacks. A server refuses silently, so a refused
client fails with `seal.ErrNoReply`; a rejected peer fails with an error matching `seal.ErrVerifyRejected`,
and a verifier that could not judge with one matching `seal.ErrVerifyUnavailable`.

## HTTP exchanges

Where a long-lived HTTP request does not survive the path (CDNs and
reverse proxies that buffer whole bodies, HTTP/1.0 hops, request duration
limits, caches), an exchange carrier builds one zmux carrier out of a
sequence of short POST requests. The server mounts a handler in front of
its `Listener`:

```go
h, err := zmuxhttp.NewExchangeHandler(l, nil, &zmuxhttp.ExchangeConfig{PSK: psk})
if err != nil {
	return err
}
defer func() { _ = h.Close() }()
http.Handle("/x/", h)
```

and the client opens the carrier with the same PSK:

```go
p, err := zmuxhttp.ExchangePipe(ctx, client, "https://example.com/x/{instance}/{nonce}",
	&zmuxhttp.ExchangeConfig{PSK: psk})
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
a carrier to the same one, for example by hashing `{instance}`. The
default piece size keeps every body at most 1,000,000 bytes after the body
encoding, below the 1 MiB request body limit common on reverse proxies;
each end accepts pieces up to 1 MiB, or its own piece size when larger. A
DOWN body over the client's limit fails the carrier, and an UP body over
the server's is not taken and fails the carrier at the client's deadline.
Requests are numbered, idempotent and
authenticated, so lost, repeated, reordered and cached exchanges are
recovered. Every request carries its send time and a random nonce: the
server takes a request only within `MaxClockSkew` of its own clock and only
once per (carrier ID, nonce), so both ends need roughly synchronized
clocks. Requests it does not take go to `Next` (`http.NotFound` without
one), with their body; `zmuxhttp.ExchangeConfig` documents the timers, sizes and limits and
their defaults. Keep the server's `HoldTime` below every intermediary's
response timeout, and the client transport's `MaxIdleConnsPerHost` at least
`DownRequests+UpRequests`.

The exchange MACs authenticate both ends with the PSK and bind each
response to its request, but they do not encrypt. A PSK is required;
`AllowInsecure` runs the carrier without one (tests and protected
networks), and the carrier ID then is a bearer secret. When TLS is terminated by
a proxy or CDN, that intermediary can read the carrier.

## Behind a reverse proxy or CDN

Which HTTP carriers pass depends on the path: a front that buffers request
bodies passes downlinks but not streaming uplinks, one that buffers whole
bodies passes only the exchange carrier, and some fronts pass WebSocket
only. The server can serve every kind on one target, and the client can
choose among them.

**One endpoint.** `zmuxhttp.Endpoint` classifies each request by its head
(`zmuxhttp.Options.Classify`) and hands it to the adapter of its class;
requests that are not carriers, and those an adapter's admission does not
take, go to the host's handler with their body. The endpoint and its
adapters share one `zmuxhttp.Options`:

```go
o := &zmuxhttp.Options{Next: site, Trusted: trusted}
stream, err := zmuxhttp.NewHandler(l, o)
if err != nil {
	return err
}
ex, err := zmuxhttp.NewExchangeHandler(l, o, &zmuxhttp.ExchangeConfig{PSK: psk})
if err != nil {
	return err
}
ws, err := zmuxws.NewHandler(l, o)
if err != nil {
	return err
}
http.Handle("/zmux", &zmuxhttp.Endpoint{Options: o, Stream: stream, Exchange: ex, WebSocket: ws})
```

Its `WebTransport` field takes a WebTransport handler such as
`zmuxh3.NewHandler` for the WebTransport sessions served on the same target.

**Choosing a carrier.** `DialChoice` takes a `CarrierChoice`: `ExplicitChoice(c)` uses one
kind and reports its failure as it is; `AutoChoice(c...)` tries candidates in
order of preference and keeps the first whose peer answers within a short
timer, without early data and without racing them. With `Config.MaxPipes`
above 1 it establishes the session on the most robust kind (the exchange
carrier unless a kind is known to work) and moves the session to preferred
kinds by joining them as further pipes, without a second session. A
`CarrierCache` in `CarrierChoicePolicy` remembers per server and local network which
kinds worked; a kind that failed, or dropped soon after it was chosen, is
skipped until its backoff expires. Each try and the choice are reported as
`EventCarrierCandidate` and `EventCarrierChosen`, and in `ConnectionStats`.
`CarrierCache.Snapshot` and `Restore` carry what it knows across restarts
as versioned JSON. The moves to preferred kinds are a `PipePool` with a
`Choice`: each member is dialed over the most robust kind available and
rotated to a preferred one, make before break, so `MaintainPipes` can keep
a session's further pipes on the best kind a path passes.

```go
conn, err := zmux.DialChoice(ctx, zmux.AutoChoice(
	zmuxws.Candidate("wss://example.com/zmux", nil),
	zmuxhttp.StreamCandidate(client, "https://example.com/zmux", nil),
	zmuxhttp.SplitCandidate(client, "https://example.com/zmux", nil),
	zmuxhttp.ExchangeCandidate(client, "https://example.com/zmux", &zmuxhttp.ExchangeConfig{PSK: psk}),
), cfg, &zmux.CarrierChoicePolicy{Cache: cache, Key: "example.com|" + network})
```

`zmuxquic.Candidate` and `zmuxh3.Candidate` (WebTransport) add the kinds a
client can use when it reaches the server directly, and
`zmuxh3.HTTP3Candidate` a streaming request over HTTP/3. A carrier kind
describes only this end's hop, not what lies beyond its first peer.

**Client addresses.** Behind a proxy every carrier comes from the proxy's
address. `TrustedProxies` names the proxies whose report of the client
address is believed: its `Listener` wraps a `net.Listener` that receives
the PROXY protocol from them, and `zmuxhttp.Options.Trusted` reads the
forwarding header field they set.
The reported address is then the source that admissions see and that the
listener's per-source limits count; the same report from any other peer is
ignored.

```go
trusted := &zmux.TrustedProxies{Prefixes: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
	Header: "X-Forwarded-For"}
```

**TLS at the proxy.** The server then sees no TLS: prefix tokens are
unbound, joins need `PipeAuth.PSK`, and the exchange carrier can be read by
the proxy. Keep the hop from the proxy to the server local or encrypted.

**Proxy configuration.** A location that passes every kind, in nginx
syntax:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      '';
}

location /zmux {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;          # WebSocket
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_request_buffering off;   # stream uplinks as they arrive
    proxy_buffering off;           # downlinks also send X-Accel-Buffering: no
    proxy_read_timeout 300s;       # above the session's keep-alive
    proxy_send_timeout 300s;
    client_max_body_size 1m;       # exchange bodies stay within 1,000,000 bytes
}
```

- Full duplex: a bidirectional request carrier needs both bodies forwarded
  at once, which an HTTP/1.1 hop does not guarantee. Pass it over HTTP/2
  without TLS (`grpc_pass grpc://127.0.0.1:8080;` in a server whose
  listener has `http2 on`), and let the server accept HTTP/2 by prior
  knowledge:

  ```go
  srv := &http.Server{Addr: "127.0.0.1:8080", Handler: endpoint, Protocols: new(http.Protocols)}
  srv.Protocols.SetHTTP1(true)
  srv.Protocols.SetUnencryptedHTTP2(true)
  ```

- Early data: keep `ssl_early_data off`, or forward its marker with
  `proxy_set_header Early-Data $ssl_early_data;`: the adapters answer a
  carrier request that carries `Early-Data: 1` with `425 (Too Early)`, and
  the proxy repeats it after its handshake. An HTTP/3 server with
  `quic.Config.Allow0RTT` hands requests over before the handshake
  completes; the adapters answer those carrier requests 425 as well.
- Idle limits: proxies end requests that stay silent (60 s by default for
  the read and send timeouts, 30 s to 125 s for a CDN's response): keep
  `Config.Liveness.KeepAlivePeriod` (15 s by default, at most half the idle timeout; negative turns it off)
  and, with multipath, the probe interval below the shortest limit on the
  path, and the
  exchange server's `HoldTime` below the CDN's response timeout.

## Admission

An `Admission` is configured on the listener (`ListenerConfig.Admission`)
and decides on every carrier of it before anything is sent on it,
whatever entry point took it: `Listener.Serve` and `HandlePipe`,
`zmuxhttp.NewHandler`, the exchange handler, `zmuxws.NewHandler`,
`zmuxh3.NewHandler` and `zmuxquic.Serve`. Every entry runs the same steps: the carrier takes a
slot of the listener (`MaxPending`) and of its source (`MaxPendingPerSource`) before its outer handshake
(TLS, an HTTP upgrade, QUIC), the admission reads up to `Peek` bytes (on a
message-framed carrier its first message) and
calls `Decide` with them and the carrier's metadata (`CarrierInfo`: its
`Source` and `Peer`, TLS state, HTTP request), then `WrapPipe` runs and the
carrier is classified. `Decide` gets a context that ends with what remains
of `PeekTimeout` and when the listener stops. Its `AdmissionVerdict` accepts the carrier
(without the bytes it skips), hands it with every byte read to the
fallback of its kind (`ListenerConfig.Fallback` for byte streams,
`zmuxhttp.Options.Next` for HTTP, the `fallback` of `zmuxquic.Serve`), stalls
it, or closes it; `VerdictMore` reads a longer prefix (up to `MaxPeek`)
and decides again, and `Decision.Variant` labels the carrier, counted per
label in `Listener.VariantStats` (one of a few host constants, never derived
from the peer's bytes; past `MaxAdmissionVariants` labels share one bucket). A source holds at most `MaxPendingPerSource - 1` stalled
carriers, and a stall lasts the same duration for every carrier from one
source during the listener's lifetime (`Listener.StallDuration`). A source
is a client address aggregated to its prefix, `SourcePrefixV4` bits for IPv4
(default 32, the address on its own) and `SourcePrefixV6` bits for IPv6
(default 64), ports ignored; the slots, the stalled-carrier count and the
stall duration all use it.
`ListenerStats.Admission` counts the outcomes. An adapter with its own
accept loop takes a carrier with `Listener.Arrive` (`AdmitUpgrade` for an
HTTP upgrade) and hands it on with `Arrival.HandlePipe`. `admit.PrefixAuth` is one `Decide`: a carrier starts with a token of
`admit.PrefixTokenLen` bytes, optionally bound to its TLS handshake; a carrier
decided on its request head (a WebSocket or WebTransport upgrade, an HTTP
request carrier without a body such as a downlink `GET`) carries it in
the `Zmux-Prefix-Auth` request field instead (`admit.HeaderPrefixAuth`, 43
characters of base64url). The same `Admission` serves both:

```go
auth := &admit.PrefixAuth{Key: key, Slot: 30 * time.Second, Skew: 1, BindTLS: true}
v, err := admit.NewPrefixAuthVerifier(auth)
if err != nil {
	return nil, err
}
l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg,
	Admission: &zmux.Admission{Peek: admit.PrefixTokenLen, Decide: v.Decide}, Fallback: fallback})
if err != nil {
	return nil, err
}
go func() { _ = l.Serve(ctx, tls.NewListener(ln, tlsConf)) }()

// client: Config.CarrierCredential is presented by the dialers
conn, err := zmux.DialTLS(ctx, "tcp", addr, tlsConf, &zmux.Config{CarrierCredential: auth})
```

A `CarrierCredential` is the client side of an admission: `Prefix` gives the
first bytes of a carrier, `Header` the request field of a carrier decided
on its request head, each
from the carrier's metadata (`CarrierInfo`, with its TLS state for a
binding). `Client`, `Dial`, `DialTLS`, `zmuxquic.Dial` and `Client`,
`zmuxws.Dial` and `zmuxh3.Dial` present `Config.CarrierCredential`; the pipe
dialers for joins take theirs in `zmuxws.DialOptions.CarrierCredential`,
`zmuxquic.NewPool`, `zmuxh3.NewPool` and `zmuxh3.DialSession`;
`zmuxhttp.StreamPipe` and `zmuxhttp.SplitPipe` take one (unbound: the request's connection
is not known in advance), and `PresentPipe` presents one on any other
byte-stream pipe.

A split bootstrap (`zmuxhttp.SplitPipe`, `zmuxhttp.SplitCandidate`) holds its first half
in a pairing slot until the other half arrives or the handshake timeout
ends: 64 slots per listener and 8 per source ([pairing a split bootstrap](https://github.com/zmuxio/zmux-spec/blob/main/SPEC.md#611-pairing-a-split-bootstrap)), counted by
the source that `MaxPendingPerSource` counts (a client by its source prefix).
Without an `Admission`, a few sources can keep those slots full and delay
other clients' split bootstraps; serve a split bootstrap that the open
Internet reaches with an `Admission`, such as `admit.PrefixAuth`.


## Carrier transforms

A transform (compression, a byte transform, a handshake of its own) wraps
a carrier on both ends with the public API alone: the client in a
`Candidate`'s `Dial` (or before `Start`, `AddPipe`), the server in
`ListenerConfig.WrapPipe`, after an `Admission` that tells its first bytes
from a zmux first record (`admit.ClassifyFirstBytes`, `VerdictMore`,
`Decision.Variant`). A batch of records is one write on the wrapped half
(one `WriteMessages` on a message-framed `MessageBatchWriter`). The halves
offer capabilities as interfaces (`CloseWriter`, `Aborter`,
`ConnectionStater`, `KeyingMaterialExporter`, `EarlyClientCarrier`, ...); a transform forwards a lower one only where its
meaning survives. A transform whose first message carries the client's
first flight uses `EarlyData.HoldFirstFlight` with `Conn.ReleaseFirstFlight`
and reports the server's verdict as carrier early data
(`EarlyClientCarrier.SendsEarlyData`); `admit.ReplayFilter` is the single-use
cache for its nonces, checked only after the nonce and its timestamp were
authenticated together. `PipeIO.Variant` labels the carriers
in events, statistics, shaping and `CarrierCache` (with
`CarrierCache.Report` for outcomes the host learns later).
[examples/xorpipe](examples/xorpipe) is a complete example.

## QUIC

```go
conn, err := zmuxquic.Dial(ctx, "example.com:4433", tlsConf, nil, nil)
```

On the server, `zmuxquic.Serve(ctx, l, ln, nil)` hands the streams of a QUIC
listener to a `zmux.Listener`; `zmuxquic.ServeConn` serves one QUIC
connection the caller already accepted or dialled, such as one made by hole
punching. `zmuxh3.Dial` and `zmuxh3.Serve` do the same for WebTransport
sessions.

The default dialer of `zmuxquic.Dial` and `NewPool` binds `udp4` or `udp6`
by the destination's address family. Native QUIC 0-RTT streams are
currently created only after the handshake completes; zmux's own
replayable startup and resumption are not affected. A `quic.Conn` passed
to `zmuxquic.Client` or `zmuxquic.ConnPipe` stays owned by the caller.
`zmuxh3.SessionPipe` and `zmuxh3.StreamPipe` do the same for a WebTransport
session or stream.

A QUIC close discards stream data the peer has not acknowledged, so a QUIC
connection is closed only after the zmux close on its streams was read or
given up. The connections `zmuxquic.Dial`, `NewPool` and `Candidate` open
(and the WebTransport sessions of `zmuxh3.NewPool` and `Candidate`) close
once none of their pipes has not ended, each pipe's inbound side having
ended or lingered out, with the application error code of the last zmux
session they carried (0 after a transport error, an idle timeout or no
session). A caller that owns the connection or session closes it after
`Wait` returns (or `Done` is closed), with the code of the
`*zmux.ApplicationError` that `Wait` returned; `zmuxh3.SessionCloseCode`
gives it for a WebTransport session.

## Datagram queues

Received datagrams wait in bounded queues, the oldest dropped and counted
when one is full: the session queue (`DatagramReceiveQueue`, 128) and the
datagrams held for streams not opened yet share `DatagramReceiveBytes`
(1 MiB); each stream's queue has its own count (`DatagramStreamQueue`, 32)
and payload bytes (`DatagramStreamBytes`, 512 KiB; negative: no byte
bound), a single larger datagram being kept alone. An application that
forwards packets at a high rate raises both.

## Datagram channels

QUIC, HTTP/3 and WebTransport carriers have an unreliable datagram service
of their own besides the stream that is the pipe. When both endpoints
enable datagrams and support it, a datagram that fits is sent there
instead of in a record, so a lost packet loses that datagram rather than
delaying every later one behind the stream's retransmission. Nothing
changes for the application: the same calls, sizes and queues; larger
datagrams, pipes whose records are shaped, and peers without the channel
use records. `ConnectionStats` counts the datagrams that went on channels
(`DatagramsSentChannel`, `DatagramsReceivedChannel`), and
`zmuxquic.ChannelDropped` and `zmuxh3.ChannelDropped` the datagrams that
named no attached pipe.

- QUIC: the connection's QUIC datagrams, each naming its pipe by stream
  ID, so several pipes and sessions share a connection. `zmuxquic.Config`
  enables them; a connection that carries pipes leaves its datagrams to
  zmux.
- WebTransport: the session's datagrams, named the same way.
- HTTP/3: the HTTP Datagrams of a bidirectional carrier's request stream.
  `zmuxh3.NewServer` enables them (`zmuxh3.HTTP3Handler` wraps the handler
  of any other HTTP/3 server); both take the `zmuxhttp.Options` of the zmux
  handler. Pipes opened with an `http.Client`, as `zmuxh3.HTTP3Candidate`
  does, keep datagrams in records.

A carrier of another kind offers its own channel by implementing
`zmux.DatagramChannel` on a half of its `PipeIO`; `carrier.ChannelDemux`
routes the datagrams of a connection shared by several pipes.

## Shaping

`Config.Shaper`, `Config.HandshakeShaper`, `Config.ShapingLimits` and
`Config.ShapingBudget` take hooks and bounds for the sizes and timing of what a session writes, and `PipePool.Policy`,
`Config.RedialPolicy` and `carrier.Pool.Policy` a pool's decisions (packages `shaping` and `shaping/machine`,
`shaping/bundle`).

## More

The package documentation and its examples cover datagrams, stream
preambles and priorities, early data and its retry, splicing streams
between sessions, lanes, nested sessions and the scheduler. The protocol
is specified in the [zmux specification repository](https://github.com/zmuxio/zmux-spec).

`zmuxtest` is experimental test support (a misbehaving reverse proxy and a
carrier-cutting relay), outside the API stability promise.

## Testing

```
go test -race ./...
```

Run it in each module directory. The root `go.work` makes the sub-modules
build against the local root module; see RELEASING.md for the release
order.

## License

Apache-2.0
