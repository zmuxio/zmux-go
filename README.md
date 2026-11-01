# zmux-go

[![CI](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zmuxio/zmux-go.svg)](https://pkg.go.dev/github.com/zmuxio/zmux-go)
[![Release](https://img.shields.io/github/v/tag/zmuxio/zmux-go?filter=v*&label=release)](https://github.com/zmuxio/zmux-go/tags)
[![Go version](https://img.shields.io/github/go-mod/go-version/zmuxio/zmux-go)](go.mod)
[![License](https://img.shields.io/github/license/zmuxio/zmux-go)](LICENSE)

[![zmuxquic](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxquic%2F%40latest&query=%24.Version&label=zmuxquic&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxquic)
[![zmuxh3](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxh3%2F%40latest&query=%24.Version&label=zmuxh3&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxh3)
[![zmuxws](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fzmuxio%2Fzmux-go%2Fzmuxws%2F%40latest&query=%24.Version&label=zmuxws&color=blue)](https://pkg.go.dev/github.com/zmuxio/zmux-go/zmuxws)

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
| `github.com/zmuxio/zmux-go` | core protocol, TCP/TLS/HTTP carriers, any message carrier |
| `github.com/zmuxio/zmux-go/zmuxquic` | QUIC carriers (quic-go) |
| `github.com/zmuxio/zmux-go/zmuxh3` | HTTP/3 and WebTransport carriers |
| `github.com/zmuxio/zmux-go/zmuxws` | WebSocket carriers |

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

`l.Shutdown(ctx, 0, "")` stops accepting new sessions, ends every session
gracefully while still taking their joins and resumptions, and then
closes the listener; `l.Close()` refuses every carrier at once. `zmux.Server` serves a single connection without multipath.

In `Config` and the other configuration structs, the zero value of a field
selects its stated default, a negative timeout means no timeout, other
negative values are invalid unless the field gives them a meaning, and the
`Disable` and `No` fields switch features off.

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
p, err := conn.AddPipe(ctx, zmux.NetPipe(nc), zmux.PipeOptions{Label: "backup", SendStandby: new(true)})
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

The pointer fields of `PipeOptions` (`SendPriority`, `SendStandby`,
`Weight`) are nil to follow the peer's request or the default; a set value,
zero included, wins, so `Weight: new(uint64(0))` excludes the pipe from
spreading. `Probation` is the base of a pipe's probation (zero: the
session's `Config.SchedulerParams.Probation`, whose zero derives it from
the RTT and the pipe's history); `NoProbation` turns it off, and both
exist in `SchedulerParams` too. `PipeStats` reports per-pipe loss:
`BytesLost` and `RecordsLost` when a pipe fails, `BytesRetransmitted` and
`LossRate` from the carrier's own retransmissions; see the godoc for
their exact definitions.

## WebSocket

`zmuxws` carries one zmux record per binary message (`FramingRecords`),
so proxies that re-chunk the byte stream cannot break records. The
subprotocol is the application protocol identifier: `qmux-02` by default,
`qmux-02.<application>` or an application's own identifier, each also
accepted as its draft-01 alias.

```go
// server: upgrades go to l; other requests, and those the admission
// does not accept, to Next
h, err := zmuxws.Handler(l, &zmux.HTTPOptions{Next: site})
http.Handle("/zmux", h)

// client
conn, err := zmuxws.Dial(ctx, "wss://example.com/zmux", nil, nil)
```

Nothing is answered before a request is accepted: a request that is not a
WebSocket upgrade, or offers no accepted subprotocol, goes to `Next`, and
the admission decides on the upgrade request's head. A client's
credential travels in the upgrade request: `Dial` presents
`Config.Credential` and `DialPipe` `DialOptions.Credential`, for a
`PrefixAuth` in its `Zmux-Prefix-Auth` field, which
`PrefixAuthVerifier.Decide` checks.
`DialPipe` opens a further pipe, with the `Zmux-Pipe-Join` field of a
`Reservation` for an out-of-band join, and `zmuxws.Pipe` adapts a
WebSocket connection established otherwise. A close frame ends a pipe
gracefully; an aborted pipe closes the connection without one.

## HTTP exchanges

Where a long-lived HTTP request does not survive the path (CDNs and
reverse proxies that buffer whole bodies, HTTP/1.0 hops, request duration
limits, caches), an exchange carrier builds one zmux carrier out of a
sequence of short POST requests. The server mounts a handler in front of
its `Listener`:

```go
h, err := zmux.NewHTTPExchangeHandler(l, &zmux.HTTPExchangeConfig{PSK: psk})
if err != nil {
	return err
}
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
one), with their body; `HTTPExchangeConfig` documents the timers, sizes and limits and
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

**One endpoint.** `HTTPEndpoint` classifies each request by its head
(`ClassifyHTTP`) and hands it to the adapter of its class; requests that
are not carriers, and those an adapter's admission does not take, go to
the host's handler with their body:

```go
stream, err := zmux.NewHTTPHandler(l, &zmux.HTTPOptions{Next: site, Trusted: trusted})
ex, err := zmux.NewHTTPExchangeHandler(l, &zmux.HTTPExchangeConfig{PSK: psk, Next: site, Trusted: trusted})
ws, err := zmuxws.Handler(l, &zmux.HTTPOptions{Next: site, Trusted: trusted})
http.Handle("/zmux", &zmux.HTTPEndpoint{Stream: stream, Exchange: ex, WebSocket: ws, Next: site})
```

Its `WebTransport` field takes a WebTransport handler such as
`zmuxh3.Handler` for the WebTransport sessions served on the same target.

**Choosing a carrier.** `DialMode` takes a mode: `ExplicitMode(c)` uses one
kind and reports its failure as it is; `AutoMode(c...)` tries candidates in
order of preference and keeps the first whose peer answers within a short
timer, without early data and without racing them. With `Config.MaxPipes`
above 1 it establishes the session on the most robust kind (the exchange
carrier unless a kind is known to work) and moves the session to preferred
kinds by joining them as further pipes, without a second session. A
`CarrierCache` in `ModePolicy` remembers per server and local network which
kinds worked; a kind that failed, or dropped soon after it was chosen, is
skipped until its backoff expires. Each try and the choice are reported as
`EventCarrierCandidate` and `EventCarrierChosen`, and in `ConnectionStats`.
`CarrierCache.Snapshot` and `Restore` carry what it knows across restarts
as versioned JSON. The moves to preferred kinds are a `PipePool` with a
`Mode`: each member is dialed over the most robust kind available and
rotated to a preferred one, make before break, so `MaintainPipes` can keep
a session's further pipes on the best kind a path passes.

```go
conn, err := zmux.DialMode(ctx, zmux.AutoMode(
	zmuxws.Candidate("wss://example.com/zmux", nil),
	zmux.StreamingCandidate(client, "https://example.com/zmux", nil),
	zmux.SplitCandidate(client, "https://example.com/zmux", nil),
	zmux.ExchangeCandidate(client, "https://example.com/zmux", &zmux.HTTPExchangeConfig{PSK: psk}),
), cfg, &zmux.ModePolicy{Cache: cache, Key: "example.com|" + network})
```

`zmuxquic.Candidate` and `zmuxh3.WebTransportCandidate` add the kinds a
client can use when it reaches the server directly, and
`zmuxh3.HTTP3Candidate` a streaming request over HTTP/3. A carrier kind
describes only this end's hop, not what lies beyond its first peer.

**Client addresses.** Behind a proxy every carrier comes from the proxy's
address. `TrustedProxies` names the proxies whose report of the client
address is believed: its `Listener` wraps a `net.Listener` that receives
the PROXY protocol from them, and `HTTPOptions.Trusted` (and
`HTTPExchangeConfig.Trusted`) reads the forwarding header field they set.
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
  `Config.KeepAlivePeriod` (15 s by default, at most half the idle timeout; negative turns it off)
  and, with multipath, the probe interval below the shortest limit on the
  path, and the
  exchange server's `HoldTime` below the CDN's response timeout.

## Admission

An `Admission` is configured on the listener (`ListenerConfig.Admission`)
and decides on every carrier of it before anything is sent on it,
whatever entry point took it: `Listener.Serve` and `HandlePipe`,
`NewHTTPHandler`, the exchange handler, `zmuxws.Handler`, `zmuxh3.Handler`
and `zmuxquic.Serve`. Every entry runs the same steps: the carrier takes a
slot of its source (`MaxPendingPerSource`) before its outer handshake
(TLS, an HTTP upgrade, QUIC), the admission reads up to `Peek` bytes and
calls `Decide` with them and the carrier's metadata (`CarrierInfo`: its
`Source` and `Peer`, TLS state, HTTP request), then `WrapPipe` runs and the
carrier is classified. `Decide` gets a context that ends with what remains
of `PeekTimeout` and when the listener stops. It accepts the carrier
(without the bytes it skips), hands it with every byte read to the
fallback of its kind (`ListenerConfig.Fallback` for byte streams,
`HTTPOptions.Next` for HTTP, the `fallback` of `zmuxquic.Serve`), stalls
it, or closes it. A source holds at most `MaxPendingPerSource - 1` stalled
carriers, and a stall lasts the same duration for every carrier from one
source during the listener's lifetime (`Listener.StallDuration`).
`ListenerStats.Admission` counts the outcomes. An adapter with its own
accept loop takes a carrier with `Listener.Arrive` (`AdmitUpgrade` for an
HTTP upgrade) and hands it on with `Arrival.HandlePipe`. `PrefixAuth` is one `Decide`: a carrier starts with a token of
`PrefixTokenLen` bytes, optionally bound to its TLS handshake; a carrier
decided on its request head (a WebSocket or WebTransport upgrade, an HTTP
request carrier without a body such as a downlink `GET`) carries it in
the `Zmux-Prefix-Auth` request field instead (`HeaderPrefixAuth`, 43
characters of base64url). The same `Admission` serves both:

```go
auth := &zmux.PrefixAuth{Key: key, Slot: 30 * time.Second, Skew: 1, BindTLS: true}
v, err := zmux.NewPrefixAuthVerifier(auth)
l := zmux.NewListener(&zmux.ListenerConfig{Config: cfg,
	Admission: &zmux.Admission{Peek: zmux.PrefixTokenLen, Decide: v.Decide}, Fallback: fallback})
go l.Serve(ctx, tls.NewListener(ln, tlsConf))

// client: Config.Credential is presented by the dialers
conn, err := zmux.DialTLS(ctx, "tcp", addr, tlsConf, &zmux.Config{Credential: auth})
```

A `Credential` is the client side of an admission: `Prefix` gives the
first bytes of a carrier, `Header` the request field of a carrier decided
on its request head, each
from the carrier's metadata (`CarrierInfo`, with its TLS state for a
binding). `Client`, `Dial`, `DialTLS`, `zmuxquic.Dial` and `Client`,
`zmuxws.Dial` and `zmuxh3.Dial` present `Config.Credential`; the pipe
dialers for joins take theirs in `zmuxws.DialOptions.Credential`,
`zmuxquic.NewPool`, `zmuxh3.NewPool` and `zmuxh3.DialSession`;
`HTTPPipe` and `HTTPSplitPipe` take one (unbound: the request's connection
is not known in advance), and `PresentPipe` presents one on any other
byte-stream pipe.

A split bootstrap (`HTTPSplitPipe`, `SplitCandidate`) holds its first half
in a pairing slot until the other half arrives or the handshake timeout
ends: 64 slots per listener and 8 per source ([SPEC.md 6.1.1](https://github.com/zmuxio/zmux-spec/blob/main/SPEC.md#611-pairing-a-split-bootstrap)), counted by
the source that `MaxPendingPerSource` counts (an IPv6 client by its /64).
Without an `Admission`, a few sources can keep those slots full and delay
other clients' split bootstraps; serve a split bootstrap that the open
Internet reaches with an `Admission`, such as `PrefixAuth`.

`ListenerConfig.AnswerUnknownJoins` (off by default) answers an admitted
join for a session this instance does not hold with `ZMUX_RESUME_FAILED`,
so a suspended client fails at once instead of at its resume timeout.
Leave it off where a load balancer can route a join to another instance
with the same `InstanceID`.

## QUIC

```go
conn, err := zmuxquic.Dial(ctx, "example.com:4433", tlsConf, nil, nil)
```

On the server, `zmuxquic.Serve(ctx, ln, l, nil)` hands the streams of a QUIC
listener to a `zmux.Listener`; `zmuxquic.ServeConn` serves one QUIC
connection the caller already accepted or dialled, such as one made by hole
punching. `zmuxh3.Dial` and `zmuxh3.Serve` do the same for WebTransport
sessions.

The default dialer of `zmuxquic.Dial` and `NewPool` binds `udp4` or `udp6`
by the destination's address family. Native QUIC 0-RTT streams are
currently created only after the handshake completes; zmux's own
replayable startup and resumption are not affected. A `quic.Conn` passed
to `Client` or `OpenPipe` stays owned by the caller.

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
`zmuxquic.ChannelDropped` the QUIC datagrams that named no attached pipe.

- QUIC: the connection's QUIC datagrams, each naming its pipe by stream
  ID, so several pipes and sessions share a connection. `QUICConfig`
  enables them; a connection that carries pipes leaves its datagrams to
  zmux.
- WebTransport: the session's datagrams, named the same way.
- HTTP/3: the HTTP Datagrams of a bidirectional carrier's request stream.
  `NewHTTP3Server` enables them (`HTTP3Handler` wraps the handler of any
  other HTTP/3 server). Pipes opened with an `http.Client`, as
  `HTTP3Candidate` does, keep datagrams in records.

A carrier of another kind offers its own channel by implementing
`zmux.DatagramChannel` on a half of its `PipeIO`; `zmux.ChannelDemux`
routes the datagrams of a connection shared by several pipes.

## Pool policies

`PipePool.Policy` and `CarrierPool.Policy` take a `zmux.PoolPolicy`.
`machine.PoolPolicy` (package `shaping/machine`) builds one from JSON:

```json
{
  "count": {"const": 2},
  "dial_after": [{"const": 1}, {"range": [5, 10]}],
  "retire": [{"metric": "age", "above": {"range": [1800, 3600], "scope": "pipe"}}]
}
```

Every field is optional; an absent field keeps the pool's own value, and
an unknown field or metric fails construction. Values have the form and
validation of definition values; times are in seconds.

- `count` is rounded; below 1 it is 1.
- `dial_after[k]` is the delay after the last dial when `k` dials in a
  row failed; the last entry applies to any higher number.
- `dial_jitter`, 0 to below 1, shortens each `dial_after` wait by a random
  fraction of up to it; it needs `dial_after`.
- `retire` lists a member when one rule's metric is strictly greater than
  its threshold; members are listed in view order, each once. Metrics:
  `age`, `idle` (seconds), `pipes`, `bytes_sent`, `bytes_received`,
  `bytes` (their sum). A rule's `jitter`, 0 to below 1, lowers its
  threshold by a random fraction of up to it, drawn once per member.
- `probe_interval` is the probe interval of the pipes the pool dials; 0
  turns their idle probes off. `probe_jitter`, 0 to below 1, is their probe
  jitter and needs `probe_interval`.
- Scopes: `each` (or none) draws at every decision, `shared` once per
  policy, `pipe` once per member ID while the member is in the view.
  `pipe` is accepted only in `retire` thresholds.

`Recheck` is set to the earliest time at which an `age` or `idle` rule
would hold for a member not retired, or at which `dial_after` after the
last dial elapses; 0 when there is neither. Byte and pipe rules do not set
it. Pools decide again no later than `Recheck` (at least
`zmux.MinRecheck`, 10 ms) in addition to their own triggers.

`Options.NewSource` sets the random source; `Decide` does not block.

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
