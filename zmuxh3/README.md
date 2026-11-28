# HTTP/3 and WebTransport carriers

`zmuxh3` carries zmux over HTTP/3 requests or over a WebTransport session. A
WebTransport bidirectional stream carries one zmux pipe, and unidirectional
streams carry its lanes. Native datagrams use the WebTransport session's
HTTP/3 Datagram channel. Ordinary HTTP/3 request carriers send datagrams
through zmux records.

## Serve WebTransport

Construct a `zmuxh3.Server` with an `http3.Server` that supplies TLS and the
HTTP handler. Install the handler returned by `zmuxh3.NewHandler` on the
HTTP/3 server, then call `Server.Serve` with your UDP socket. The caller
closes the socket. Call `Server.Close` to stop accepting and close the
server's active QUIC connections.

```go
server := &zmuxh3.Server{H3: &http3.Server{TLSConfig: serverTLS}}
handler, err := zmuxh3.NewHandler(listener, options, server)
if err != nil {
    return err
}
server.H3.Handler = handler
defer server.Close()
return server.Serve(udpSocket)
```

Mount the handler within your HTTP router or supply an HTTP fallback via
`zmuxhttp.Options.Next`. The default origin policy accepts native clients
that omit `Origin` and browser requests with a matching HTTPS origin. Use
`Server.CheckOrigin` to configure the origins permitted by your application.
For a caller-managed QUIC listener, configure ALPN `h3` and enable QUIC
datagrams, then dispatch each connection through `Server.ServeQUICConn`.

For a host-managed CONNECT handler, call `Server.Upgrade` and pass the
accepted session to `zmuxh3.Serve`. Use `NewHandler` when the listener should
decide admission before upgrading and may defer its decision to the first
bidirectional stream.

## Dial WebTransport

Create a `zmuxh3.Transport` with your client TLS settings. Use
`zmuxh3.Dial` to start a zmux session on the first pipe. Use
`zmuxh3.DialSession` and `zmuxh3.SessionPipe` to manage the WebTransport
session and its pipes yourself, or use `NewConnPool` or
`WebTransportCandidate` for managed pipes. Each QUIC connection carries
one WebTransport session. Several zmux pipes can share that session.

```go
transport := &zmuxh3.Transport{TLSClientConfig: clientTLS}
defer transport.Close()
conn, session, err := zmuxh3.Dial(ctx, transport, targetURL, headers, config)
if err != nil {
    return err
}
// Use conn, then wait for all zmux pipes on session to finish.
err = conn.Wait()
code, reason := zmuxh3.SessionCloseCode(err)
return session.CloseWithError(code, reason)
```

`DialSession` owns the QUIC connection through session shutdown.
`Transport.NewClientConn` binds a caller-owned connection to an HTTP/3
driver. Use one binding and leave its incoming QUIC streams to that driver.
Canceling a CONNECT attempt ends that request. A borrowed QUIC connection
remains under the caller's control. A refused CONNECT returns
`*zmuxh3.ConnectError` with its final HTTP status.

The adapter sends the draft-16 `SETTINGS_WT_ENABLED` and advertises the
historical WT settings for peers that use them to discover support. It
sends the `webtransport-h3` CONNECT token and also accepts the earlier
`webtransport` token on incoming CONNECT requests. Both endpoints need HTTP/3 and QUIC
datagrams. Dials use one CONNECT attempt with the draft-16 token, so a
server that accepts only the legacy token cannot be dialed. Native
datagrams always use the RFC 9297 Quarter Stream ID. Clients that use a
full-ID prefix can exchange native datagrams only on CONNECT ID zero.
The adapter uses partial stream resets where negotiated, and ordinary
RESET_STREAM and STOP_SENDING otherwise.

`Session.Context()` ends with a cause. A `*zmuxh3.SessionError` carries the
application code, reason and initiating side. `Session.Draining()` signals
that the peer has asked for graceful shutdown. Finish the zmux pipes before
closing their WebTransport session.

For a host takeover, use `Takeover.Pipe()` for byte replay and half-close.
Its write half's `Unwrap() io.WriteCloser` gives the original WT writer
and its `zmux.DatagramChannel`. Use that channel for pipe datagrams, or
obtain the `*Session` from `CarrierConn()` to handle session datagrams
directly. Only one receiver should own the session's datagram queue.
Use `Detach` before keeping the native endpoint past the fallback call.

## HTTP/3 request carriers

Use `HTTPClient` for the client side and close its returned transport when
done. `HTTP3Candidate` wraps a streaming HTTP client for a session
controller. `NewServer` serves an HTTP handler over HTTP/3, including a
`zmuxhttp` streaming or Split handler. If you already own an HTTP/3 server,
set `ConnContext` to `QUICConnContext` and wrap the handler with
`HTTP3Handler` to publish connection samples.
