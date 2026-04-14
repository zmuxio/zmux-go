# quicmux

`quicmux` wraps `quic-go` connections behind the stable `zmux.Session` API.

It is an adapter. It does not implement zmux-native `NativeSession` or native stream interfaces.

## Usage

```go
import (
	quic "github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/adapter/quicmux"
)

var qconn *quic.Conn
session := quicmux.WrapSession(qconn)
```

Use `WrapSessionWithOptions` when you need adapter-local tuning:

```go
session := quicmux.WrapSessionWithOptions(qconn, quicmux.SessionOptions{
	AcceptedPreludeReadTimeout: 2 * time.Second,
})
```

`AcceptedPreludeReadTimeout`:

- `0`: use `quicmux.DefaultAcceptedPreludeReadTimeout`
- `< 0`: disable the adapter-managed timeout

## Stable API Coverage

The wrapper returns the stable `zmux.Session` surface, so callers use the same methods as other adapters or native
`AsSession(...)` values:

- `AcceptStream` / `AcceptUniStream`
- `OpenStream` / `OpenUniStream`
- `OpenStreamWithOptions` / `OpenUniStreamWithOptions`
- `OpenAndSend` / `OpenUniAndSend`
- `Close`, `CloseWithError`, `Wait`, `Closed`, `State`, `Stats`

Wrapped streams expose the stable `zmux.Stream`, `zmux.SendStream`, and `zmux.RecvStream` methods:

- `StreamID`, `OpenInfo`, `Metadata`, `UpdateMetadata`
- `WriteFinal`, `WritevFinal`
- `CloseRead`, `CancelRead`
- `CloseWrite`, `CancelWrite`
- `CloseWithError`
- deadlines on the matching stream shapes

## Mapping

- `AcceptStream` / `OpenStream` map to QUIC bidirectional streams
- `AcceptUniStream` / `OpenUniStream` map to QUIC unidirectional streams
- `CloseRead` maps to QUIC `CancelRead(CANCELLED)`
- `CancelRead(code)` maps to QUIC `CancelRead(code)`
- `CloseWrite` maps to QUIC send-side `Close`
- `CancelWrite(code)` maps to QUIC `CancelWrite(code)`
- `CloseWithError(code, reason)` is best-effort: bidirectional streams cancel both local directions; unidirectional
  streams cancel only the locally meaningful direction QUIC exposes
- `Closed()` reports whether the wrapped QUIC connection context has terminated
- `Accept*` and `Open*` treat `nil` contexts as `context.Background()`

## Metadata Support

`quicmux` carries zmux open-time metadata in a small stream prelude:

- prelude format: `varint(metadata_len) + TLV...`
- `OpenStreamWithOptions` and `OpenUniStreamWithOptions` support `OpenInfo`, `InitialPriority`, and `InitialGroup`
- `OpenInfo()` and `Metadata()` expose decoded opener metadata on accepted streams
- `UpdateMetadata(...)` works only before the prelude is emitted

Once the prelude is emitted, further metadata updates are not representable on the QUIC wire. The adapter returns
`errors.Join(zmux.ErrAdapterUnsupported, zmux.ErrPriorityUpdateUnavailable)`.

## Errors

- QUIC stream and connection application errors are normalized to `*zmux.ApplicationError`
- QUIC stream-limit errors are normalized to `zmux.ErrOpenLimited`
- QUIC idle, stateless-reset, version-negotiation, and local no-error close conditions are normalized to
  `zmux.ErrSessionClosed`
- connection-level QUIC closes preserve code and reason text
- stream-level QUIC cancellations preserve only the numeric application code

## Unsupported Or Reduced Behavior

- native zmux session helpers such as `Ping`, `GoAway`, `PeerGoAwayError`, `PeerCloseError`, `LocalPreface`,
  `PeerPreface`, and `Negotiated`
- native stream queries such as `OpenedLocally`, `Bidirectional`, `ReadClosed`, and `WriteClosed`
- post-open native advisory frames such as `PRIORITY_UPDATE`
- detailed runtime counters in `Stats()`: the adapter only reports coarse session state because `quic-go` does not
  expose matching mux internals

## Non-goals

- translating QUIC into full native zmux wire behavior
- exposing capabilities that QUIC does not actually carry on the wire
