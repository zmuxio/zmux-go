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
	AcceptedPreludeMaxConcurrent: 4,
})
```

`AcceptedPreludeReadTimeout`:

- `0`: use `quicmux.DefaultAcceptedPreludeReadTimeout`
- `< 0`: disable the adapter-managed timeout
- accepted QUIC streams whose adapter prelude never arrives in time are dropped instead of blocking later ready streams

`AcceptedPreludeMaxConcurrent`:

- `0`: use `quicmux.DefaultAcceptedPreludeMaxConcurrent()`
- `> 0`: per-session upper bound for concurrently parsing accepted adapter preludes
- accepted prelude parsing is on-demand; idle sessions keep no prelude worker goroutines alive

If you want one process-wide default for many wrapped sessions, set it during process init:

```go
quicmux.SetDefaultAcceptedPreludeMaxConcurrent(2)
```

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
- accepted-stream prelude parsing runs in the background, so later ready streams can be accepted before stalled or invalid
  adapter preludes time out
- `CloseRead` maps to QUIC `CancelRead(CANCELLED)`
- `CancelRead(code)` maps to QUIC `CancelRead(code)`
- a fresh locally opened bidirectional stream submits the zmux metadata prelude
  before `CloseRead` / `CancelRead` sends QUIC `STOP_SENDING`, so the peer can
  accept the adapter stream before observing read-side cancellation
- `CloseWrite` maps to QUIC send-side `Close`
- `CancelWrite(code)` maps to QUIC `CancelWrite(code)`
- `CloseWithError(code, reason)` is best-effort: bidirectional streams cancel both local directions; unidirectional
  streams cancel only the locally meaningful direction QUIC exposes
- fresh write-side reset / abort visibility is intentionally not a portable
  adapter guarantee because QUIC `RESET_STREAM` can discard earlier
  unacknowledged stream data, including a just-written prelude
- `Closed()` reports whether the wrapped QUIC connection context has terminated
- `Accept*` and `Open*` treat `nil` contexts as `context.Background()`

## Metadata Support

`quicmux` carries zmux open-time metadata in a small stream prelude:

- prelude format: `varint(metadata_len) + TLV...`
- `OpenStreamWithOptions` and `OpenUniStreamWithOptions` support `OpenInfo`, `InitialPriority`, and `InitialGroup`
- `OpenInfo()` and `Metadata()` expose decoded opener metadata on accepted streams
- `UpdateMetadata(...)` works only before the prelude is emitted
- fresh locally opened streams submit the prelude before read-side terminal
  control, including `CloseRead` / `CancelRead` before any application payload

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
