# quicmux

`quicmux` adapts `quic-go` sessions / connections to the repository-default
`zmux.Session` interface without adding any QUIC dependency to the main
`zmux-go` module.

## Usage

```go
import (
	quic "github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go/adapter/quicmux"
)

var qconn *quic.Conn
var session = quicmux.WrapSession(qconn)
```

`WrapSession` accepts the QUIC connection shape directly, so the result of
`quic.DialAddr`, `Transport.Dial`, or `Listener.Accept` can be wrapped without
any extra shim object.

## Mapping

- `AcceptStream` / `OpenStream` map to QUIC bidirectional streams directly.
- `AcceptUniStream` / `OpenUniStream` map to QUIC unidirectional streams directly.
- `CloseRead` maps to QUIC `CancelRead(CANCELLED)`.
- `CloseWrite` maps to QUIC's send-side `Close`.
- `Reset` / `ResetWithReason` map to QUIC `CancelWrite`.
- `Abort*` are best-effort local termination helpers that
  cancel both readable and writable directions with the same application code.
- QUIC stream / connection application errors are normalized to
  `*zmux.ApplicationError`.
- QUIC stream-limit errors are normalized to `zmux.ErrOpenLimited`.
- QUIC idle / stateless-reset / version-negotiation / local no-error close
  conditions are normalized to `zmux.ErrSessionClosed`.
- `Closed()` reports whether the wrapped QUIC connection context has already
  terminated.
- `Accept*` / `Open*` treat `nil` contexts as `context.Background()`.

## Metadata Support

`quicmux` carries the zmux open-time metadata subset by prepending a tiny
adapter header to each QUIC stream:

- prelude format: `varint(metadata_len) + TLV...`
- `OpenStreamWithOptions` and `OpenUniStreamWithOptions` support `OpenInfo`,
  `InitialPriority`, and `InitialGroup`
- `OpenInfo()` and `Metadata()` expose the decoded opener metadata on accepted
  streams
- `UpdateMetadata(...)` is supported only before the prelude has been emitted;
  this lets callers set priority / group just before the stream first becomes
  peer-visible on the QUIC wire

Once the prelude has been emitted, later metadata updates are not representable
on the QUIC wire. The adapter then returns
`errors.Join(zmux.ErrAdapterUnsupported, zmux.ErrPriorityUpdateUnavailable)`.

## Unsupported Or Degraded Methods

- `UpdateMetadata(...)` only works before the adapter prelude has been emitted.
  After the stream becomes peer-visible, the adapter returns
  `errors.Join(zmux.ErrAdapterUnsupported, zmux.ErrPriorityUpdateUnavailable)`.
- `ResetWithReason(code, reason)` cannot put `reason` on the QUIC stream wire.
  QUIC stream cancellation carries only the numeric application code.
- `AbortWithError(err)` and `AbortWithErrorCode(code, reason)` on streams can
  only propagate the numeric code on the wire. A free-form reason string is not
  available on QUIC stream cancellation.
- `Stats()` only reports the coarse session `State`. Queue, pressure,
  keepalive, and similar repository-default counters stay zero because
  `quic-go` does not expose matching per-session mux diagnostics.

## Error Surface

- connection-level QUIC application closes preserve both code and reason text
  and are mapped to `*zmux.ApplicationError`
- stream-level QUIC cancellations preserve only the numeric application code

## Non-goals

- Translating QUIC streams into full native zmux wire behavior. This adapter
  only presents the repository-default `zmux` API over an existing QUIC mux.
- Implementing post-open zmux advisory frames such as native
  `PRIORITY_UPDATE`; the adapter only supports the open-time metadata subset.
