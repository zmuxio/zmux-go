// Package quicmux adapts quic-go connections to zmux Session / Stream
// interfaces without adding QUIC dependencies to the main module.
//
// Mapping rules:
//   - bidirectional and unidirectional stream open / accept map directly to
//     QUIC streams
//   - QUIC stream cancelation codes surface as zmux ApplicationError values
//   - open-time zmux metadata is carried in a per-stream prelude:
//     varint(metadata_len) followed by metadata TLVs
//   - CloseRead / CancelRead on a fresh bidirectional stream writes the prelude
//     before STOP_SENDING
//   - fresh write-side reset / abort visibility is not portable because QUIC
//     RESET_STREAM may discard unacknowledged prelude data
//   - accepted-stream preludes are parsed with bounded background concurrency
//   - post-open metadata updates are not representable on the QUIC wire and
//     return ErrAdapterUnsupported joined with ErrPriorityUpdateUnavailable
//   - stream-level reason strings are advisory-only in the zmux API and are
//     not carried by QUIC stream cancellation; only the numeric code survives
package quicmux
