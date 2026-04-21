// Package quicmux adapts quic-go connections into the repository-default zmux
// Session / Stream interfaces without pulling QUIC dependencies into the main
// zmux module.
//
// Mapping rules:
//   - bidirectional and unidirectional stream open / accept map directly to
//     QUIC streams
//   - QUIC stream cancelation codes surface as zmux ApplicationError values
//   - open-time zmux metadata is carried through a tiny per-stream prelude:
//     varint(metadata_len) followed by metadata TLVs
//   - a fresh locally opened bidirectional stream that first performs
//     CloseRead / CancelRead writes that prelude before STOP_SENDING so the
//     peer can parse the adapter opener before seeing the read-side stop
//   - fresh write-side reset / abort visibility is intentionally not a portable
//     adapter guarantee because QUIC RESET_STREAM may discard previously written
//     but unacknowledged stream data, including a just-written prelude
//   - accepted-stream prelude parsing runs in the background with bounded,
//     on-demand concurrency so stalled or invalid adapter preludes do not
//     block later ready streams without forcing idle sessions to keep worker
//     goroutines alive
//   - post-open metadata updates are not representable on the QUIC wire and
//     return ErrAdapterUnsupported joined with ErrPriorityUpdateUnavailable
//   - stream-level reason strings are advisory-only in the zmux API and are
//     not carried by QUIC stream cancellation; only the numeric code survives
package quicmux
