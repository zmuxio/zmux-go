// Package join is the sans-IO core of the secure carrier's Join protocol and
// the primitives Establish shares with it: the suite table, Sealed records,
// the handshake payload codec, the first-byte dispatch and the exporter.
//
// It holds bytes and keys only: no IO, clock, callback or goroutine. It imports
// the standard library and varint, so the root module gains no dependency. It
// has Suite 1 itself; other suites register from their own module at init.
package join
