// Package zmux implements the zmux v1 single-connection stream multiplexer.
//
// It exposes Session and Stream interfaces for multiplexed streams over one
// transport connection.
//
// Wire codecs, protocol-state helpers, and runtime policy stay in internal
// packages:
//
//   - internal/wire
//   - internal/state
//   - internal/runtime
//
// Treat internal packages as implementation details.
package zmux
