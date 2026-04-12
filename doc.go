// Package zmux implements the zmux v1 single-connection stream multiplexer.
//
// The package exposes the library-facing Session/Stream API for opening,
// accepting, reading, writing, and closing multiplexed streams over a single
// transport connection.
//
// The root package is the supported integration surface. Wire codec logic,
// protocol-state helpers, and repository-default runtime policy remain in
// shallow internal packages:
//
//   - internal/wire
//   - internal/state
//   - internal/runtime
//
// Library users should treat those internal packages as implementation
// details rather than import contracts.
package zmux
