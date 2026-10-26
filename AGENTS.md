# AGENTS.md

- Three modules: the root `github.com/zmuxio/zmux-go`, `zmuxquic/` and
  `zmuxh3/`. The root `go.work` resolves the root module locally for the
  sub-modules (no `replace` directives); run every check below in each module.
- Before you finish, `gofmt -l .` must print nothing and `go vet ./...` and
  `go test -race ./...` and `golangci-lint run` must pass in every module.
- The root module depends on the standard library only (plus
  `golang.org/x` where already required); QUIC and HTTP/3 dependencies live
  in the sub-modules.
- Treat everything from peers and the network as untrusted: bound every read
  and every parse, and reject ambiguous framing instead of guessing.
- Methods on nil connection and stream pointers return an error instead of
  panicking.
- User callbacks never run under a lock.
- Tests must not leak goroutines, sockets or timers, and must not rely on OS
  socket buffer sizes.
- `testdata/spec` mirrors the protocol conformance fixtures; do not edit it
  by hand.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
