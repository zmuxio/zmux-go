# AGENTS.md

- Five modules: the root `github.com/zmuxio/zmux-go`, `zmuxquic/`, `zmuxh3/`,
  `zmuxws/` and `seal/`. The root `go.work` resolves the root module locally for the
  sub-modules (no `replace` directives); run every check below in each module.
- Before you finish, `gofmt -l .` must print nothing and `go vet ./...` and
  `go test -race ./...` and `golangci-lint run` must pass in every module.
- The root module depends on the standard library and the standard-library-only
  modules already in its `go.mod`; QUIC and HTTP/3 dependencies live in the
  sub-modules.
- Treat everything from peers and the network as untrusted: bound every read
  and every parse, and reject ambiguous framing instead of guessing.
- Methods on nil connection and stream pointers return an error instead of
  panicking.
- User callbacks never run under a lock. The exception is a record `Shaper`
  (`Plan`, `OnEvent`, its factory): it decides each record as the record is
  assembled, must not block or call the session, and a panic stops the shaping
  of that pipe. A `HandshakeShaper` is no exception.
- Tests must not leak goroutines, sockets or timers, and must not rely on OS
  socket buffer sizes.
- `testdata/spec`, `testdata/carrier-cache`, `testdata/shaping` and
  `seal/proto/testdata/vectors` are copied from the specification; do not
  edit them by hand.
- Commit subjects are short, English and imperative.
- Never commit host names, IP addresses or credentials.
