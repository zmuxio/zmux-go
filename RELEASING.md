# Releasing

This repository holds five released Go modules:

- root module: `github.com/zmuxio/zmux-go`
- QUIC carrier module: `github.com/zmuxio/zmux-go/zmuxquic`
- HTTP/3 carrier module: `github.com/zmuxio/zmux-go/zmuxh3`
- WebSocket carrier module: `github.com/zmuxio/zmux-go/zmuxws`
- secure carrier module: `github.com/zmuxio/zmux-go/seal`

`examples/xorpipe` is a module of its own that is never tagged; it builds
against the local root module (`replace`).

The root module and `zmuxws` require `github.com/djylb/netx` and
`github.com/djylb/timeshim` at pseudo-versions of their `main` branch; those
libraries are not tagged. A change this repository needs is pushed to their
`main` first, then required with `go get <module>@main`.

The sub-modules track the current root release directly. They do not keep
compatibility shims for older root versions.

## Version Tags

- root module tag: `vX.Y.Z`
- sub-module tags: `zmuxquic/vX.Y.Z`, `zmuxh3/vX.Y.Z`, `zmuxws/vX.Y.Z` and
  `seal/vX.Y.Z`

## Local Development

The root `go.work` file makes the sub-modules build against the local root
module during development (and in CI):

```go
go 1.27

use (
	.
	./zmuxh3
	./zmuxquic
	./zmuxws
	./seal
	./examples/xorpipe
)
```

The sub-modules' `go.mod` files keep requiring the last released root
version until release time; they carry no `replace` directive.

## Release Order

Always release in this order:

1. finalize root changes
2. tag the root module
3. push the root tag
4. update each sub-module to that root tag
5. tag the sub-modules
6. push `main`
7. push the sub-module tags

Do not tag a sub-module first.

## Dependency Update

Before every release, update every dependency of every module to its latest
release (`github.com/djylb/netx` and `github.com/djylb/timeshim` to their
latest `main`), then review the changes and run the full tests:

```bash
go get -u -t ./...
go get github.com/djylb/netx@main github.com/djylb/timeshim@main
go mod tidy
(cd zmuxquic && GOWORK=off go get -u -t ./... && GOWORK=off go mod tidy)
(cd zmuxh3 && GOWORK=off go get -u -t ./... && GOWORK=off go mod tidy)
(cd zmuxws && GOWORK=off go get -u -t ./... && GOWORK=off go get github.com/djylb/netx@main github.com/djylb/timeshim@main && GOWORK=off go mod tidy)
(cd seal && GOWORK=off go get -u -t ./... && GOWORK=off go mod tidy)
go test -race ./... ./zmuxquic/... ./zmuxh3/... ./zmuxws/... ./seal/...
```

Commit the updates on their own before the release commit. Keep the `go`
directive unchanged unless raising it is an intentional release decision.

## Root Release

From the repository root:

```bash
go test ./...
git add .
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

Push the root tag before updating the sub-modules: they must be able to
resolve the published root version. Push `main` later together with the
sub-module release commit.

## Sub-module Release

From `zmuxquic`, `zmuxh3`, `zmuxws` and `seal`, with the workspace off so that the
published root version is resolved:

```bash
export GOWORK=off
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...
```

Then from the repository root:

```bash
git add zmuxquic/go.mod zmuxquic/go.sum zmuxh3/go.mod zmuxh3/go.sum zmuxws/go.mod zmuxws/go.sum seal/go.mod seal/go.sum
git commit -m "zmuxquic, zmuxh3, zmuxws, seal: bump zmux-go to vX.Y.Z"
git tag -a zmuxquic/vX.Y.Z -m "zmuxquic/vX.Y.Z"
git tag -a zmuxh3/vX.Y.Z -m "zmuxh3/vX.Y.Z"
git tag -a zmuxws/vX.Y.Z -m "zmuxws/vX.Y.Z"
git tag -a seal/vX.Y.Z -m "seal/vX.Y.Z"
git push origin main
git push origin zmuxquic/vX.Y.Z zmuxh3/vX.Y.Z zmuxws/vX.Y.Z seal/vX.Y.Z
```

## Proxy Lag

Some module proxies can lag behind a newly pushed tag. If `go get` or
`go mod tidy` cannot resolve the just-published root version, retry with direct
fetching for this repository:

```bash
export GOPROXY=direct
export GOPRIVATE='github.com/zmuxio/*'
export GONOSUMDB='github.com/zmuxio/*'
go mod tidy
go test ./...
```

Use that only as a temporary local override while the proxy catches up.

## Template

Release `vX.Y.Z`:

```bash
# root
go test ./...
git add .
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z

# zmuxquic, zmuxh3, zmuxws and seal (each directory)
export GOWORK=off
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...

# root
git add zmuxquic/go.mod zmuxquic/go.sum zmuxh3/go.mod zmuxh3/go.sum zmuxws/go.mod zmuxws/go.sum seal/go.mod seal/go.sum
git commit -m "zmuxquic, zmuxh3, zmuxws, seal: bump zmux-go to vX.Y.Z"
git tag -a zmuxquic/vX.Y.Z -m "zmuxquic/vX.Y.Z"
git tag -a zmuxh3/vX.Y.Z -m "zmuxh3/vX.Y.Z"
git tag -a zmuxws/vX.Y.Z -m "zmuxws/vX.Y.Z"
git tag -a seal/vX.Y.Z -m "seal/vX.Y.Z"
git push origin main
git push origin zmuxquic/vX.Y.Z zmuxh3/vX.Y.Z zmuxws/vX.Y.Z seal/vX.Y.Z
```
