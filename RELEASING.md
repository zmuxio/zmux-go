# Releasing

This repository holds three Go modules:

- root module: `github.com/zmuxio/zmux-go`
- QUIC carrier module: `github.com/zmuxio/zmux-go/zmuxquic`
- HTTP/3 carrier module: `github.com/zmuxio/zmux-go/zmuxh3`

The sub-modules track the current root release directly. They do not keep
compatibility shims for older root versions.

## Version Tags

- root module tag: `vX.Y.Z`
- sub-module tags: `zmuxquic/vX.Y.Z` and `zmuxh3/vX.Y.Z`

## Local Development

The root `go.work` file makes the sub-modules build against the local root
module during development (and in CI):

```go
go 1.27

use (
	.
	./zmuxh3
	./zmuxquic
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

From `zmuxquic`, then from `zmuxh3`, with the workspace off so that the
published root version is resolved:

```bash
export GOWORK=off
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...
```

Then from the repository root:

```bash
git add zmuxquic/go.mod zmuxquic/go.sum zmuxh3/go.mod zmuxh3/go.sum
git commit -m "zmuxquic, zmuxh3: bump zmux-go to vX.Y.Z"
git tag -a zmuxquic/vX.Y.Z -m "zmuxquic/vX.Y.Z"
git tag -a zmuxh3/vX.Y.Z -m "zmuxh3/vX.Y.Z"
git push origin main
git push origin zmuxquic/vX.Y.Z zmuxh3/vX.Y.Z
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

# zmuxquic and zmuxh3 (each directory)
export GOWORK=off
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...

# root
git add zmuxquic/go.mod zmuxquic/go.sum zmuxh3/go.mod zmuxh3/go.sum
git commit -m "zmuxquic, zmuxh3: bump zmux-go to vX.Y.Z"
git tag -a zmuxquic/vX.Y.Z -m "zmuxquic/vX.Y.Z"
git tag -a zmuxh3/vX.Y.Z -m "zmuxh3/vX.Y.Z"
git push origin main
git push origin zmuxquic/vX.Y.Z zmuxh3/vX.Y.Z
```
