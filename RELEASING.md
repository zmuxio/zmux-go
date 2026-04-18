# Releasing

This repository uses two Go modules:

- root module: `github.com/zmuxio/zmux-go`
- adapter module: `github.com/zmuxio/zmux-go/adapter/quicmux`

The adapter tracks the current root release directly. It does not keep
compatibility shims for older root versions.

## Version Tags

- root module tag: `vX.Y.Z`
- adapter module tag: `adapter/quicmux/vX.Y.Z`

Examples:

- `vX.Y.Z`
- `adapter/quicmux/vX.Y.Z`

## Local Development

Use the root `go.work` file so the adapter always builds against the current
local root module during normal development:

```go
go 1.25.0

use (
	.
	./adapter/quicmux
)
```

That keeps local iteration simple and avoids changing `adapter/quicmux/go.mod`
until release time.

## Release Order

Always release in this order:

1. finalize root changes
2. tag the root module
3. push the root tag
4. update the adapter to that root tag
5. tag the adapter module
6. push `main`
7. push the adapter tag

Do not tag the adapter first.

## Root Release

From the repository root:

```bash
go test ./...
git add .
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

Push the root tag before updating the adapter. The adapter module must be able
to resolve the published root version. Push `main` later together with the
adapter release commit.

## Adapter Release

From `adapter/quicmux`:

```bash
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...
```

Then from the repository root:

```bash
git add adapter/quicmux/go.mod adapter/quicmux/go.sum
git commit -m "adapter/quicmux: bump zmux-go to vX.Y.Z"
git tag -a adapter/quicmux/vX.Y.Z -m "adapter/quicmux/vX.Y.Z"
git push origin main
git push origin adapter/quicmux/vX.Y.Z
```

## Proxy Lag

Some module proxies can lag behind a newly pushed tag. If `go get` or
`go mod tidy` cannot resolve the just-published root version, retry with direct
fetching for this repository:

```powershell
$env:GOPROXY='direct'
$env:GOPRIVATE='github.com/zmuxio/*'
$env:GONOSUMDB='github.com/zmuxio/*'
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

# adapter/quicmux
go get github.com/zmuxio/zmux-go@vX.Y.Z
go mod tidy
go test ./...

# root
git add adapter/quicmux/go.mod adapter/quicmux/go.sum
git commit -m "adapter/quicmux: bump zmux-go to vX.Y.Z"
git tag -a adapter/quicmux/vX.Y.Z -m "adapter/quicmux/vX.Y.Z"
git push origin main
git push origin adapter/quicmux/vX.Y.Z
```
