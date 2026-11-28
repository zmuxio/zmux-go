# Releasing

This repository holds five released Go modules. Tag the root module as
`vX.Y.Z` and each carrier module as `<module>/vX.Y.Z`.

## Local development

Create a local workspace from the repository root to build all modules
against the checkout.

```bash
go work init . ./zmuxquic ./zmuxh3 ./zmuxws ./seal ./examples/xorpipe ./interop/webpeer
```

CI creates its workspace in a temporary directory. The examples and
interop peers have their own modules for development and receive no tags.

## Release order

1. Verify that the reviewed development SHA is the source of the public
   sync. Run the public sync check, then verify the root module independently
   with `GOWORK=off`. Tag and push the root release.
2. Once the root tag resolves, update the root requirement in `zmuxquic`,
   `zmuxws` and `seal`. Run the independent checks below in each module
   and push the updated module files and tags.
3. Once the root and `zmuxquic` tags resolve, update both requirements in
   `zmuxh3`. Run the independent checks, then push its updated module files
   and tag.

Development uses the local workspace while the module requirements await their
release tags. Keep the release checks independent of the workspace so they
exercise the same dependency graph that downstream users resolve.

## Independent checks

Set `GOWORK=off` for each release step, including dependency updates.
In a carrier module, use `go get` with the selected dependency tags and
`go mod tidy` before running the checks. Include both `go.mod` and `go.sum`
in the release commit.

```bash
export GOWORK=off
go vet ./...
go test -race ./...
golangci-lint run --config=/path/to/checkout/.golangci.yml
```

If the module proxy lags after a tag is pushed, retry the resolution with
`GOPROXY=direct` for that command.
