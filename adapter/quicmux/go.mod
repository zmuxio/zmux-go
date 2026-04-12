module github.com/zmuxio/zmux-go/adapter/quicmux

go 1.25.0

require (
	github.com/quic-go/quic-go v0.59.0
	github.com/zmuxio/zmux-go v0.0.0-20260412233857-b89a6360834e
)

require (
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/zmuxio/zmux-go => ../..
