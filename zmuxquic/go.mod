module github.com/zmuxio/zmux-go/zmuxquic

go 1.27.0

require (
	github.com/quic-go/quic-go v0.64.1
	github.com/zmuxio/zmux-go v0.0.0
)

require (
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/zmuxio/zmux-go => ../
