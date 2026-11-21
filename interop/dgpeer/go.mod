module github.com/zmuxio/zmux-go/interop/dgpeer

go 1.27

require (
	github.com/quic-go/quic-go v0.64.1
	github.com/zmuxio/zmux-go v1.5.6
	github.com/zmuxio/zmux-go/zmuxquic v0.0.0
)

require (
	github.com/djylb/netx v0.0.0-20261121064300-ffc850492f27 // indirect
	github.com/djylb/timeshim v0.0.0-20261121052148-1ed9b7a27ad0 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxquic => ../../zmuxquic
