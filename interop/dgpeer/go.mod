module github.com/zmuxio/zmux-go/interop/dgpeer

go 1.27

require (
	github.com/quic-go/quic-go v0.64.1
	github.com/zmuxio/zmux-go v1.5.2
	github.com/zmuxio/zmux-go/zmuxquic v0.0.0
)

require (
	github.com/djylb/netx v0.0.0-20261118123252-73956d2fc343 // indirect
	github.com/djylb/timeshim v0.0.0-20261114151544-5e7682ca67cc // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxquic => ../../zmuxquic
