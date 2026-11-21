module github.com/zmuxio/zmux-go/interop/webpeer

go 1.27

require (
	github.com/zmuxio/zmux-go v1.5.6
	github.com/zmuxio/zmux-go/zmuxws v1.4.12
)

require (
	github.com/djylb/netx v0.0.0-20261121064300-ffc850492f27 // indirect
	github.com/djylb/timeshim v0.0.0-20261121052148-1ed9b7a27ad0 // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxws => ../../zmuxws
