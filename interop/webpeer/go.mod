module github.com/zmuxio/zmux-go/interop/webpeer

go 1.27

require (
	github.com/zmuxio/zmux-go v1.4.12
	github.com/zmuxio/zmux-go/zmuxws v1.4.11
)

require (
	github.com/djylb/netx v0.0.0-20261114154740-c89a319ecfb2 // indirect
	github.com/djylb/timeshim v0.0.0-20261114151544-5e7682ca67cc // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxws => ../../zmuxws
