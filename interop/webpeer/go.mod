module github.com/zmuxio/zmux-go/interop/webpeer

go 1.27

require (
	github.com/zmuxio/zmux-go v1.5.7
	github.com/zmuxio/zmux-go/zmuxws v1.5.6
)

require (
	github.com/djylb/netx v0.0.0-20261122082912-c25ca8c3a7d1 // indirect
	github.com/djylb/timeshim v0.0.0-20261121052148-1ed9b7a27ad0 // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxws => ../../zmuxws
