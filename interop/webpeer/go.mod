module github.com/zmuxio/zmux-go/interop/webpeer

go 1.27

require (
	github.com/zmuxio/zmux-go v1.5.7
	github.com/zmuxio/zmux-go/zmuxws v1.5.7
)

require (
	github.com/djylb/netx v0.0.0-20261128041904-a1937dc2039d // indirect
	github.com/djylb/timeshim v0.0.0-20261127195401-343d87836828 // indirect
)

replace github.com/zmuxio/zmux-go => ../..

replace github.com/zmuxio/zmux-go/zmuxws => ../../zmuxws
