package runtime

import (
	"sync/atomic"
	"time"
)

const keepaliveJitterGamma uint64 = 0x9e3779b97f4a7c15

var keepaliveJitterSeedCounter atomic.Uint64

func InitKeepaliveJitterState(seed uint64) uint64 {
	if seed != 0 {
		return seed
	}
	return keepaliveJitterSeedCounter.Add(keepaliveJitterGamma)
}

func InitSessionNonceState(seed uint64) uint64 {
	return InitKeepaliveJitterState(seed)
}

func NextKeepaliveJitter(base time.Duration, state *uint64) time.Duration {
	window := base / 8
	if window <= 0 {
		return 0
	}

	return time.Duration(nextKeepaliveJitterValue(state) % uint64(window+1))
}

func NextSessionNonce(state *uint64) uint64 {
	return nextKeepaliveJitterValue(state)
}

func nextKeepaliveJitterValue(state *uint64) uint64 {
	if state == nil {
		var local uint64
		state = &local
	}
	x := *state
	if x == 0 {
		x = InitKeepaliveJitterState(0)
	}
	x += keepaliveJitterGamma
	*state = x

	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
