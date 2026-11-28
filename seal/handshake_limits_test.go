package seal

import (
	"runtime"
	"testing"
)

func TestDefaultHandshakeLimitsReservePSK(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	a, b := new(ServerConfig), new(ServerConfig)
	first, second := a.handshakeLimits(), b.handshakeLimits()
	if first == second || first != a.handshakeLimits() {
		t.Fatal("default limits must belong to their configuration")
	}
	if !first.acquire(true) || first.acquire(true) || !first.acquire(false) || first.acquire(false) {
		t.Fatal("one Go processor must provide one permit for anonymous Hellos and reserve one for PSK")
	}
	if !second.acquire(true) {
		t.Fatal("one configuration's permits must not affect another's")
	}
	first.release(true)
	first.release(false)
	second.release(true)
}
