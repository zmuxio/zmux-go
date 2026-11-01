package zmux

import "testing"

// The per-batch socket queries of a TCP carrier allocate nothing.
func TestTCPQueryAllocs(t *testing.T) {
	a, b := tcpPair(t)
	defer func() { _ = a.Close(); _ = b.Close() }()
	ti := tcpOf(a)
	for name, f := range map[string]func(){
		"unsent":        func() { _, _ = ti.unsent() },
		"retransmitted": func() { _, _ = ti.retransmitted() },
		"minRTT":        func() { _, _ = ti.minRTT() },
		"setLowWater":   func() { _ = ti.setLowWater(singleLowWater) },
	} {
		if n := testing.AllocsPerRun(100, f); n != 0 {
			t.Errorf("%s: %v allocations", name, n)
		}
	}
}
