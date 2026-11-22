package zmux

import (
	"sync"
	"syscall"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

// Low-water marks of TCP carriers:
// 128 KiB on a single pipe, 32 KiB per pipe of a multipath session.
const (
	singleLowWater = 128 << 10
	multiLowWater  = 32 << 10
)

// tcpInfo is the TCP socket under a carrier, when there is one: its
// low-water mark and its minimum RTT are optional carrier capabilities.
type tcpInfo struct {
	rc syscall.RawConn
	q  *tcpQuery
}

// tcpQuery holds the RawConn.Control callbacks of one socket, bound once,
// and their results, so that a query allocates nothing (a closure per call
// would escape through Control); mu serialises the queries.
type tcpQuery struct {
	unsent, rtt func(uintptr)
	lowWater    func(uintptr)
	n           uint64
	d           time.Duration
	low         int
	mu          sync.Mutex
	ok          bool
}

func newTCPQuery() *tcpQuery {
	q := &tcpQuery{}
	q.unsent = func(fd uintptr) { q.n, q.ok = tcpUnsent(fd) }
	q.rtt = func(fd uintptr) { q.d, q.ok = tcpMinRTT(fd) }
	q.lowWater = func(fd uintptr) { q.ok = setNotsentLowat(fd, q.low) == nil }
	return q
}

// run runs f, one of q's callbacks, on rc and returns its results.
func (q *tcpQuery) run(rc syscall.RawConn, f func(uintptr)) (uint64, time.Duration, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.n, q.d, q.ok = 0, 0, false
	if rc.Control(f) != nil {
		return 0, 0, false
	}
	return q.n, q.d, q.ok
}

func tcpOf(x any) tcpInfo {
	//goland:noinspection GoResourceLeak // borrowed view of the carrier; its owner closes it
	if c := carrierstats.TCPConnOf(x); c != nil {
		if rc, err := c.SyscallConn(); err == nil {
			return tcpInfo{rc: rc, q: newTCPQuery()}
		}
	}
	return tcpInfo{}
}

// setLowWater sets TCP_NOTSENT_LOWAT where the platform has it; true when
// the carrier now bounds its unsent bytes.
func (t tcpInfo) setLowWater(n int) bool {
	if t.rc == nil {
		return false
	}
	q := t.q
	q.mu.Lock()
	defer q.mu.Unlock()
	q.low, q.ok = n, false
	return t.rc.Control(q.lowWater) == nil && q.ok
}

// minRTT is the carrier's own minimum RTT (Linux tcpi_min_rtt, Windows
// MinRttUs); Darwin reports only a smoothed value, which is not used.
func (t tcpInfo) minRTT() (time.Duration, bool) {
	if t.rc == nil {
		return 0, false
	}
	_, d, ok := t.q.run(t.rc, t.q.rtt)
	return d, ok && d > 0
}

// unsent is the carrier's unsent count, an optional capability: the bytes
// the socket holds and has not sent (Linux SIOCOUTQNSD).
func (t tcpInfo) unsent() (uint64, bool) {
	if t.rc == nil {
		return 0, false
	}
	n, _, ok := t.q.run(t.rc, t.q.unsent)
	return n, ok
}

func (t tcpInfo) hasMinRTT() bool {
	_, ok := t.minRTT()
	return ok
}
