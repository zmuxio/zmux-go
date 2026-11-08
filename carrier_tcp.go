package zmux

import (
	"crypto/tls"
	"net"
	"sync"
	"syscall"
	"time"
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
	rc      syscall.RawConn
	retrans *counter32 // widens a 32-bit retransmission count (Windows)
	q       *tcpQuery
}

// tcpQuery holds the RawConn.Control callbacks of one socket, bound once,
// and their results, so that a query allocates nothing (a closure per call
// would escape through Control); mu serialises the queries.
type tcpQuery struct {
	mu                   sync.Mutex
	n                    uint64
	d                    time.Duration
	low                  int
	ok                   bool
	unsent, retrans, rtt func(uintptr)
	lowWater             func(uintptr)
}

func newTCPQuery() *tcpQuery {
	q := &tcpQuery{}
	q.unsent = func(fd uintptr) { q.n, q.ok = tcpUnsent(fd) }
	q.retrans = func(fd uintptr) { q.n, q.ok = tcpRetrans(fd) }
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

// counter32 extends a wrapping 32-bit counter to 64 bits, read often
// enough that it wraps at most once between reads.
type counter32 struct {
	mu   sync.Mutex
	last uint32
	high uint64 // wraps seen, << 32
}

func (c *counter32) widen(v uint32) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v < c.last {
		c.high += 1 << 32
	}
	c.last = v
	return c.high | uint64(v)
}

// tcpOf finds the TCP socket under a carrier half (a TCP connection, or a
// TLS connection over one).
func tcpOf(x any) tcpInfo {
	if nc, ok := x.(interface{ NetConn() net.Conn }); ok {
		x = nc.NetConn() // TLS, or a transform's half over a connection
		if tc, ok := x.(*tls.Conn); ok {
			x = tc.NetConn()
		}
	}
	if c, ok := x.(*net.TCPConn); ok {
		if rc, err := c.SyscallConn(); err == nil {
			return tcpInfo{rc: rc, retrans: retransCounter(), q: newTCPQuery()}
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

// retransmitted is the bytes the socket retransmitted, an optional
// capability: Linux tcpi_bytes_retrans of TCP_INFO (64-bit, Linux 4.19
// and later), Darwin tcpi_txretransmitbytes of TCP_CONNECTION_INFO
// (64-bit), Windows BytesRetrans of SIO_TCP_INFO (32-bit in every version
// of TCP_INFO, widened here: it is read at most once a second, far less
// than 4 GiB of retransmissions apart).
func (t tcpInfo) retransmitted() (uint64, bool) {
	if t.rc == nil {
		return 0, false
	}
	n, _, ok := t.q.run(t.rc, t.q.retrans)
	if ok && t.retrans != nil {
		n = t.retrans.widen(uint32(n))
	}
	return n, ok
}

func (t tcpInfo) hasMinRTT() bool {
	_, ok := t.minRTT()
	return ok
}
