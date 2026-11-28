package core

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const (
	idleBufShift  = 8
	maxBufShift   = 17
	RecvIdleSize  = 1 << idleBufShift
	RecvBlockSize = 1 << maxBufShift // completes any legal byte-framed record
	poolBytes     = 8 << 20
)

// Each class has a lock-free single-slot cache. Further idle blocks share
// one byte limit across the classes so a burst can reuse the sizes it needs.
// An excess return lets GC reclaim the block.
type recvBin struct {
	hot      atomic.Pointer[Buf]
	detached atomic.Pointer[Buf]
	free     *Buf
	size     int
	mu       sync.Mutex
}

const typicalSealedClass = 1280 // carries a 1200-byte plaintext and its tag

var recvBins = func() [maxBufShift - idleBufShift + 2]*recvBin {
	var bins [maxBufShift - idleBufShift + 2]*recvBin
	for i := range bins {
		size := 1 << (i + idleBufShift)
		switch {
		case i == 3:
			size = typicalSealedClass
		case i > 3:
			size >>= 1
		}
		bins[i] = &recvBin{size: size}
	}
	return bins
}()

// The shared list budget excludes the space earmarked for hot slots.
var recvFreeLimit = func() int64 {
	limit := int64(poolBytes)
	for _, bin := range recvBins {
		limit -= int64(bin.size)
	}
	return limit
}()

var recvFreeBytes atomic.Int64

func recvClass(n int) *recvBin {
	i := max(idleBufShift, bits.Len(uint(n-1))) - idleBufShift
	if n > typicalSealedClass {
		i++
	}
	return recvBins[i]
}

// GetRecvBuf returns an exclusively held block from the shared receive allocator.
func GetRecvBuf(n int) *Buf {
	bin := recvClass(n)
	b := bin.hot.Swap(nil)
	if b == nil {
		b = bin.takeFree()
	}
	if b != nil {
		b.Reuse()
		return b
	}
	b = bin.detached.Swap(nil)
	if b == nil {
		b = NewBuf(make([]byte, bin.size))
		b.pool, b.Free = bin, putRecvBuf
		return b
	}
	b.B = make([]byte, bin.size)
	b.Free = putRecvBuf
	b.Reuse()
	return b
}

// A caller that took the bytes keeps the allocation. Retain at most one
// empty descriptor per class for its replacement.
func putDetachedBuf(b *Buf) {
	b.B = nil
	b.pool.detached.CompareAndSwap(nil, b)
}

func putRecvBuf(b *Buf) {
	bin := b.pool
	if bin.hot.CompareAndSwap(nil, b) {
		return
	}
	size := int64(bin.size)
	for n := recvFreeBytes.Load(); n <= recvFreeLimit-size; n = recvFreeBytes.Load() {
		if recvFreeBytes.CompareAndSwap(n, n+size) {
			bin.mu.Lock()
			b.poolNext, bin.free = bin.free, b
			bin.mu.Unlock()
			return
		}
	}
}

func (bin *recvBin) takeFree() *Buf {
	bin.mu.Lock()
	b := bin.free
	if b != nil {
		bin.free, b.poolNext = b.poolNext, nil
	}
	bin.mu.Unlock()
	if b != nil {
		recvFreeBytes.Add(-int64(bin.size))
	}
	return b
}

// Detach transfers an exclusively owned span out of the receive allocator.
func (v Span) Detach() []byte {
	if v.Buf == nil {
		return nil
	}
	out := v.Bytes()
	if v.Buf.pool != nil {
		v.Buf.Free = putDetachedBuf
	}
	v.Buf.Unref()
	return out
}
