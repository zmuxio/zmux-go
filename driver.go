package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// pipeDrv owns the goroutines and carrier halves of one attachment.
// Ownership: a reader goroutine owns its half and its read buffer; a writer
// goroutine owns its half (the write token) and its batch; halves are
// closed through hc (lane 0) or their laneOut exactly once each.
type pipeDrv struct {
	c      *Conn
	id     uint64
	hc     *halfCloser
	wsig   *core.Signal   // wakes the lane-0 writer
	opener laneOpener     // nil: the carrier offers no lanes
	tcp    tcpInfo        // the TCP socket under the carrier, if any
	outer  *Conn          // the outer session of a stream-backed carrier
	gs     pipeGoroutines // the attachment's goroutines (HandlePipe waits on them)

	// Guarded by the session lock.
	lanes   map[uint64]*laneOut // outbound lanes above 0
	inLanes []io.ReadCloser     // inbound lanes above 0
	dead    bool                // replaced (early-data restart): its goroutines report nothing

	smu      sync.Mutex // leaf: shutdown request of lane 0
	shutdown bool
	framing  Framing
}

// laneOut is the writer side of one outbound lane above 0.
type laneOut struct {
	lane uint64
	w    io.WriteCloser
	sig  *core.Signal
	once sync.Once
	smu  sync.Mutex
	shut bool
}

func (l *laneOut) close(abort bool) {
	l.once.Do(func() {
		if a, ok := l.w.(aborter); ok && abort {
			a.Abort(net.ErrClosed)
		}
		_ = l.w.Close()
	})
}

// pipeGoroutines counts an attachment's goroutines without a goroutine of
// its own to wait for them: ended is closed when the count drops to zero
// (lock level L3).
type pipeGoroutines struct {
	mu    sync.Mutex
	n     int
	ended chan struct{}
}

func (g *pipeGoroutines) add() {
	g.mu.Lock()
	if g.n == 0 {
		g.ended = make(chan struct{})
	}
	g.n++
	g.mu.Unlock()
}

func (g *pipeGoroutines) done() {
	g.mu.Lock()
	if g.n--; g.n == 0 {
		close(g.ended)
	}
	g.mu.Unlock()
}

// wait returns a channel closed once no goroutine of the attachment runs.
func (g *pipeGoroutines) wait() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n == 0 {
		return closedChan
	}
	return g.ended
}

var closedChan = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

func (pd *pipeDrv) requestShutdown() {
	pd.smu.Lock()
	pd.shutdown = true
	pd.smu.Unlock()
	pd.wsig.Fire()
}

func (pd *pipeDrv) takeShutdown() bool {
	pd.smu.Lock()
	defer pd.smu.Unlock()
	s := pd.shutdown
	pd.shutdown = false
	return s
}

// carrierInfo describes a carrier half to the core.
func carrierInfo(p PipeIO, tcp tcpInfo, lowWater bool) core.CarrierInfo {
	// Records aligned to TLS records only on a TLS byte stream (SPEC.md
	// 3.3); a QUIC stream reports a TLS state for its exporter only.
	_, isTLS := p.W.(*tls.Conn)
	_, lanes := p.W.(laneOpener)
	if !lanes {
		_, lanes = p.R.(laneOpener)
	}
	return core.CarrierInfo{Framing: core.Framing(p.Framing), TLS: isTLS, MinRTT: tcp.hasMinRTT(),
		Lanes: lanes, QueueBound: lowWater}
}

// newPipeDrv prepares the driver of an attachment (under the lock).
func (c *Conn) newPipeDrv(id uint64, p PipeIO) *pipeDrv {
	pd := &pipeDrv{c: c, id: id, hc: newHalfCloser(p), framing: p.Framing, wsig: core.NewSignal()}
	if o, ok := p.W.(laneOpener); ok {
		pd.opener = o
	} else if o, ok := p.R.(laneOpener); ok {
		pd.opener = o
	}
	pd.tcp = tcpOf(p.W)
	return pd
}

// startPipeLocked starts the goroutines of an attachment.
func (c *Conn) startPipeLocked(pd *pipeDrv, read, write bool) {
	c.pipes[pd.id] = pd
	for _, x := range []any{pd.hc.io.W, pd.hc.io.R} {
		if cc, ok := x.(carrierConner); ok {
			c.s.SetCarrierConn(pd.id, cc.CarrierConn())
			break
		}
	}
	if read && pd.hc.io.R != nil {
		c.wg.Add(1)
		pd.gs.add()
		go func() {
			defer pd.gs.done()
			c.readLoop(pd, 0, pd.hc.io.R)
		}()
	}
	if write && pd.hc.io.W != nil {
		c.wg.Add(1)
		pd.gs.add()
		go func() {
			defer pd.gs.done()
			c.writeLoop(pd, 0, pd.wsig, nil)
		}()
	}
}

// addBootstrap attaches pipe 0 and starts its goroutines (under the lock).
func (c *Conn) addBootstrap(p PipeIO) {
	pd := c.newPipeDrv(0, p)
	pd.outer = outerOf(p)
	_ = nestPipe(c, pd.outer) // a new session cannot form a loop
	low := pd.tcp.setLowWater(singleLowWater)
	info := carrierInfo(p, pd.tcp, low)
	c.orphanSig.Store(pd.wsig)
	c.s.AttachBootstrap(info, info, pd.wsig)
	if d, ok := pd.tcp.minRTT(); ok {
		c.s.SetCarrierMinRTT(core.Duration(d))
	}
	c.startPipeLocked(pd, true, true)
}

// readLoop feeds what a carrier half delivers into the core. It never
// waits for anything but the carrier, so reads never wait behind writes
// (SPEC.md 9.1). A quiet pipe blocks on a small buffer and holds no pooled
// read buffer (IMPLEMENTATION.md 2.8).
func (c *Conn) readLoop(pd *pipeDrv, lane uint64, r io.Reader) {
	defer c.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	if pd.framing == Records {
		c.readMessages(pd, lane, r)
		return
	}
	var small [idleBufSize]byte
	var buf *core.Buf
	used := 0
	reads := 0
	for {
		var n int
		var err error
		if buf == nil {
			n, err = r.Read(small[:])
			if n > 0 {
				buf = getReadBuf()
				copy(buf.B, small[:n])
				used = n
			}
		} else {
			n, err = r.Read(buf.B[used:])
			used += n
		}
		consumed := 0
		c.lock()
		if pd.dead {
			c.unlock()
			if buf != nil {
				buf.Unref()
			}
			return
		}
		now := c.clk.now()
		if used > 0 {
			consumed = c.s.Receive(pd.id, lane, buf, buf.B[:used], now)
		}
		if err != nil {
			graceful := errors.Is(err, io.EOF)
			c.s.PipeEnded(pd.id, lane, graceful, used > consumed, now)
		}
		if reads++; reads&63 == 0 && pd.id == 0 && lane == 0 {
			// The carrier's own minimum RTT feeds auto-tuning.
			if d, ok := pd.tcp.minRTT(); ok {
				c.s.SetCarrierMinRTT(core.Duration(d))
			}
		}
		c.unlock()
		c.passDatagrams()
		if err != nil {
			if buf != nil {
				buf.Unref()
			}
			if lane == 0 {
				pd.hc.closeRead()
			} else if rc, ok := r.(io.Closer); ok {
				_ = rc.Close()
			}
			return
		}
		rest := used - consumed
		switch {
		case buf == nil:
		case rest == 0:
			// No partial record: give the buffer back and wait on the small
			// one, unless the read filled it (bulk keeps its buffer).
			if used < len(buf.B) || buf.Refs() > 1 {
				buf.Unref()
				buf, used = nil, 0
			} else {
				used = 0
			}
		case consumed > 0:
			if buf.Refs() == 1 {
				copy(buf.B, buf.B[consumed:used])
			} else {
				nb := getReadBuf()
				copy(nb.B, buf.B[consumed:used])
				buf.Unref()
				buf = nb
			}
			used = rest
		}
	}
}

// readMessages reads a message-framed carrier.
func (c *Conn) readMessages(pd *pipeDrv, lane uint64, r io.Reader) {
	mr, ok := r.(MessageReader)
	for {
		var msg []byte
		var err error
		if ok {
			msg, err = mr.ReadMessage()
		} else {
			err = errors.New("zmux: Records framing needs a MessageReader")
		}
		c.lock()
		now := c.clk.now()
		if err == nil {
			c.s.ReceiveMessage(pd.id, lane, &core.Buf{B: msg}, msg, now)
		} else {
			c.s.PipeEnded(pd.id, lane, errors.Is(err, io.EOF), false, now)
		}
		c.unlock()
		c.passDatagrams()
		if err != nil {
			pd.hc.closeRead()
			return
		}
	}
}

// writeLoop produces batches for one outbound lane and writes them. It owns
// the lane's write side: no other goroutine writes to it.
func (c *Conn) writeLoop(pd *pipeDrv, lane uint64, sig *core.Signal, lo *laneOut) {
	defer c.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	var w io.Writer = pd.hc.io.W
	if lo != nil {
		w = lo.w
	}
	var b *core.Batch // held while the lane has something to send
	var reported bool // b was written and WriteDone is owed
	defer func() {
		if b != nil && !reported {
			core.PutBatch(b)
		}
	}()
	var werr error
	for {
		if lo == nil && pd.takeShutdown() {
			pd.hc.shutdownWrite()
		}
		if lo != nil && lo.takeShut() {
			lo.close(false)
		}
		c.lock()
		if pd.dead {
			if reported {
				b.Discard()
				reported = false
			}
			c.unlock()
			return
		}
		now := c.clk.now()
		if reported {
			c.s.WriteDone(b, werr, now)
			reported = false
		}
		if lane == 0 {
			c.releaseOrphansLocked()
		}
		produced := false
		if werr == nil {
			if b == nil {
				b = core.GetBatch()
			}
			produced = c.s.PollTransmit(pd.id, lane, b, core.DefaultBatch, now)
		}
		closing := c.closing
		c.unlock()
		if produced {
			_, werr = writeBatch(w, pd.framing, b)
			reported = true
			if b.Shutdown {
				if lo == nil {
					pd.requestShutdown()
				} else {
					lo.requestShut()
				}
			}
			continue
		}
		if closing || werr != nil {
			return
		}
		if b != nil {
			core.PutBatch(b) // nothing to send: wait without it
			b = nil
		}
		select {
		case <-sig.C:
		case <-c.done:
			return
		}
	}
}

func (l *laneOut) requestShut() {
	l.smu.Lock()
	l.shut = true
	l.smu.Unlock()
	l.sig.Fire()
}

func (l *laneOut) takeShut() bool {
	l.smu.Lock()
	defer l.smu.Unlock()
	s := l.shut
	l.shut = false
	return s
}

// batchWriter takes a batch without a copy (a stream carrying a nested
// session's pipe).
type batchWriter interface {
	WriteBatch(b *core.Batch) (int, error)
}

// writeBatch hands a batch to the carrier in one call (gather write on a
// net.Conn).
func writeBatch(w io.Writer, framing Framing, b *core.Batch) (int, error) {
	if bw, ok := w.(batchWriter); ok && framing == ByteStream {
		return bw.WriteBatch(b)
	}
	if framing == Records {
		mw, ok := w.(MessageWriter)
		if !ok {
			return 0, errors.New("zmux: Records framing needs a MessageWriter")
		}
		n := 0
		for _, m := range b.Messages() {
			if err := mw.WriteMessage(m); err != nil {
				return n, err
			}
			n += len(m)
		}
		return n, nil
	}
	bufs := b.Bufs()
	if len(bufs) == 1 {
		return w.Write(bufs[0])
	}
	if nc, ok := w.(net.Conn); ok {
		nb := net.Buffers(bufs)
		n, err := nb.WriteTo(nc)
		return int(n), err
	}
	n := 0
	for _, p := range bufs {
		m, err := w.Write(p)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// ---- lanes (MULTIPATH.md 2.5, 5.8) ----

// openLane opens a requested outbound lane outside the lock.
func (c *Conn) openLane(pd *pipeDrv) {
	defer c.wg.Done()
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	w, err := pd.opener.OpenLane(ctx)
	c.lock()
	defer c.unlock()
	if err != nil || c.closing {
		c.s.LaneOpenFailed(pd.id)
		if w != nil {
			_ = w.Close()
		}
		return
	}
	lo := &laneOut{w: w, sig: core.NewSignal()}
	info := core.CarrierInfo{Framing: core.Framing(pd.framing), Lanes: true}
	lane, ok := c.s.LaneOpened(pd.id, info, lo.sig)
	if !ok {
		_ = w.Close()
		return
	}
	lo.lane = lane
	if pd.lanes == nil {
		pd.lanes = map[uint64]*laneOut{}
	}
	pd.lanes[lane] = lo
	c.wg.Add(1)
	pd.gs.add()
	go func() {
		defer pd.gs.done()
		c.writeLoop(pd, lane, lo.sig, lo)
	}()
}

// ---- carrier commands ----

// doAction performs a carrier command outside the lock.
func (c *Conn) doAction(a core.Action) {
	c.mu.Lock()
	pd := c.pipes[a.Pipe]
	var lo *laneOut
	var start bool
	if pd != nil {
		lo = pd.lanes[a.Lane]
		if a.Kind == core.ActOpenLane && pd.opener != nil && !c.closing {
			c.wg.Add(1)
			start = true
		}
	}
	c.mu.Unlock()
	if pd == nil {
		return
	}
	switch a.Kind {
	case core.ActShutdown:
		if a.Lane == 0 {
			pd.requestShutdown()
		} else if lo != nil {
			lo.requestShut()
		}
	case core.ActAbort:
		pd.closeAll(true)
	case core.ActClose:
		if a.Dir&core.DirIn != 0 {
			pd.hc.closeRead()
		}
		if a.Dir&core.DirOut != 0 {
			pd.hc.shutdownWrite()
		}
	case core.ActOpenLane:
		if start {
			go c.openLane(pd)
		} else if pd.opener == nil {
			c.mu.Lock()
			c.s.LaneOpenFailed(pd.id)
			c.mu.Unlock()
		}
	case core.ActResetLane:
		if lo != nil {
			lo.close(true)
		}
	}
}

// closeAll closes every half and lane of the attachment.
func (pd *pipeDrv) closeAll(abort bool) {
	pd.hc.closeAll(abort, net.ErrClosed)
	pd.c.mu.Lock()
	lanes := make([]*laneOut, 0, len(pd.lanes))
	for _, l := range pd.lanes {
		lanes = append(lanes, l)
	}
	in := pd.inLanes
	pd.inLanes = nil
	pd.c.mu.Unlock()
	for _, l := range lanes {
		l.close(abort)
	}
	for _, r := range in {
		_ = r.Close()
	}
}

// fail closes the session after a panic outside the lock.
func (c *Conn) fail(p any) {
	c.lock()
	defer c.unlock()
	c.poison(p)
}

// ---- orphaned handles (GC cleanup) ----

// orphan queues a stream whose handle was collected without being closed;
// it runs on the runtime's cleanup goroutine and takes only a leaf lock.
func (c *Conn) orphan(st *core.Stream) {
	c.orphMu.Lock()
	c.orphans = append(c.orphans, st)
	c.orphMu.Unlock()
	if sig := c.orphanSig.Load(); sig != nil {
		sig.Fire()
	}
}

func (c *Conn) releaseOrphansLocked() {
	c.orphMu.Lock()
	os := c.orphans
	c.orphans = nil
	c.orphMu.Unlock()
	for _, st := range os {
		c.s.Release(st)
	}
}
