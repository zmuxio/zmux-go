package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// pipeDrv owns the goroutines and carrier halves of one attachment.
// Ownership: a reader goroutine owns its half and its read buffer; a writer
// goroutine owns its half (the write token) and its batch; halves are
// closed through hc (lane 0) or their laneOut exactly once each.
type pipeDrv struct {
	c      *Conn
	id     uint64
	hc     *halfCloser
	wsig   *core.Signal    // wakes the lane-0 writer
	opener LaneOpener      // nil: the carrier offers no lanes
	maxRec int             // the pipe's local record limit (carrierRecordLimit), applied to every lane
	tcp    tcpInfo         // the TCP socket under the carrier, if any
	nest   *nestEdge       // the nesting edge of a stream-backed carrier (nil: none)
	gs     pipeGoroutines  // the attachment's goroutines (HandlePipe waits on them)
	lctx   context.Context // the goroutine labels of the pipe (labels.go)

	admitted any // the admission value of the carrier that joined it (set once, under the session lock)

	// Guarded by the session lock (and dead, below).
	lanes   map[uint64]*laneOut // outbound lanes above 0
	inLanes []io.ReadCloser     // inbound lanes above 0

	// The carrier's datagram channel (nil: none) and its sender.
	ch    DatagramChannel
	chSig *core.Signal
	chEnd chan struct{}
	// chReady is closed once the channel's receiver is registered: the
	// first record waits for it.
	chReady chan struct{}
	chOnce  sync.Once

	// graceT is the abort bound of a graceful close (closeGraceful), with
	// the lanes it aborts (graceAb); under smu.
	graceT  *time.Timer
	graceAb []Aborter

	smu      sync.Mutex // leaf: shutdown request of lane 0
	shutdown bool
	shutReq  bool // lane 0's shutdown was requested (sticky)
	framing  Framing
	dead     bool // guarded by the session lock: replaced (early-data restart), its goroutines report nothing
	// wstop (session lock): the lane-0 writer stops before writing again
	// (an early-data restart on the same halves); wdone is closed when it
	// has returned.
	wstop bool
	wdone chan struct{}

	// The end of the attachment (Pipe.Wait, Pipe.Done); a handle keeps it
	// after the session has forgotten the attachment.
	end *pipeEnd
}

// pipeEnd is how an attachment ended: retired is closed when the session
// retires it (err, guarded by the session lock, is set before),
// waited when, besides, its goroutines ended.
type pipeEnd struct {
	err     error
	drv     atomic.Pointer[pipeDrv] // the current driver (an early-data restart replaces pipe 0's)
	retired chan struct{}
	waited  chan struct{}
	once    sync.Once
}

// goneEnd is the end of an attachment the session no longer knows.
var goneEnd = &pipeEnd{retired: closedChan, waited: closedChan}

// laneOut is the writer side of one outbound lane above 0.
type laneOut struct {
	w    io.WriteCloser
	sig  *core.Signal
	lane uint64
	once sync.Once
	smu  sync.Mutex
	shut bool
}

func (l *laneOut) close(abort bool) {
	l.once.Do(func() {
		if a, ok := l.w.(Aborter); ok && abort {
			a.Abort(net.ErrClosed)
		}
		_ = l.w.Close()
	})
}

// pipeGoroutines counts an attachment's goroutines without a goroutine of
// its own to wait for them: ended is closed when the count drops to zero
// (lock level L3).
type pipeGoroutines struct {
	ended  chan struct{}
	n      int
	mu     sync.Mutex
	sealed bool // seal: no goroutine joins any more
}

func (g *pipeGoroutines) add() {
	g.mu.Lock()
	g.addLocked()
	g.mu.Unlock()
}

func (g *pipeGoroutines) addLocked() {
	if g.n == 0 {
		g.ended = make(chan struct{})
	}
	g.n++
}

// tryAdd counts one more goroutine unless the count was sealed.
func (g *pipeGoroutines) tryAdd() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sealed {
		return false
	}
	g.addLocked()
	return true
}

// seal refuses every later tryAdd: a wait that starts after it covers
// every goroutine the count will ever have.
func (g *pipeGoroutines) seal() {
	g.mu.Lock()
	g.sealed = true
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
	pd.shutReq = true
	pd.smu.Unlock()
	pd.wsig.Fire()
}

// shutdownRequested reports whether lane 0's shutdown was ever requested.
func (pd *pipeDrv) shutdownRequested() bool {
	pd.smu.Lock()
	defer pd.smu.Unlock()
	return pd.shutReq
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
	// Records aligned to TLS records only on a TLS byte stream;
	// a QUIC stream reports a TLS state for its exporter only.
	isTLS := tlsRecords(p.W)
	_, lanes := p.W.(LaneOpener)
	if !lanes {
		_, lanes = p.R.(LaneOpener)
	}
	_, half := p.W.(CloseWriter)
	half = half || p.R == nil || p.W == nil || !carrierio.SameHalf(p.R, p.W)
	info := core.CarrierInfo{HalfClose: half, Framing: core.Framing(p.Framing), TLS: isTLS, MinRTT: tcp.hasMinRTT(),
		Lanes: lanes, QueueBound: lowWater, WriteOverhead: p.WriteOverhead, MaxRecord: carrierRecordLimit(p)}
	info.Channel = channelOf(p) != nil
	return info
}

// newPipeDrv prepares the driver of an attachment (under the lock).
func (c *Conn) newPipeDrv(id uint64, p PipeIO) *pipeDrv {
	pd := &pipeDrv{c: c, id: id, hc: newHalfCloser(p), framing: p.Framing, wsig: core.NewSignal(), lctx: c.pipeLabels(id)}
	pd.end = &pipeEnd{retired: make(chan struct{})}
	pd.end.drv.Store(pd)
	if o, ok := p.W.(LaneOpener); ok {
		pd.opener = o
	} else if o, ok := p.R.(LaneOpener); ok {
		pd.opener = o
	}
	pd.tcp = tcpOf(p.W)
	pd.ch = channelOf(p)
	pd.maxRec = carrierRecordLimit(p)
	return pd
}

// adopt makes q the carrier of a joined attachment; the TCP socket stays the one
// below it, for the statistics.
func (pd *pipeDrv) adopt(q PipeIO) {
	pd.hc, pd.framing, pd.maxRec = newHalfCloser(q), q.Framing, carrierRecordLimit(q)
}

// startPipeLocked starts the goroutines of an attachment.
//
// Every PipeIO that becomes part of a session starts here once, so this is
// where the padding of the handshake run on it is metered: an attachment
// that fails before (a join not confirmed) spends nothing.
func (c *Conn) startPipeLocked(pd *pipeDrv, read, write bool) {
	c.s.ShapeSpendHandshake(pd.hc.io.HandshakePadding)
	pd.hc.take(pd.closeAll)
	c.pipes[pd.id] = pd
	for _, x := range []any{pd.hc.io.W, pd.hc.io.R} {
		if cc, ok := x.(CarrierConner); ok {
			c.s.SetCarrierConn(pd.id, cc.CarrierConn())
			break
		}
	}
	c.startChannel(pd) // its receiver before the first record
	if read && pd.hc.io.R != nil {
		c.wg.Add(1)
		pd.gs.add()
		labels.Go(pd.lctx, "read", func() {
			defer pd.gs.done()
			c.readLoop(pd, 0, pd.hc.io.R)
		})
	}
	if write {
		c.startWriterLocked(pd)
	}
}

// startWriterLocked starts the lane-0 writer of pd.
func (c *Conn) startWriterLocked(pd *pipeDrv) {
	if pd.hc.io.W == nil {
		return
	}
	done := make(chan struct{})
	pd.wstop, pd.wdone = false, done
	c.wg.Add(1)
	pd.gs.add()
	ready := pd.chReady
	labels.Go(pd.lctx, "write", func() {
		defer pd.gs.done()
		defer close(done)
		if ready != nil {
			<-ready // the channel's receiver is registered (startChannel)
		}
		c.writeLoop(pd, 0, pd.wsig, nil)
	})
}

// addBootstrap attaches pipe 0 and starts its goroutines (under the lock).
func (c *Conn) addBootstrap(p PipeIO) {
	pd := c.newPipeDrv(0, p)
	pd.nest, _ = nestPipe(c, outerOf(p)) // a new session cannot form a loop
	low := pd.tcp.setLowWater(singleLowWater)
	info := carrierInfo(p, pd.tcp, low)
	c.s.AttachBootstrap(info, info, pd.wsig)
	c.s.SetPipeKind(0, uint8(p.Carrier)) // before its first record
	if d, ok := pd.tcp.minRTT(); ok {
		c.s.SetCarrierMinRTT(core.Duration(d))
	}
	c.startPipeLocked(pd, true, true)
}

// bootstrapExporter is the late exporter that the reader of lane 0 of the
// bootstrap pipe loads before delivering the carrier's first bytes, nil
// for any other reader.
func (c *Conn) bootstrapExporter(pd *pipeDrv, lane uint64) *lateExporter {
	if pd.id != 0 || lane != 0 {
		return nil
	}
	return c.exp
}

// readLoop feeds what a carrier half delivers into the core: the bytes of
// a byte stream, or the messages of a message carrier (FramingRecords),
// each of at most MaxRecordSize bytes. It never waits for anything but the
// carrier (its handshake, its early-data verdict), so reads never wait
// behind writes. A quiet byte stream blocks on a small buffer and holds no
// pooled read buffer.
func (c *Conn) readLoop(pd *pipeDrv, lane uint64, r io.Reader) {
	defer c.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	var mr MessageReader
	if pd.framing == FramingRecords {
		var ok bool
		if mr, ok = r.(MessageReader); !ok {
			mr = noMessages{}
		}
	}
	exp := c.bootstrapExporter(pd, lane)
	first := true
	var small [idleBufSize]byte
	var buf *core.Buf
	var msg []byte
	used := 0
	reads := 0
	for {
		var n int
		var err error
		switch {
		case mr != nil:
			msg, err = mr.ReadMessage(MaxRecordSize)
		case buf == nil:
			n, err = r.Read(small[:])
			if n > 0 {
				buf = getReadBuf()
				copy(buf.B, small[:n])
				used = n
			}
		default:
			n, err = r.Read(buf.B[used:])
			used += n
		}
		got := n > 0 || mr != nil && err == nil
		if first && got {
			first = false
			c.awaitVerdict(pd, lane)
		}
		if exp != nil && got {
			if lerr := exp.load(c.done); lerr != nil {
				// What was read waited for a handshake that failed.
				err, used, msg = lerr, 0, nil
			}
			exp = nil
		}
		consumed, dead := 0, false
		// A deferred unlock turns a panic of the core into a closed
		// session (Conn.unlock) instead of leaving the lock held.
		func() {
			c.lock()
			defer c.unlock()
			if pd.dead { // replaced (an early-data restart): its ID is another pipe's now
				dead = true
				return
			}
			now := c.clk.now()
			switch {
			case mr != nil && err == nil:
				c.s.ReceiveMessage(pd.id, lane, &core.Buf{B: msg}, msg, now)
			case used > 0:
				consumed = c.s.Receive(pd.id, lane, buf, buf.B[:used], now)
			}
			if err != nil {
				graceful := errors.Is(err, io.EOF)
				var cause error
				if !graceful {
					cause = err
				}
				c.s.PipeEnded(pd.id, lane, graceful, used > consumed, cause, now)
			}
			if reads++; reads&63 == 0 && pd.id == 0 && lane == 0 {
				// The carrier's own minimum RTT feeds auto-tuning.
				if d, ok := pd.tcp.minRTT(); ok {
					c.s.SetCarrierMinRTT(core.Duration(d))
				}
			}
		}()
		msg = nil
		if dead {
			if buf != nil {
				buf.Unref()
			}
			return
		}
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

// noMessages is the message source of a Records carrier whose receiving
// half reads no messages: it fails the pipe.
type noMessages struct{}

func (noMessages) ReadMessage(int) ([]byte, error) {
	return nil, errors.New("zmux: Records framing needs a MessageReader")
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
	// The carrier's unsent count: the half's own, else the TCP socket's.
	var unsentOf func() (uint64, bool)
	if u, ok := w.(UnsentReporter); ok {
		unsentOf = u.Unsent
	} else if lo == nil && pd.tcp.rc != nil {
		unsentOf = pd.tcp.unsent
	}
	// The carrier's retransmissions (lane 0: lanes share their carrier).
	var retransOf func() (uint64, bool)
	if lo == nil {
		if r, ok := w.(RetransReporter); ok {
			retransOf = r.Retransmitted
		} else if pd.tcp.rc != nil {
			retransOf = pd.tcp.retransmitted
		}
	}
	var retransAt time.Time
	var werr error
	retired, ended := pd.end.retired, false
	for {
		// Read outside the session lock.
		var unsent, retrans uint64
		var unsentOK, retransOK bool
		if unsentOf != nil {
			unsent, unsentOK = unsentOf()
		}
		if retransOf != nil {
			if t := time.Now(); t.Sub(retransAt) >= time.Second {
				retransAt = t
				retrans, retransOK = retransOf()
			}
		}
		if lo == nil && pd.takeShutdown() {
			pd.hc.shutdownWrite()
		}
		if lo != nil && lo.takeShut() {
			lo.close(false)
		}
		produced, closing, dead := false, false, false
		func() {
			c.lock()
			defer c.unlock()
			if pd.dead || lo == nil && pd.wstop {
				if reported {
					b.Discard()
					reported = false
				}
				dead = true
				return
			}
			now := c.clk.now()
			if unsentOK {
				c.s.SetCarrierUnsent(pd.id, lane, unsent)
			}
			if retransOK {
				c.s.SetCarrierRetransmitted(pd.id, retrans)
			}
			if reported {
				c.s.WriteDone(b, werr, now)
				reported = false
			}
			if werr == nil {
				if b == nil {
					b = core.GetBatch()
				}
				produced = c.s.PollTransmit(pd.id, lane, b, core.DefaultBatch, now)
			}
			closing = c.closing
		}()
		if dead {
			return
		}
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
		if closing || werr != nil || ended {
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
		case <-retired:
			// The attachment is gone: one more pass for what was queued
			// (a shutdown), then the writer ends.
			retired, ended = nil, true
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
// net.Conn or a BuffersWriter).
func writeBatch(w io.Writer, framing Framing, b *core.Batch) (int, error) {
	if bw, ok := w.(batchWriter); ok && framing == FramingByteStream {
		return bw.WriteBatch(b)
	}
	if framing == FramingRecords {
		msgs := b.Messages()
		n := 0
		if bw, ok := w.(MessageBatchWriter); ok {
			for _, m := range msgs {
				n += len(m)
			}
			if err := bw.WriteMessages(msgs); err != nil {
				return 0, err
			}
			return n, nil
		}
		mw, ok := w.(MessageWriter)
		if !ok {
			return 0, errors.New("zmux: Records framing needs a MessageWriter")
		}
		for _, m := range msgs {
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
	if tlsRecords(w) {
		// A gather write would cost a TLS record per buffer.
		return carrierio.WriteFlat(w, bufs)
	}
	if nc, ok := w.(net.Conn); ok {
		nb := net.Buffers(bufs)
		n, err := nb.WriteTo(nc)
		return int(n), err
	}
	if bw, ok := w.(BuffersWriter); ok {
		return bw.WriteBuffers(bufs)
	}
	// Any other writer (a transform) gets one write per batch.
	return carrierio.WriteFlat(w, bufs)
}

// ---- lanes ----

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
	info := core.CarrierInfo{Framing: core.Framing(pd.framing), Lanes: true, MaxRecord: pd.maxRec}
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
	labels.Go(pd.lctx, "lane-write", func() {
		defer pd.gs.done()
		c.writeLoop(pd, lane, lo.sig, lo)
	})
}

// ---- carrier commands ----

// pendingAct is a carrier command with its attachment, resolved under the
// lock in the critical section that queued it: a retired attachment leaves
// c.pipes in a later critical section, which may run before the command is
// performed, and a command looked up then would be lost (an aborted half
// that is never closed blocks its goroutine for good).
type pendingAct struct {
	pd     *pipeDrv
	lo     *laneOut
	reason string
	a      core.Action
	code   ApplicationErrorCode
	start  bool
	// coded: the session has ended, with an application close of code and
	// reason or (0, "") otherwise, before this command ended the pipe
	// (CloseCodeRecorder). The
	// shutdown of a closing session's pipe comes before its inbound side
	// ends, so the code is recorded before either half has ended.
	coded bool
}

// resolveActionLocked binds a to its attachment (under the lock).
func (c *Conn) resolveActionLocked(a core.Action) pendingAct {
	pa := pendingAct{a: a, pd: c.pipes[a.Pipe]}
	if pd := pa.pd; pd != nil {
		pa.lo = pd.lanes[a.Lane]
		if a.Kind == core.ActOpenLane && pd.opener != nil && !c.closing {
			c.wg.Add(1)
			pa.start = true
		}
		if a.Kind == core.ActShutdown || a.Kind == core.ActAbort || a.Kind == core.ActClose {
			if e := c.s.Err(); e != nil {
				pa.coded = true // 0 and no reason after any end but an application close
				if e.Kind == core.CloseApplication {
					pa.code, pa.reason = ApplicationErrorCode(e.Code), e.Reason
				}
			}
		}
	}
	return pa
}

// doAction performs a carrier command outside the lock.
func (c *Conn) doAction(pa pendingAct) {
	a, pd, lo, start := pa.a, pa.pd, pa.lo, pa.start
	if pd == nil {
		return
	}
	switch a.Kind {
	case core.ActShutdown:
		if pa.coded {
			pd.hc.recordCloseCode(pa.code, pa.reason)
		}
		if a.Lane == 0 {
			pd.requestShutdown()
		} else if lo != nil {
			lo.requestShut()
		}
	case core.ActAbort:
		if pa.coded {
			pd.hc.recordCloseCode(pa.code, pa.reason)
		}
		pd.closeAll()
	case core.ActClose:
		if pa.coded {
			pd.hc.recordCloseCode(pa.code, pa.reason)
		}
		if a.Dir == core.DirBidi {
			pd.closeGraceful(time.Duration(a.After)) // the peer's CONNECTION_CLOSE was read: SPEC Section 6.5 step 4
			return
		}
		if a.Dir&core.DirIn != 0 {
			pd.hc.closeRead()
		}
		if a.Dir&core.DirOut != 0 {
			pd.hc.shutdownWrite()
		}
	case core.ActOpenLane:
		if start {
			if !c.goSide(pd.lctx, "lane-open", func() { c.openLane(pd) }) {
				c.wg.Done() // the session ended: no lane opens
			}
		} else if pd.opener == nil {
			// A full critical section: the wakes and actions the failure
			// produces run now, and a panic poisons the session.
			func() {
				c.lock()
				defer c.unlock()
				c.s.LaneOpenFailed(pd.id)
			}()
		}
	case core.ActResetLane:
		if lo != nil {
			lo.close(true)
		}
	}
}

// closeAll aborts every half and lane of the attachment.
func (pd *pipeDrv) closeAll() {
	pd.smu.Lock()
	t := pd.graceT
	pd.smu.Unlock()
	if t != nil {
		// Closed gracefully before: the abort the bound holds runs now.
		if t.Stop() {
			pd.graceAbort()
		}
		return
	}
	pd.closeHalves(true)
}

// closeGraceful closes every half and lane of the attachment without
// aborting: the write sides end as a close, never as a reset. A carrier
// whose graceful close has not ended after bound is aborted then (SPEC.md
// Section 6.5 step 4): a close never leaves the carrier's work unbounded.
func (pd *pipeDrv) closeGraceful(bound time.Duration) {
	lanes := pd.closeHalves(false)
	pd.smu.Lock()
	defer pd.smu.Unlock()
	if pd.graceT != nil {
		return
	}
	for _, l := range lanes {
		if a, ok := l.w.(Aborter); ok {
			pd.graceAb = append(pd.graceAb, a)
		}
	}
	// Host clock: a bound on carrier IO (Time). Nothing tells when the
	// carrier's own close work ends, so the abort runs at the bound (on a
	// carrier that already ended it does nothing), or earlier when the
	// attachment is aborted meanwhile (closeAll).
	pd.graceT = time.AfterFunc(bound, pd.graceAbort)
}

// graceAbort aborts what closeGraceful closed: the pipe's write side and
// its lanes.
func (pd *pipeDrv) graceAbort() {
	pd.hc.abort(net.ErrClosed)
	for _, a := range pd.graceAb {
		a.Abort(net.ErrClosed)
	}
}

// closeHalves closes every half and lane of the attachment and returns the
// outbound lanes it closed.
func (pd *pipeDrv) closeHalves(abort bool) []*laneOut {
	pd.endChannel()
	pd.hc.closeAll(abort, net.ErrClosed)
	var lanes []*laneOut
	var in []io.ReadCloser
	pd.c.peek(func() {
		lanes = make([]*laneOut, 0, len(pd.lanes))
		for _, l := range pd.lanes {
			lanes = append(lanes, l)
		}
		in = pd.inLanes
		pd.inLanes = nil
	})
	for _, l := range lanes {
		l.close(abort)
	}
	for _, r := range in {
		_ = r.Close()
	}
	return lanes
}

// fail closes the session after a panic outside the lock.
func (c *Conn) fail(p any) {
	c.lock()
	defer c.unlock()
	c.poison(p)
}

// ---- orphaned handles (GC cleanup) ----

// orphanQueue is a lock-free stack of the handles collected unclosed: the
// runtime's cleanup goroutine pushes, the session takes them all under its
// lock.
type orphanQueue struct{ head atomic.Pointer[handle] }

func (q *orphanQueue) push(h *handle) {
	for {
		old := q.head.Load()
		h.orphanNext = old
		if q.head.CompareAndSwap(old, h) {
			return
		}
	}
}

// orphanSweep bounds how long a handle collected unclosed waits for its
// release on a session without other activity: while the session has
// unreleased handles its timer fires at least this often.
const orphanSweep = 5 * time.Second

// releaseOrphansLocked releases the handles collected unclosed (under the
// lock); every critical section calls it.
func (c *Conn) releaseOrphansLocked() {
	if c.orphans.head.Load() == nil {
		return
	}
	for h := c.orphans.head.Swap(nil); h != nil; {
		next := h.orphanNext
		h.orphanNext = nil
		h.released(c.s.Release(h.st))
		h = next
	}
}
