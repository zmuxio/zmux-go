package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// pipeDrv owns the goroutines and carrier halves of one attachment.
// Ownership: a reader goroutine owns its half and its read buffer; a writer
// goroutine owns its half (the write token) and its batch; halves are
// closed through hc (lane 0) or their laneOut exactly once each.
type pipeDrv struct {
	physical core.CarrierKeys
	tcp      tcpInfo         // the TCP socket under the carrier, if any
	opener   LaneOpener      // nil: the carrier offers no lanes
	lctx     context.Context // the goroutine labels of the pipe (labels.go)
	admitted any             // the admission value of the carrier that joined it (set once, under the session lock)
	// Guarded by the session lock.
	openCtx  context.Context
	stopOpen context.CancelFunc
	lanes    map[uint64]*laneOut       // outbound lanes above 0
	inLanes  map[*inboundLane]struct{} // inbound lanes above 0
	// The carrier's datagram channel (nil: none) and its sender.
	ch      DatagramChannel
	sampler *carrierstats.Sampler
	c       *Conn
	hc      *halfCloser
	nest    *nestEdge // the nesting edge of a stream-backed carrier (nil: none)
	chSig   *core.Signal
	chEnd   chan struct{}
	// chReady is closed once the channel's receiver is registered: the
	// first record waits for it.
	chReady chan struct{}
	// graceT is the abort bound of a graceful close (closeGraceful), with
	// the abort action for its halves and lanes, under the session lock.
	graceT  *time.Timer
	graceAb func()
	wdone   chan struct{} // closed when a stopped lane-0 writer releases its half
	// The end of the attachment (Pipe.Wait, Pipe.Done); a handle keeps it
	// after the session has forgotten the attachment.
	end                       *pipeEnd
	accountSegment            *accountSegment
	accountRead, accountWrite *accountHalf
	gs                        pipeGoroutines // the attachment's goroutines (Pipe.Done waits for them)
	writer                    laneWriter     // lane zero
	sendCarrier, recvCarrier  CarrierID
	id                        uint64
	maxRec                    int // the pipe's local record limit (carrierRecordLimit), applied to every lane
	chOnce                    sync.Once
	channelAccountingClosed   bool // guarded by Conn.chIn.mu
	hasCarrierSlot            bool
	physicalReserved          bool
	retiring                  bool // under the session lock: the carrier close has claimed the lanes.
	shutReq                   bool // under the session lock: lane 0's shutdown was requested (sticky)
	framing                   Framing
	dead                      bool // guarded by the session lock: replaced (early-data restart), its goroutines report nothing
	// wstop (session lock): the lane-0 writer stops before writing again
	// (an early-data restart on the same halves).
	wstop    bool
	hasOwner bool // holds a close reference until retirement
}

// pipeEnd is how an attachment ended: retired is closed when the session
// retires it (err, guarded by the session lock, is set before),
// waited when, besides, its goroutines ended.
type pipeEnd struct {
	accounting *pipeAccounting // published under the session lock at carrier handoff
	join       *joinOutcome    // on additional pipes, persists through retirement
	err        error
	drv        atomic.Pointer[pipeDrv] // the current driver (an early-data restart replaces pipe 0's)
	retired    chan struct{}
	// waited is Pipe.Done's channel, made on demand; ended is set once
	// the pipe was retired and its goroutines ended, or the session ended.
	waited chan struct{}
	// opts are the pipe's options as the application set them, every
	// update applied (guarded by the session lock, like err); a pipe the
	// application added has its Direction, RecordMode and FateGroup resolved.
	// Pipe.Options reads them, also once the pipe is gone.
	opts  PipeOptions
	mu    sync.Mutex
	ended bool
}

// done returns Pipe.Done's channel, without a goroutine to close it.
func (e *pipeEnd) done() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return closedChan
	}
	if e.waited == nil {
		e.waited = make(chan struct{})
	}
	return e.waited
}

// end closes Pipe.Done's channel.
func (e *pipeEnd) end() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ended {
		return
	}
	e.ended = true
	if e.waited != nil {
		close(e.waited)
	}
}

// goneEnd is the end of an attachment the session no longer knows.
var goneEnd = &pipeEnd{retired: closedChan, waited: closedChan, ended: true}

// laneOut is the writer side of one outbound lane above 0.
type laneOut struct {
	w         io.WriteCloser
	account   *accountHalf
	writer    laneWriter
	lane      uint64
	once      sync.Once
	abortOnce sync.Once
}

func (l *laneOut) abort() {
	l.abortOnce.Do(func() {
		if a, ok := l.w.(Aborter); ok {
			safeLog(nil, "lane Abort", func() { a.Abort(net.ErrClosed) })
		}
	})
}

func (l *laneOut) close(abort bool) {
	l.account.retire()
	if abort {
		l.abort()
	}
	l.once.Do(func() { carrierio.CloseHalf(l.w) })
}

// pipeGoroutines counts an attachment's goroutines without a goroutine of
// its own to wait for them (lock level L3).
type pipeGoroutines struct {
	// fin is closed once finish was called and the count is zero, by
	// whichever of the two comes last; made on demand (finished).
	fin chan struct{}
	// onFin, when set before finish, runs (under mu) when fin would close.
	afterIO      func() // Runs outside mu once the carrier closed and no worker remains.
	onFin        func()
	onAccounting func()
	n            int
	mu           sync.Mutex
	closed       bool // physical close returned; the last worker may release ownership
	sealed       bool // seal: no goroutine joins any more
	final        bool // finish: the owner ended
	fired        bool // the owner ended and no goroutine ran since
}

func (g *pipeGoroutines) add() {
	g.mu.Lock()
	g.n++
	g.mu.Unlock()
}

// tryAdd counts one more goroutine unless the count was sealed.
func (g *pipeGoroutines) tryAdd() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sealed {
		return false
	}
	g.n++
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
	g.n--
	g.closeFinLocked()
	after := g.takeAfterIOLocked()
	g.mu.Unlock()
	if after != nil {
		after()
	}
}

// finishThen is finish, with f run once no goroutine runs.
func (g *pipeGoroutines) finishThen(f func()) {
	g.mu.Lock()
	if g.fired {
		g.mu.Unlock()
		f()
		return
	}
	g.onFin = f
	g.final = true
	g.closeFinLocked()
	g.mu.Unlock()
}

// finish marks the owner ended: finished's channel closes once no
// goroutine runs, at once when none does.
func (g *pipeGoroutines) finish() {
	g.mu.Lock()
	g.final, g.closed = true, true
	g.closeFinLocked()
	after := g.takeAfterIOLocked()
	g.mu.Unlock()
	if after != nil {
		after()
	}
}

func (g *pipeGoroutines) takeAfterIOLocked() func() {
	if !g.closed || g.n != 0 {
		return nil
	}
	after := g.afterIO
	g.afterIO = nil
	return after
}

func (g *pipeGoroutines) closeFinLocked() {
	if !g.final || g.n != 0 || g.fired {
		return
	}
	g.fired = true
	if g.onAccounting != nil {
		g.onAccounting()
		g.onAccounting = nil
	}
	if g.fin != nil {
		close(g.fin)
	}
	if g.onFin != nil {
		g.onFin()
	}
}

// finished returns a channel closed once finish was called and no
// goroutine runs; no goroutine waits for it.
func (g *pipeGoroutines) finished() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fired {
		return closedChan
	}
	if g.fin == nil {
		g.fin = make(chan struct{})
	}
	return g.fin
}

var closedChan = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

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
	if sealed, ok := unprefixed(p.W).(*sealedWriter); ok {
		info.SealSuffix = sealTag
		if !sealed.msgs {
			info.SealPrefix = sealedLen
		}
	}
	info.Channel = channelOf(p) != nil
	info.OwnFailure = reportsFailure(p)
	return info
}

// reportsFailure reports whether p's carrier reports its own failure
// (FailureReporter): every half p has must report it, since a half that
// does not (a raw connection) can stall silently beside one that does.
// One value as both halves counts once.
func reportsFailure(p PipeIO) bool {
	switch {
	case p.R == nil:
		return halfReportsFailure(p.W)
	case p.W == nil:
		return halfReportsFailure(p.R)
	}
	return halfReportsFailure(p.W) && halfReportsFailure(p.R)
}

// preparedPipe holds the host properties sampled before committing an attachment.
type preparedPipe struct {
	pd      *pipeDrv
	carrier any
	outer   outerRef
	info    core.CarrierInfo
}

func (c *Conn) preparePipe(id uint64, p, raw PipeIO, lowWater int) (a preparedPipe, err error) {
	err = hostCall(c.cfg.Logger, "carrier properties", func() error {
		a.pd = c.newPipeDrv(id, p)
		a.pd.physical = physicalCarrierKeys(raw)
		policy := c.cfg.Accounting
		if policy.Enabled && policy.Strict {
			if err := checkAccountingSources(nil, policy.sourceLimit(), p.ReceiveSource, p.SendSource, p.R != nil, p.W != nil); err != nil {
				return err
			}
		}
		a.pd.tcp = tcpOf(raw.W)
		a.pd.sampler = primeCarrierSample(p.W)
		a.carrier, a.outer = carrierIdentity(p)
		low := lowWater > 0 && a.pd.tcp.setLowWater(lowWater)
		a.info = carrierInfo(p, a.pd.tcp, low)
		liveness, err := p.Liveness.core()
		if err != nil {
			return err
		}
		a.info.Liveness = liveness
		a.info.FateDomain = p.SendSource.FateDomain
		if p.SendSource.CarrierID != (CarrierID{}) {
			a.info.CarrierIdentity = p.SendSource.CarrierID
		} else if a.pd.sampler != nil {
			a.info.CarrierIdentity = carrierstats.Cached(a.pd.sampler).ID
		}
		a.pd.sendCarrier, a.pd.recvCarrier = p.SendSource.CarrierID, p.ReceiveSource.CarrierID
		if id, ok := a.info.CarrierIdentity.(CarrierID); ok {
			a.pd.sendCarrier = id
		}
		if p.R != nil && a.pd.recvCarrier == (CarrierID{}) {
			if carrierio.SameHalf(p.R, p.W) {
				a.pd.recvCarrier = a.pd.sendCarrier
			} else if receiver := samplerOf(p.R); receiver != nil {
				a.pd.recvCarrier = carrierstats.Cached(receiver).ID
			}
		}
		a.pd.maxRec = a.info.MaxRecord
		return nil
	})
	return a, err
}

// newPipeDrv samples the carrier before the session lock is taken.
func (c *Conn) newPipeDrv(id uint64, p PipeIO) *pipeDrv {
	pd := &pipeDrv{c: c, id: id, hc: newHalfCloser(p), framing: p.Framing}
	pd.end = &pipeEnd{retired: make(chan struct{})}
	pd.end.drv.Store(pd)
	if o, ok := p.W.(LaneOpener); ok {
		pd.opener = o
	} else if o, ok := p.R.(LaneOpener); ok {
		pd.opener = o
	}
	pd.ch = channelOf(p)
	return pd
}

// startPipeLocked starts the goroutines of an attachment.
//
// Every PipeIO that becomes part of a session starts here once, so this is
// where the padding of the handshake run on it is metered: an attachment
// that fails before (a join not confirmed) spends nothing.
func (c *Conn) startPipeLocked(pd *pipeDrv, read, write bool, carrier any, held *receiveFlight) {
	c.bindCarrierLocked(pd)
	pd.lctx = c.pipeLabels(pd.id)
	pd.hc.take(pd.closeAll)
	c.pipes[pd.id] = pd
	c.s.SetNow(c.clk.now())
	if !c.s.AdmitHandshake(pd.hc.io.HandshakeWrites, c.clk.wall(c.s.Now())) {
		c.s.FailInternal("carrier handshake exceeds shaping profile")
		return
	}
	if c.cfg.Accounting.Enabled {
		p := pd.hc.io
		a := pd.end.accounting
		if a == nil {
			a = newPipeAccounting(pd.id, c.cfg.Accounting)
			pd.end.accounting = a
		}
		pd.accountSegment = a.startSegment()
		if read && p.R != nil {
			pd.accountRead = pd.accountSegment.startHalf(p.ReceiveSource, false, 0)
		}
		if write && p.W != nil {
			pd.accountWrite = pd.accountSegment.startHalf(p.SendSource, true, 0)
		}
		pd.gs.onAccounting = pd.accountSegment.finish
		if out := pd.end.join; out != nil && out.attempt != nil {
			out.attempt.view.Attachments[out.index].Ticket = a
		}
	}
	c.s.SetCarrierConn(pd.id, carrier)
	if !read || !write {
		pd.worker("pipe-unused", func() {
			if !read {
				pd.hc.closeRead()
			} else {
				pd.hc.shutdownWrite()
			}
		})
	}
	c.startChannel(pd) // its receiver before the first record
	if read && pd.hc.io.R != nil {
		flight := *held
		*held = receiveFlight{}
		c.side.add()
		pd.gs.add()
		labels.Go(pd.lctx, "read", func() {
			defer pd.gs.done()
			defer flight.release()
			c.readLoop(pd, 0, pd.hc.io.R, pd.hc.closeRead, pd.accountRead)
		})
	}
	if write && pd.hc.io.W != nil {
		pd.writer.ctx = labels.With(pd.lctx, "zmux", "write")
		c.queuePipeWriterLocked(pd)
	}
}

// bindCarrierLocked transfers a real carrier slot to a driver after admission.
func (c *Conn) bindCarrierLocked(pd *pipeDrv) {
	if pd.hasCarrierSlot {
		return
	}
	pd.hasCarrierSlot = true
	if !pd.hasOwner {
		pd.hasOwner = true
		pd.gs.add() // retained until physical close returns
	}
	if !pd.physicalReserved {
		c.s.AcquireCarriers(pd.physical)
	}
	releaseSetup := pd.gs.afterIO
	pd.gs.afterIO = func() {
		c.locked(func() { c.s.ReleaseCarriers(pd.physical) })
		if releaseSetup != nil {
			releaseSetup()
		}
	}
}

// queuePipeWriterLocked rechecks work received before handover or
// while the early-data writer was stopped.
func (c *Conn) queuePipeWriterLocked(pd *pipeDrv) {
	if pd.writer.poll.Ready.Load() || pd.writer.shutdown != nil {
		c.s.QueueWriter(pd.id, 0, &pd.writer.poll)
	}
}

// worker starts attachment-owned work reserved under the session lock.
func (pd *pipeDrv) worker(name string, f func()) {
	pd.gs.add()
	pd.c.side.add()
	labels.Go(pd.lctx, name, func() {
		defer pd.c.side.done()
		defer pd.gs.done()
		safeLog(pd.c.cfg.Logger, name, f)
	})
}

const writerIdle = time.Second

// A negative idle deadline means the session timer has requested exit.
const writerExpired core.Time = -1

// laneWriter serializes output on one carrier half. sessMu guards its state.
type laneWriter struct {
	ctx      context.Context
	shutdown <-chan struct{} // pending half-close, ready after the close code is recorded
	wake     core.Signal
	idle     core.Deadline[*laneWriter]
	idleAt   core.Time // zero while active
	starts   uint64    // reservations over this lane
	poll     core.WriteSignal
	running  bool
}

func (c *Conn) expireWritersLocked(now core.Time) {
	first := c.writerIdle.PopDue(now)
	if first == nil {
		return
	}
	c.s.TrimSendReserve()
	for d := first; d != nil; d = c.writerIdle.PopDue(now) {
		w := d.Value
		w.idleAt = writerExpired
		w.wake.FireLocked()
	}
}

type writerStart struct {
	pd     *pipeDrv
	lo     *laneOut
	writer *laneWriter
	lane   uint64
}

func (c *Conn) reserveWritersLocked(starts []writerStart) []writerStart {
	requests := c.s.TakePollRequests()
	for _, req := range requests {
		pd := c.pipes[req.Pipe]
		if c.closing || pd == nil || pd.retiring || pd.dead {
			continue
		}
		select {
		case <-pd.end.retired:
			continue
		default:
		}
		if req.Lane == 0 {
			if !pd.wstop && req.Matches(&pd.writer.poll) {
				starts = c.reserveWriterLocked(starts, pd, nil, &pd.writer, 0)
			}
		} else if lo := pd.lanes[req.Lane]; lo != nil && req.Matches(&lo.writer.poll) {
			starts = c.reserveWriterLocked(starts, pd, lo, &lo.writer, req.Lane)
		}
	}
	clear(requests)
	return starts
}

func (c *Conn) reserveWriterLocked(starts []writerStart, pd *pipeDrv, lo *laneOut, w *laneWriter, lane uint64) []writerStart {
	if w.ctx == nil || w.shutdown == nil && !w.poll.Ready.Load() {
		return starts
	}
	if w.running {
		if w.idleAt != 0 {
			w.wake.FireLocked()
		}
		return starts
	}
	w.wake.Make()
	w.starts++
	w.running = true
	pd.gs.add()
	c.side.add()
	return append(starts, writerStart{pd, lo, w, lane})
}

// releaseLocked ends ownership of the carrier half before the epilogue can
// reserve it again. The worker counts cover the remaining lock-free epilogue.
func (s writerStart) releaseLocked() {
	s.pd.c.writerIdle.Cancel(&s.writer.idle)
	s.writer.idleAt = 0
	s.writer.running = false
	if s.lo == nil && s.pd.wdone != nil {
		close(s.pd.wdone)
		s.pd.wdone = nil
	}
}

func (s writerStart) run() {
	defer s.pd.c.side.done()
	defer s.pd.gs.done()
	pprof.SetGoroutineLabels(s.writer.ctx)
	if s.lo == nil && s.pd.chReady != nil {
		<-s.pd.chReady
	}
	s.pd.c.writeLoop(s)
}

// addBootstrap attaches pipe 0 and starts its goroutines (under the lock).
func (c *Conn) addBootstrap(a preparedPipe, wait *attachment, held *receiveFlight) {
	pd := a.pd
	pd.nest, _ = nestPipe(c, a.outer) // a new session cannot form a loop
	c.s.AttachBootstrap(a.info, a.info, &pd.writer.poll)
	if p := pd.hc.io.Liveness; p != (PipeLiveness{}) {
		pd.end.opts.Liveness = Set(p)
	}
	c.s.SetPipeKind(0, uint8(pd.hc.io.Carrier)) // before its first record
	if d, ok := pd.tcp.minRTT(); ok {
		c.s.SetCarrierMinRTT(core.Duration(d))
	}
	wait.attach(pd)
	c.startPipeLocked(pd, true, true, a.carrier, held)
}

// receiveFlight owns the space in which one reader may complete a record.
// A nested reader may retain one range descriptor per byte of a record;
// the underlying payload is already owned by the source's lease.
type receiveFlight struct {
	ledger *core.Budget
	owner  *core.Session
	bytes  int64
}

func reserveReceiveFlight(b *core.Budget, r io.Reader) (receiveFlight, error) {
	if b == nil || r == nil {
		return receiveFlight{}, nil
	}
	n := receiveFlightSize(r)
	if !b.TryCharge(uint64(n)) {
		return receiveFlight{}, ErrMemoryBudget
	}
	return receiveFlight{ledger: b, bytes: n}, nil
}

func receiveFlightSize(r io.Reader) int64 {
	n := int64(3 * core.RecvBlockSize)
	if byteSpanReader(r) != nil {
		n += MaxRecordSize * 2 * 32 // descriptor capacity during geometric growth
	}
	return n
}

func (c *Conn) reserveReceiveFlight(r io.Reader) (receiveFlight, error) {
	if r == nil {
		return receiveFlight{}, nil
	}
	n := receiveFlightSize(r)
	if !c.s.ReserveFlight(n) {
		return receiveFlight{}, ErrMemoryBudget
	}
	return receiveFlight{owner: c.s, bytes: n}, nil
}

func (f receiveFlight) release() {
	if f.owner != nil {
		f.owner.ReleaseFlight(f.bytes)
	} else if f.ledger != nil {
		f.ledger.Release(uint64(f.bytes))
	}
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

// readLoop delivers owned, bounded input without waiting for the writer.
func (c *Conn) readLoop(pd *pipeDrv, lane uint64, r io.Reader, closeRead func(), account *accountHalf) {
	defer c.side.done()
	defer func() {
		if p := recover(); p != nil {
			c.fail(p)
		}
	}()
	if lane != 0 {
		defer closeRead() // A replaced driver or a failed reader also releases its lane.
	}
	if account != nil {
		defer account.retire()
	}
	messages := pd.framing == FramingRecords
	limit := MaxRecordSize
	if pd.maxRec > 0 {
		limit = min(limit, pd.maxRec)
	}
	exp := c.bootstrapExporter(pd, lane)
	first := true
	var br byteReadState
	defer br.release()
	var record ReceivedMessage
	defer record.Release()
	reads := 0
	for {
		var input core.Input
		var n int
		var err error
		switch {
		case messages:
			record, err = readMessage(r, limit)
		default:
			n, input, err = br.read(r)
		}
		got := n > 0 || messages && err == nil
		if messages && got {
			err = record.end
		}
		if first && got {
			first = false
			c.awaitVerdict(pd, lane)
		}
		if exp != nil && got {
			if lerr := exp.load(c.done); lerr != nil {
				err, input, got = lerr, core.Input{}, false
			}
			exp = nil
		}
		var fault *core.PhysicalFault
		var graceful bool
		if err != nil {
			fault = carrierFaultEvidence(err)
			graceful = fault == nil && errors.Is(err, io.EOF)
		}
		var tally RecordTotals
		if account != nil {
			switch {
			case messages:
				tally = accountMessageRead(&record, err, graceful)
			default:
				tally = tallyStreamRead(input.Bytes())
			}
		}
		consumed, dead := 0, false
		func() {
			c.lock()
			defer c.unlock()
			if pd.dead { // replaced: its ID may already belong to another attachment
				dead = true
				if account != nil {
					account.add(tally, true)
				}
				return
			}
			now := c.clk.now()
			switch {
			case messages && got:
				switch c.s.ReceiveMessage(pd.id, lane, record.span, now) {
				case core.RecordValid:
					tally.Valid = 1
				case core.RecordProtocolInvalid:
					tally.ProtocolInvalid = 1
				case core.RecordUnclassified:
				}
			case input.Len() > 0 && account != nil:
				var validation core.RecordValidations
				consumed = c.s.ReceiveTracked(pd.id, lane, input, now, &validation)
				tally.Valid, tally.ProtocolInvalid = validation.Valid, validation.ProtocolInvalid
			case input.Len() > 0:
				consumed = c.s.Receive(pd.id, lane, input, now)
			}
			if account != nil {
				if err == nil && consumed < input.Len() {
					tally.PartialBytes = 0 // The reader keeps the unfinished suffix for the next batch.
				}
				account.add(tally, err != nil)
			}
			if err != nil {
				cause := err
				// Multipath decides whether the end was expected from the
				// attachment's drain and close state.
				if graceful && !c.s.Features().Multipath {
					cause = nil
				}
				c.s.PipeEnded(pd.id, lane, graceful, input.Len() > consumed, cause, fault, now)
			}
			if reads++; reads&63 == 0 && pd.id == 0 && lane == 0 {
				if d, ok := pd.tcp.minRTT(); ok {
					c.s.SetCarrierMinRTT(core.Duration(d))
				}
			}
		}()
		record.Release()
		if dead {
			return
		}
		c.passDatagrams()
		if err != nil {
			if lane == 0 {
				closeRead()
			}
			return
		}
		if !messages {
			br.advance(consumed)
		}
	}
}

// writeLoop produces batches for one outbound lane and writes them. It owns
// the lane's write side: no other goroutine writes to it.
func (c *Conn) writeLoop(start writerStart) {
	pd, lane, lo := start.pd, start.lane, start.lo
	var b *core.Batch
	var gather net.Buffers
	var written int
	var werr error
	var fault *core.PhysicalFault
	var reported bool // b was written and WriteDone is owed
	defer func() {
		panicValue := recover()
		if panicValue != nil || reported || b != nil && b.Handoffs() {
			c.locked(func() {
				if reported {
					c.s.WriteDone(b, written, werr, c.clk.now(), fault)
				} else if b != nil && b.Handoffs() {
					b.Discard()
				}
				if panicValue != nil {
					if b != nil {
						c.s.ShapeWriteUnknown(b, b.EncodedLen())
					}
					// A panicking carrier cannot accept another batch.
					start.writer.ctx = nil
					start.releaseLocked()
					c.poison(panicValue)
				}
			})
		}
		if b != nil {
			core.PutBatch(b)
		}
	}()
	var w io.Writer = pd.hc.io.W
	account := pd.accountWrite
	if lo != nil {
		w, account = lo.w, lo.account
	}
	sealedOverhead := 0
	if account != nil {
		if sealed, ok := unprefixed(w).(*sealedWriter); ok {
			sealedOverhead = sealed.framedOverhead()
		}
	}
	// The carrier's unsent count: the half's own, else the TCP socket's.
	var unsentOf func() (uint64, bool)
	if u, ok := w.(UnsentReporter); ok {
		unsentOf = u.Unsent
	} else if lo == nil && pd.tcp.rc != nil {
		unsentOf = pd.tcp.unsent
	}

	var ending bool

	for {
		// A carrier with a backlog reporter samples outside the session
		// lock. Without one (including QUIC), the claim can consume a
		// request published up to the point of the claim under the lock.
		requested := unsentOf != nil && start.writer.poll.Ready.Load()
		var unsent uint64
		var unsentOK bool
		if requested && werr == nil {
			unsent, unsentOK = unsentOf()
		}
		var shutdown <-chan struct{}
		produced, stop, wait := false, false, false
		func() {
			c.lock()
			if !requested && !reported && !ending && !pd.dead && !pd.wstop && !pd.retiring && !c.closing &&
				start.writer.idleAt > 0 && !start.writer.poll.Ready.Load() && start.writer.shutdown == nil {
				// The timer still owns this parked writer. A stale token
				// changes neither protocol state nor its expiry.
				start.writer.wake.Drain()
				wait = true
				c.unlockPeek()
				return
			}
			defer c.unlock()
			defer func() {
				if stop {
					start.releaseLocked()
				}
			}()
			if pd.dead || lo == nil && pd.wstop {
				if reported {
					reported = false
					c.s.WriteDone(b, written, werr, c.clk.now(), fault)
				}
				stop = true
				return
			}
			if start.writer.idleAt == writerExpired && !start.writer.poll.Ready.Load() && start.writer.shutdown == nil {
				stop = true
				return
			}
			c.writerIdle.Cancel(&start.writer.idle)
			start.writer.idleAt = 0
			now := c.clk.now()
			if unsentOK {
				c.s.SetCarrierUnsent(pd.id, lane, unsent, now)
			}
			if reported {
				reported = false
				c.s.WriteDone(b, written, werr, now, fault)
			}
			shutdown = start.writer.shutdown
			start.writer.shutdown = nil
			if shutdown == nil && werr == nil && (unsentOf == nil || requested) && c.s.ClaimWrite(pd.id, lane, &start.writer.poll) {
				if b == nil {
					b = core.GetBatch()
				}
				produced = c.s.PollTransmit(pd.id, lane, b, core.DefaultBatch, now)
			}
			stop = !produced && shutdown == nil && (werr != nil || c.closing || pd.retiring || ending)
			if !stop && !produced && shutdown == nil && !start.writer.poll.Ready.Load() {
				start.writer.idleAt = now.Add(core.Duration(writerIdle))
				start.writer.idle.Value = start.writer
				c.writerIdle.Set(&start.writer.idle, start.writer.idleAt)
				start.writer.wake.Drain()
				wait = true
			}
		}()
		if stop {
			return
		}
		if wait {
			if b != nil {
				core.PutBatch(b)
				b = nil
			}
			select {
			case <-start.writer.wake.C:
			case <-pd.end.retired:
				ending = true
			case <-c.done:
				ending = true
			}
			continue
		}
		if produced {
			if account != nil {
				b.RequireProgress()
			}
			var progress func(int)
			if b.TracksShapeProgress() {
				progress = func(n int) { c.locked(func() { c.s.ShapeWriteProgress(b, n, c.clk.now()) }) }
			}
			written, werr = writeBatch(w, pd.framing, b, &gather, progress)
			if pd.framing != FramingRecords && werr == nil && written != b.Len() {
				werr = io.ErrShortWrite
			}
			if werr != nil {
				if e, ok := errors.AsType[*unknownWriteError](werr); ok {
					c.locked(func() { c.s.SetNow(c.clk.now()); c.s.ShapeWriteUnknown(b, e.end) })
				}
				fault = carrierFaultEvidence(werr)
			}
			reported = true
			if account != nil {
				framed := pd.framing == FramingRecords
				bytes, records, partial := b.Tally(written, framed, sealedOverhead)
				imprecise := werr != nil && framed
				if progress, ok := errors.AsType[*recordWriteError](werr); ok {
					imprecise = false
					if !progress.complete {
						partial = progress.bytes
					}
				}
				account.add(RecordTotals{Bytes: bytes, Records: records, PartialBytes: partial, Imprecise: imprecise}, werr != nil)
			}
			if b.Shutdown {
				shutdown = closedChan
			}
		}
		if shutdown != nil {
			<-shutdown
			if lo == nil {
				account.retire()
				pd.hc.shutdownWrite()
			} else {
				lo.close(false)
			}
		}
	}
}

// batchWriter takes a batch without a copy (a stream carrying a nested
// session's pipe).
type batchWriter interface {
	WriteBatch(b *core.Batch) (int, error)
}

// unknownWriteError marks a terminal carrier result with an unidentified accepted
// prefix. end is the encoded end of the attempted unit within the batch.
type unknownWriteError struct {
	cause error
	end   int
}

func (e *unknownWriteError) Error() string { return e.cause.Error() }
func (e *unknownWriteError) Unwrap() error { return e.cause }

// observeWrite distinguishes a carrier refusal from a lost acceptance report.
func observeWrite(write func() error) error {
	returned := false
	err := hostCall(nil, "carrier write", func() error {
		err := write()
		returned = true
		return err
	})
	if !returned {
		return &unknownWriteError{cause: err}
	}
	return err
}

// writeBatch hands a batch to the carrier in one call (gather write on a
// net.Conn or a BuffersWriter).
func writeBatch(w io.Writer, framing Framing, b *core.Batch, gather *net.Buffers, progress func(int)) (int, error) {
	if bw, ok := w.(batchWriter); ok && framing == FramingByteStream {
		return bw.WriteBatch(b)
	}
	if framing == FramingRecords {
		msgs := b.Messages()
		n := 0
		if bw, ok := w.(MessageBatchWriter); ok && !b.NeedsProgress() {
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
		write := mw.WriteMessage
		if b.NeedsProgress() {
			write = func(m []byte) error {
				return observeWrite(func() error { return mw.WriteMessage(m) })
			}
		}
		for i, m := range msgs {
			var err error
			if sealed, ok := unprefixed(w).(*sealedWriter); ok && progress != nil && !sealed.msgs {
				err = sealed.writeMessages([][]byte{m}, func(prefix int) { progress(n + i*b.EnvelopeSize() + prefix) })
			} else {
				err = write(m)
			}
			if err != nil {
				if unknown, ok := errors.AsType[*unknownWriteError](err); ok {
					return n, &unknownWriteError{cause: unknown.cause, end: n + len(m) + (i+1)*b.EnvelopeSize()}
				}
				if progress, ok := errors.AsType[*recordWriteError](err); ok && progress.complete {
					n += len(m)
				}
				return n, err
			}
			n += len(m)
			if progress != nil {
				progress(n + (i+1)*b.EnvelopeSize())
			}
		}
		return n, nil
	}
	if progress != nil {
		// Keep the immutable layout until every short-write suffix is submitted.
		if bufs := b.Bufs(); len(bufs) == 1 {
			return writeSuffix(w, bufs[0], progress)
		}
		return writeSuffix(w, b.Flat(), progress)
	}
	if _, ok := w.(interface{ PreferFlatWrite() }); ok {
		// These byte-stream carriers take one contiguous slice. Reuse the
		// batch's scratch for exactly as long as the carrier owns the write.
		return w.Write(b.FlatView())
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
		*gather = bufs
		n, err := gather.WriteTo(nc)
		*gather = nil
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
	defer c.side.done()
	defer pd.gs.done()
	var w io.WriteCloser
	err := hostCall(c.cfg.Logger, "LaneOpener.OpenLane", func() error {
		var err error
		w, err = pd.opener.OpenLane(pd.openCtx)
		return err
	})
	defer func() {
		if w != nil {
			safeLog(c.cfg.Logger, "lane close", func() { _ = w.Close() })
		}
	}()
	var source CarrierHalfSource
	if pd.end.accounting != nil && err == nil && w != nil {
		if named, ok := w.(LaneSource); ok {
			err = hostCall(c.cfg.Logger, "LaneSource", func() error { source = named.LaneSource(); return nil })
		}
		if source == (CarrierHalfSource{}) && pd.hc.io.LanesShareSource {
			source = pd.hc.io.SendSource
		}
	}
	c.lock()
	defer c.unlock()
	if pd.retiring || pd.dead || c.pipes[pd.id] != pd {
		return
	}
	if err != nil || c.closing || w == nil {
		c.s.LaneOpenFailed(pd.id)
		return
	}
	if a := pd.end.accounting; a != nil {
		if a.checkSources(CarrierHalfSource{}, source, false, true) != nil {
			c.s.LaneOpenFailed(pd.id)
			return
		}
	}
	lo := &laneOut{w: w, writer: laneWriter{ctx: labels.With(pd.lctx, "zmux", "lane-write")}}
	info := core.CarrierInfo{Framing: core.Framing(pd.framing), Lanes: true, MaxRecord: pd.maxRec}
	lane, ok := c.s.LaneOpened(pd.id, info, &lo.writer.poll)
	if !ok {
		return
	}
	if pd.accountSegment != nil {
		lo.account = pd.accountSegment.startHalf(source, true, lane)
	}
	w = nil // The lane now owns its writer.
	lo.lane = lane
	if pd.lanes == nil {
		pd.lanes = map[uint64]*laneOut{}
	}
	pd.lanes[lane] = lo
}

// ---- carrier commands ----

// pendingAct is a carrier command with its attachment, resolved under the
// lock in the critical section that queued it: a retired attachment leaves
// c.pipes in a later critical section, which may run before the command is
// performed, and a command looked up then would be lost (an aborted half
// that is never closed blocks its goroutine for good).
type pendingAct struct {
	pd            *pipeDrv
	lo            *laneOut
	shutdownReady chan struct{}
	reason        string
	a             core.Action
	code          ApplicationErrorCode
	reserved      bool // action may touch the carrier until doAction returns
	start         bool
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
		if pd.retiring {
			// An abort may expedite the outstanding graceful-close timer.
			if a.Kind != core.ActAbort {
				pa.pd = nil
			}
			return pa
		}
		pd.gs.add()
		pa.reserved = true
		pa.lo = pd.lanes[a.Lane]
		if a.Kind == core.ActShutdown {
			pa.shutdownReady = make(chan struct{})
			if a.Lane == 0 {
				pd.shutReq = true
				pd.writer.shutdown = pa.shutdownReady
				c.s.QueueWriter(a.Pipe, a.Lane, &pd.writer.poll)
			} else if pa.lo != nil {
				pa.lo.writer.shutdown = pa.shutdownReady
				c.s.QueueWriter(a.Pipe, a.Lane, &pa.lo.writer.poll)
			}
		}
		if a.Kind == core.ActOpenLane && pd.opener != nil && !c.closing && !pd.retiring {
			if pd.openCtx == nil {
				pd.openCtx, pd.stopOpen = context.WithCancel(c.ctx) //nolint:gosec // closeHalves cancels the attachment's pending opens.
			}
			pd.gs.add()
			c.side.add()
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
	if pa.reserved {
		defer pd.gs.done()
	}
	switch a.Kind {
	case core.ActShutdown:
		// The lane owns the half-close. Release it after the host callback.
		defer close(pa.shutdownReady)
		if pa.coded {
			pd.hc.recordCloseCode(pa.code, pa.reason)
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
			labels.Go(pd.lctx, "lane-open", func() { c.openLane(pd) })
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
	pd.closeHalves(true, 0)
}

func (pd *pipeDrv) closeGraceful(bound time.Duration) {
	pd.closeHalves(false, bound)
}

// Retirement claims the halves and reserves the abort timer before the
// first host close. A concurrent abort can bring the timer forward.
func (pd *pipeDrv) closeHalves(abort bool, bound time.Duration) {
	var lanes []*laneOut
	var in map[*inboundLane]struct{}
	var stop context.CancelFunc
	var escalate func()
	owner := false
	pd.c.peek(func() {
		if pd.retiring {
			if abort && pd.graceT != nil && pd.graceT.Stop() {
				escalate = pd.graceAb
			}
			return
		}
		owner = true
		pd.retiring = true
		pd.accountSegment.retire()
		pd.writer.wake.FireLocked()
		for _, lo := range pd.lanes {
			lo.writer.wake.FireLocked()
			lanes = append(lanes, lo)
		}
		stop = pd.stopOpen
		in, pd.inLanes = pd.inLanes, nil
		refs := int32(1) // the close pass
		if !abort {
			refs++ // independently scheduled abort
		}
		for lane := range in {
			if !lane.owner.reserveRefs(refs) {
				delete(in, lane)
			}
		}
		if !abort {
			pd.gs.add()
			pd.graceAb = func() {
				defer pd.gs.done()
				defer pd.c.peek(func() { pd.graceAb, pd.graceT = nil, nil })
				for lane := range in {
					lane.abort()
					lane.owner.release()
				}
				safeLog(pd.c.cfg.Logger, "carrier Abort", func() { pd.hc.abort(net.ErrClosed) })
				for _, lo := range lanes {
					lo.abort()
				}
			}
			pd.graceT = time.AfterFunc(bound, pd.graceAb)
		}
	})
	if escalate != nil {
		escalate()
	}
	if !owner {
		return
	}
	if stop != nil {
		stop()
	}
	pd.endChannel()
	safeLog(pd.c.cfg.Logger, "carrier close", func() { pd.hc.closeAll(abort, net.ErrClosed) })
	for _, lo := range lanes {
		safeLog(pd.c.cfg.Logger, "lane close", func() { lo.close(abort) })
	}
	for lane := range in {
		if abort {
			lane.abort()
		}
		lane.close()
		lane.owner.release()
	}
	if pd.hasOwner {
		pd.gs.done()
	}
	pd.gs.finish()
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

// addAfterIO registers an independent lifetime release after physical close.
func (g *pipeGoroutines) addAfterIO(done func()) {
	g.mu.Lock()
	if g.closed && g.n == 0 {
		g.mu.Unlock()
		done()
		return
	}
	prior := g.afterIO
	g.afterIO = func() {
		if prior != nil {
			prior()
		}
		done()
	}
	g.mu.Unlock()
}
