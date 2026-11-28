package zmux

import (
	"context"
	"io"
	"net"
	"reflect"
	"sync"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// The lane keys of the process's sessions: inbound
// lanes are routed by key over every session, whatever accepted them. The
// registry's lock is a leaf.
var laneKeys = struct {
	m  map[[16]byte]*Conn
	mu sync.Mutex
}{m: map[[16]byte]*Conn{}}

func registerLaneKey(c *Conn) {
	k, ok := c.s.LaneKey()
	if !ok {
		return
	}
	laneKeys.mu.Lock()
	laneKeys.m[k] = c
	laneKeys.mu.Unlock()
}

func unregisterLaneKey(c *Conn) {
	k, ok := c.s.LaneKey()
	if !ok {
		return
	}
	laneKeys.mu.Lock()
	if laneKeys.m[k] == c {
		delete(laneKeys.m, k)
	}
	laneKeys.mu.Unlock()
}

// HandleLane routes a stream that the peer opened on a lane-capable
// carrier connection, on an endpoint without a listener: its
// first record names the session by Lane Key. A stream that names no pipe
// is closed, and so is one whose first record, the lane header, does not
// arrive within DefaultFirstRecordTimeout (the session is not known before
// the header is read, so a listener's FirstRecordTimeout cannot apply). It
// blocks until the lane ends.
func HandleLane(ctx context.Context, p PipeIO) error {
	return handleLaneOn(ctx, timeshim.Or(nil), p)
}

// handleLaneOn is HandleLane on clk.
func handleLaneOn(ctx context.Context, clk timeshim.Clock, p PipeIO) error {
	p = owned(p)
	first, cls, err := readFirstWithin(ctx, context.Background(), clk, DefaultFirstRecordTimeout, p)
	if err != nil || cls != core.ClassLane {
		closePipe(p)
		if err == nil {
			err = errRejected
		}
		return err
	}
	defer first.release()
	return handleLane(ctx, nil, first.replay(p), first.laneBody, nil)
}

// handleLane attaches an inbound lane whose first record is first; l
// counts the outcome (nil without a listener).
func handleLane(ctx context.Context, l *Listener, p PipeIO, first wire.Parts, wait *attachment) error {
	var f wire.Frame
	if _, err := wire.DecodeFrameParts(first, &f); err != nil || f.Kind != wire.KindPipeLane {
		closePipe(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	var k [16]byte
	copy(k[:], f.LaneKey)
	laneKeys.mu.Lock()
	c := laneKeys.m[k]
	laneKeys.mu.Unlock()
	if c == nil || l != nil && c.lst != l {
		closePipe(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	if l == nil {
		wait = newAttachment(c.lst, true)
	}
	closeLane, err := func() (func(), error) {
		if l == nil && c.lst != nil {
			setup, unlink := c.lst.stopContext(ctx)
			defer unlink()
			stop := wait.watch(setup, p)
			defer stop()
		}
		var conn any
		if !safeLog(c.cfg.Logger, "CarrierConn", func() {
			if cc, ok := p.R.(CarrierConner); ok {
				conn = cc.CarrierConn()
			} else if cc, ok := p.W.(CarrierConner); ok {
				conn = cc.CarrierConn()
			}
		}) {
			closePipe(p)
			return nil, errRejected
		}
		if conn != nil && !reflect.ValueOf(conn).Comparable() {
			closePipe(p)
			return nil, errRejected
		}
		return c.acceptLane(ctx, p, conn, f.PipeID, f.Lane, wait)
	}()
	if l == nil {
		wait.release()
	}
	if err != nil {
		l.rejectedBy(err)
		return err
	}
	if l != nil {
		l.st.joins.Add(1)
		return nil
	}
	select {
	case <-wait.done:
	case <-ctx.Done():
		closeLane()
		<-wait.done
	}
	return nil
}

// An inbound lane owns its individual lifetime even when its parent
// retires. Only live lanes can be acquired by a retiring driver.
type inboundLane struct {
	owner     *attachment
	close     func()
	half      io.Closer
	unused    io.Closer
	workers   int // reader and optional unused half, under the session lock
	abortOnce sync.Once
}

func (l *inboundLane) abort() {
	l.abortOnce.Do(func() {
		for _, half := range []io.Closer{l.half, l.unused} {
			if a, ok := half.(Aborter); ok {
				safeLog(nil, "lane Abort", func() { a.Abort(net.ErrClosed) })
			}
		}
	})
}

// acceptLane attaches an inbound lane and starts its reader.
func (c *Conn) acceptLane(ctx context.Context, p PipeIO, conn any, pipe, lane uint64, wait *attachment) (func(), error) {
	if err := wait.acceptHead(ctx); err != nil {
		closePipe(p)
		return nil, err
	}
	if err := hostCall(c.cfg.Logger, "CarrierTaken", func() error { markTaken(p); return nil }); err != nil {
		closePipe(p)
		return nil, err
	}
	if err := c.waitPublication(ctx); err != nil {
		closePipe(p)
		return nil, err
	}
	flight, err := c.reserveReceiveFlight(p.R)
	if err != nil {
		closePipe(p)
		return nil, err
	}
	defer func() { flight.release() }()
	c.lock()
	pd := c.pipes[pipe]
	var account *accountHalf
	source := p.ReceiveSource
	if pd != nil && source == (CarrierHalfSource{}) && pd.hc.io.LanesShareSource {
		source = pd.hc.io.ReceiveSource
	}
	if pd != nil && pd.end.accounting != nil {
		if err := pd.end.accounting.checkSources(source, CarrierHalfSource{}, true, false); err != nil {
			c.unlock()
			closePipe(p)
			return nil, err
		}
	}
	info := core.CarrierInfo{Framing: core.Framing(p.Framing), Lanes: true}
	if pd == nil || pd.retiring || c.closing || c.s.Err() != nil || !wait.commitCarrier(ctx, func() bool { return c.s.AcceptLane(conn, pipe, lane, info) }) {
		c.unlock()
		closePipe(p)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errRejected
	}
	releasePipe(p, nil)
	if pd.accountSegment != nil {
		account = pd.accountSegment.startHalf(source, false, lane)
	}
	// The reverse half of a bidirectional stream used as a lane is
	// unused: it ends at once.
	r := p.R
	var unused io.Closer
	var half io.Closer = r
	if carrierio.SameHalf(unprefixed(r), unprefixed(p.W)) {
		half = p.W
	} else if p.W != nil {
		unused = p.W
	}
	// The reader, the pipe and a standalone HandleLane share this close.
	closeLane := sync.OnceFunc(func() {
		carrierio.CloseHalf(half)
	})
	child := &inboundLane{owner: wait, half: half, unused: unused, close: closeLane, workers: 1}
	if pd.inLanes == nil {
		pd.inLanes = make(map[*inboundLane]struct{})
	}
	pd.inLanes[child] = struct{}{}
	finished := func() {
		c.peek(func() {
			child.workers--
			if child.workers == 0 {
				delete(pd.inLanes, child)
			}
		})
		wait.release()
	}
	if unused != nil {
		child.workers++
		wait.reserve()
		pd.worker("lane-unused", func() { defer finished(); carrierio.CloseHalf(unused) })
	}
	c.side.add()
	pd.gs.add()
	wait.reserve()
	held := flight
	flight = receiveFlight{}
	c.unlock()
	labels.Go(pd.lctx, "lane-read", func() {
		defer pd.gs.done()
		defer finished()
		defer held.release()
		c.readLoop(pd, lane, r, closeLane, account)
	})
	return closeLane, nil
}

// waitJoinKeys waits until the join keys exist (Conn.SetJoinSecret) or
// ctx ends.
func (c *Conn) waitJoinKeys(ctx context.Context) error {
	sig := core.NewSignal()
	for {
		var ready bool
		c.locked(func() { ready = c.s.WaitJoinKeys(sig) })
		if ready {
			return nil
		}
		select {
		case <-sig.C:
			continue
		case <-ctx.Done():
		case <-c.done:
		}
		c.locked(func() {
			c.s.UnwaitJoinKeys(sig)
		})
		return errRejected
	}
}
