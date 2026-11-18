package zmux

import (
	"context"
	"io"
	"sync"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
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
	return handleLane(ctx, nil, withPrefix(p, first), first)
}

// handleLane attaches an inbound lane whose first record is first; l
// counts the outcome (nil without a listener).
func handleLane(ctx context.Context, l *Listener, p PipeIO, first []byte) error {
	body := first
	if p.Framing == FramingByteStream {
		_, n := varint.ConsumeVarint(first)
		body = first[n:]
	}
	var f wire.Frame
	if _, err := wire.DecodeFrame(body, &f); err != nil || f.Kind != wire.KindPipeLane {
		closePipe(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	var k [16]byte
	copy(k[:], f.LaneKey)
	laneKeys.mu.Lock()
	c := laneKeys.m[k]
	laneKeys.mu.Unlock()
	if c == nil {
		closePipe(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	var conn any
	if cc, ok := p.R.(CarrierConner); ok {
		conn = cc.CarrierConn()
	} else if cc, ok := p.W.(CarrierConner); ok {
		conn = cc.CarrierConn()
	}
	done, r, err := c.acceptLane(p, conn, f.PipeID, f.Lane)
	if err != nil {
		l.rejectedBy(err)
		return err
	}
	if l != nil {
		l.st.joins.Add(1)
	}
	select {
	case <-done:
	case <-ctx.Done():
		_ = r.Close()
		<-done
	}
	return nil
}

// acceptLane attaches an inbound lane and starts its reader.
func (c *Conn) acceptLane(p PipeIO, conn any, pipe, lane uint64) (<-chan struct{}, io.Closer, error) {
	// Halves are closed after the unlock (deferred first, so run last): a
	// host lane's Close may block, and never holds the session's lock.
	var refused, released, unusedW bool
	defer func() {
		switch {
		case refused && released:
			ClosePipeIO(p) // the setup's closer let go of the halves: they are ours
		case refused:
			closePipe(p)
		case unusedW:
			_ = p.W.Close()
		}
	}()
	c.lock()
	defer c.unlock()
	pd := c.pipes[pipe]
	info := core.CarrierInfo{Framing: core.Framing(p.Framing), Lanes: true}
	if pd == nil || c.closing {
		refused = true
		return nil, nil, errRejected
	}
	// The lane's halves leave the setup's closer: R goes to the lane's
	// onceCloser, an unused W is closed below. Nothing else holds a lane
	// before it is accepted, so the release should not find it closed; if
	// it does (a holder added later), the halves are closed already and
	// the lane is refused before the session sees it.
	if !releasePipe(p, nil) {
		refused = true
		return nil, nil, errRejected
	}
	released = true
	if !c.s.AcceptLane(conn, pipe, lane, info) {
		refused = true
		return nil, nil, errRejected
	}
	markTaken(p)
	// The reverse half of a bidirectional stream used as a lane is
	// unused: it ends at once.
	unusedW = p.W != nil && !carrierio.SameHalf(p.W, p.R)
	r := &onceCloser{ReadCloser: p.R} // the pipe's end and the handler's ctx both close it: once
	pd.inLanes = append(pd.inLanes, r)
	done := make(chan struct{})
	c.side.add()
	pd.gs.add()
	labels.Go(pd.lctx, "lane-read", func() {
		defer close(done)
		defer pd.gs.done()
		c.readLoop(pd, lane, p.R)
	})
	return done, r, nil
}

// onceCloser closes its half once, whoever asks first.
type onceCloser struct {
	io.ReadCloser
	once sync.Once
}

func (o *onceCloser) Close() error {
	var err error
	o.once.Do(func() { err = o.ReadCloser.Close() })
	return err
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

// waitPipe blocks until the goroutines of an attachment ended; when ctx
// ends first the attachment is aborted.
func (c *Conn) waitPipe(ctx context.Context, id uint64) error {
	c.lock()
	pd := c.pipes[id]
	c.unlock()
	if pd == nil {
		return nil
	}
	done := pd.gs.wait()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		pd.closeAll()
		<-done
		return ctx.Err()
	}
}
