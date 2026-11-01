package zmux

import (
	"context"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// The lane keys of the process's sessions (MULTIPATH.md 5.8): inbound
// lanes are routed by key over every session, whatever accepted them. The
// registry's lock is a leaf.
var laneKeys = struct {
	mu sync.Mutex
	m  map[[16]byte]*Conn
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
// is closed. It blocks until the lane ends.
func HandleLane(ctx context.Context, p PipeIO) error {
	first, cls, err := readFirstRecord(p)
	if err != nil || cls != core.ClassLane {
		closePipeIO(p)
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
		closePipeIO(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	var k [16]byte
	copy(k[:], f.LaneKey)
	laneKeys.mu.Lock()
	c := laneKeys.m[k]
	laneKeys.mu.Unlock()
	if c == nil {
		closePipeIO(p)
		l.rejectedBy(errRejected)
		return errRejected
	}
	var conn any
	if cc, ok := p.R.(CarrierConner); ok {
		conn = cc.CarrierConn()
	} else if cc, ok := p.W.(CarrierConner); ok {
		conn = cc.CarrierConn()
	}
	done, err := c.acceptLane(p, conn, f.PipeID, f.Lane)
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
		_ = p.R.Close()
		<-done
	}
	return nil
}

// acceptLane attaches an inbound lane and starts its reader.
func (c *Conn) acceptLane(p PipeIO, conn any, pipe, lane uint64) (<-chan struct{}, error) {
	c.lock()
	defer c.unlock()
	pd := c.pipes[pipe]
	info := core.CarrierInfo{Framing: core.Framing(p.Framing), Lanes: true}
	if pd == nil || c.closing || !c.s.AcceptLane(conn, pipe, lane, info) {
		closePipeIO(p)
		return nil, errRejected
	}
	markTaken(p)
	if p.W != nil && any(p.W) != any(p.R) {
		// The reverse half of a bidirectional stream used as a lane is
		// unused: it ends at once.
		_ = p.W.Close()
	}
	pd.inLanes = append(pd.inLanes, p.R)
	done := make(chan struct{})
	c.wg.Add(1)
	pd.gs.add()
	go func() {
		defer close(done)
		defer pd.gs.done()
		c.readLoop(pd, lane, p.R)
	}()
	return done, nil
}

// acceptJoin attaches a carrier that joins the session (MULTIPATH.md 5.4);
// inband: its first record starts with the PIPE_JOIN the listener read;
// admitted is the carrier's admission value.
func (c *Conn) acceptJoin(ctx context.Context, p PipeIO, j *wire.Join, mac, binding []byte, inband bool, admitted any) (uint64, error) {
	for {
		c.lock()
		if c.closing {
			c.unlock()
			closePipeIO(p)
			return 0, errRejected
		}
		pd := c.newPipeDrv(j.PipeID, p)
		low := pd.tcp.setLowWater(multiLowWater)
		info := carrierInfo(p, pd.tcp, low)
		outer := outerOf(p)
		loop := nestPipe(c, outer) != nil
		res := c.s.AcceptJoin(j, mac, binding, core.Carrier{HasIn: p.R != nil, HasOut: p.W != nil, In: info,
			Out: info, OutSig: pd.wsig, Refuse: loop, Kind: uint8(p.Carrier)})
		if res != core.JoinAccepted && !loop {
			unnestPipe(c, outer)
		}
		pd.outer = outer
		switch res {
		case core.JoinAccepted:
			pd.admitted = admitted
			markTaken(p)
			if inband {
				c.s.SkipJoinFrame(j.PipeID)
			}
			if j.Dir == 1 && p.R != nil && any(p.R) != any(p.W) {
				_ = p.R.Close() // a downlink: the acceptor reads nothing
			}
			c.startPipeLocked(pd, j.Dir != 1, j.Dir != 0)
			c.unlock()
			return j.PipeID, nil
		case core.JoinHold:
			c.unlock()
			if err := c.waitEstablished(ctx); err != nil {
				closePipeIO(p)
				return 0, err
			}
			continue
		case core.JoinRefused:
			c.unlock()
			closePipeIO(p)
			return 0, errRefused
		case core.JoinClose:
		}
		c.unlock()
		closePipeIO(p)
		return 0, errRejected
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
