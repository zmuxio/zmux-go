package conformance

import (
	"fmt"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// listenerUp delivers a carrier through the local endpoint's listener.
func (r *runner) listenerUp(st map[string]any) error {
	name := st["carrier"].(string)
	c := &rcarrier{name: name, via: "listener", dir: st["dir"].(string)}
	if f, _ := st["framing"].(string); f == "records" {
		c.framing = core.FramingRecords
	}
	if cb, ok := st["carrier_binding"].(string); ok {
		c.binding = hexb(cb)
	}
	if conn, ok := st["conn"].(string); ok {
		c.conn = conn
	}
	if tok, ok := st["pairing"].(string); ok {
		var t [16]byte
		copy(t[:], hexb(tok))
		up := c.dir == "in" // the responder reads the initiator's uplink
		res, partner := r.pairing.Offer(t, up, "", c, r.now.Add(10*core.Second))
		switch res {
		case core.PairReject:
			c.rejected = true
		case core.PairWait:
		case core.PairDone:
			o := partner.(*rcarrier)
			c.attached, o.attached = true, true
			c.pipe, o.pipe = 0, 0
			p := &rpipe{id: 0, lanes: map[uint64]*rlane{}, writable: true}
			p.in, p.out = true, true
			r.pipes[0] = p
			ci := core.CarrierInfo{QueueBound: true, MinRTT: true}
			r.s.AttachBootstrap(ci, ci, nil)
			r.drainEvents()
		}
		r.carriers[name] = c
		return nil
	}
	r.carriers[name] = c
	if src, ok := st["source"].(string); ok {
		if !r.pending.Admit(src) {
			c.rejected = true
			return nil
		}
		c.source, c.deadline = src, r.pending.Deadline(r.now)
	}
	if oob, ok := st["oob_join"].(string); ok {
		j, mac, _, ok := core.ParseJoin(hexb(oob))
		if !ok {
			c.rejected = true
			return nil
		}
		r.tryJoin(c, j, mac, false)
	}
	return nil
}

func (r *runner) carrierInfo(c *rcarrier) core.CarrierInfo {
	return core.CarrierInfo{QueueBound: true, Framing: c.framing, MinRTT: true, Lanes: c.conn != ""}
}

// tryJoin offers a join to the session.
func (r *runner) tryJoin(c *rcarrier, j *wire.Join, mac []byte, inband bool) {
	if !r.s.LaneKeyMatches(nil) && string(j.Locator) != string(r.s.Locator()) {
		c.rejected = true
		return
	}
	hasIn, hasOut := c.dir != "out", c.dir != "in"
	info := r.carrierInfo(c)
	res := r.s.AcceptJoin(j, mac, c.binding, core.Carrier{HasIn: hasIn, HasOut: hasOut, In: info, Out: info})
	r.drainEvents()
	switch res {
	case core.JoinHold:
		c.held = true
		c.pendJoin, c.pendMac, c.pendInband = j, mac, inband
		r.held = append(r.held, c)
		return
	case core.JoinClose, core.JoinRefused:
		c.rejected = true
		c.buf = nil
		return
	case core.JoinAccepted:
	}
	c.held = false
	c.attached, c.pipe, c.lane = true, j.PipeID, 0
	p := &rpipe{id: j.PipeID, lanes: map[uint64]*rlane{}, writable: true, framing: c.framing}
	p.in, p.out = hasIn && j.Dir != 1, hasOut && j.Dir != 0
	r.pipes[j.PipeID] = p
	if c.conn != "" {
		r.s.SetCarrierConn(j.PipeID, c.conn)
	}
	if inband {
		r.s.SkipJoinFrame(j.PipeID)
	}
	r.feedCarrier(c)
}

// retryHeld offers held joins again (after establishment).
func (r *runner) retryHeld() {
	held := r.held
	r.held = nil
	for _, c := range held {
		if c.held && !c.rejected {
			c.held = false
			r.tryJoin(c, c.pendJoin, c.pendMac, c.pendInband)
		}
	}
}

func (r *runner) feedCarrier(c *rcarrier) {
	if !c.attached || len(c.buf) == 0 {
		return
	}
	p := r.pipes[c.pipe]
	if p != nil && p.framing == core.FramingRecords {
		return
	}
	buf := &core.Buf{B: c.buf}
	n := r.s.Receive(c.pipe, c.lane, buf, c.buf, r.now)
	c.buf = append([]byte(nil), c.buf[n:]...)
	r.drainEvents()
}

// listenerSend delivers bytes on a listener carrier: the first record is
// classified, then the carrier is the attached pipe's.
func (r *runner) listenerSend(c *rcarrier, st map[string]any) error {
	if c.framing == core.FramingRecords {
		var list [][]byte
		if msgs, ok := st["messages"].([]any); ok {
			for _, m := range msgs {
				list = append(list, r.enc.encodeBody(m))
			}
		} else if mh, ok := st["messages_hex"].([]any); ok {
			for _, m := range mh {
				list = append(list, hexb(m))
			}
		}
		for _, m := range list {
			if c.rejected {
				return nil
			}
			if !c.attached {
				j, mac, _, ok := core.ParseJoin(m)
				if !ok {
					c.rejected = true
					return nil
				}
				r.tryJoin(c, j, mac, true)
				if c.attached {
					r.s.ReceiveMessage(c.pipe, 0, &core.Buf{B: m}, m, r.now)
					r.drainEvents()
				}
				continue
			}
			r.s.ReceiveMessage(c.pipe, 0, &core.Buf{B: m}, m, r.now)
			r.drainEvents()
		}
		return nil
	}
	var data []byte
	if hx, ok := st["hex"].(string); ok {
		data = hexb(hx)
	} else {
		data = r.enc.encodeRecords(st["records"].([]any))
	}
	if c.rejected {
		return nil
	}
	c.buf = append(c.buf, data...)
	if c.attached {
		r.feedCarrier(c)
		return nil
	}
	if c.held {
		return nil
	}
	cl, n := core.Classify(c.buf, 65535)
	switch cl {
	case core.ClassReject, core.ClassSession:
		c.rejected = true
		c.buf = nil
		return nil
	case core.ClassMore:
		return nil
	case core.ClassJoin, core.ClassLane:
	}
	if n == 0 {
		return nil
	}
	if c.source != "" {
		// The first record is complete: the slot is free again.
		r.pending.Release(c.source)
		c.source = ""
	}
	if cl == core.ClassLane {
		c.via = "lane"
		return r.carrierSend(c, map[string]any{"hex": ""})
	}
	_, sn := wire.ReadVarint(c.buf)
	j, mac, _, ok := core.ParseJoin(c.buf[sn:n])
	if !ok {
		c.rejected = true
		c.buf = nil
		return nil
	}
	r.tryJoin(c, j, mac, true)
	return nil
}

// addPipe performs the add_pipe call: a local join.
func (r *runner) addPipe(args map[string]any) (map[string]any, error) {
	id := num(args["pipe"])
	if next := r.s.NextPipeID(); next == 0 {
		_, _, err := r.s.AddPipe(core.JoinOptions{})
		return nil, err
	} else if next != id {
		return nil, fmt.Errorf("next pipe ID %d, trace expects %d", next, id)
	}
	dir := args["dir"].(string)
	o := core.JoinOptions{Dir: map[string]uint8{"out": 0, "in": 1, "bidi": 2}[dir], FateGroup: getU(args, "fate_group"),
		OOB: getB(args, "oob"), CarrierBound: getB(args, "carrier_bound")}
	if v, ok := args["send_priority"]; ok && v != nil {
		x := num(v)
		o.SendPriority = &x
	}
	if v, ok := args["send_standby"]; ok && v != nil {
		x := v.(bool)
		o.SendStandby = &x
	}
	o.RecvPriority = getU(args, "receive_priority")
	o.RecvStandby = getB(args, "receive_standby")
	o.Weight = getU(args, "weight")
	if o.CarrierBound {
		if b, ok := args["carrier_binding"].(string); ok {
			o.Binding = hexb(b)
		}
	}
	ci := core.CarrierInfo{QueueBound: true, MinRTT: true}
	o.In, o.Out = ci, ci
	p := &rpipe{id: id, lanes: map[uint64]*rlane{}, writable: true}
	p.in, p.out = dir != "out", dir != "in"
	r.pipes[id] = p
	_, res, err := r.s.AddPipe(o)
	if err != nil {
		return nil, err
	}
	if res == core.AddPipePending {
		return map[string]any{"pending": true}, nil
	}
	return map[string]any{"ok": true}, nil
}
