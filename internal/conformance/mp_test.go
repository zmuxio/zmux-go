package conformance

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
	"github.com/zmuxio/zmux-go/varint"
)

var (
	dirByName  = map[string]uint8{"up": 0, "down": 1, "bidi": 2}
	dirNames   = []string{"up", "down", "bidi"}
	modeByName = map[string]join.Mode{"sealed": join.Sealed, "bound": join.Bound, "trusted": join.Trusted}
)

// The keys of the specification's join example, for a Hello written before the peer's
// parameters reached the session.
var (
	vectorInitNonce = hexb("00112233445566778899aabbccddeeff")
	vectorRespNonce = hexb("ffeeddccbbaa99887766554433221100")
)

func joinFraming(f core.Framing) join.Framing {
	if f == core.FramingRecords {
		return join.Message
	}
	return join.Stream
}

// joinPSKOf is join_psk of a role and epoch, as both ends derive it from the
// local parameters and the peer's.
func (r *runner) joinPSKOf(role join.Role, epoch uint64) []byte {
	secret, enc := r.sessionKeys()
	if epoch > 0 {
		secret = r.ep.secrets[epoch]
	}
	su, _ := join.LookupSuite(join.SuiteAES)
	psk, err := join.PSK(su, secret, role, enc)
	if err != nil {
		r.t.Fatalf("join psk: %v", err)
	}
	return psk
}

// sessionKeys is the session secret and session_crypto, as both ends derive them.
func (r *runner) sessionKeys() (secret, sessionCrypto []byte) {
	local := r.s.LocalParams()
	initN, respN := local.SessionNonce[:], vectorRespNonce
	setI, setR := local.SessionCrypto, wire.SessionCrypto{Suites: []uint64{join.SuiteAES}, Groups: []uint64{join.GroupX25519MLKEM768}}
	localInit := r.h.LocalRole != "responder"
	if rl, known := r.s.Role(); known {
		localInit = rl == core.RoleInitiator
	}
	if peer, ok := r.s.PeerParams(); ok {
		respN, setR = peer.SessionNonce[:], peer.SessionCrypto
	} else if !localInit {
		respN = vectorInitNonce
	}
	if !localInit {
		initN, respN = respN, initN
		setI, setR = setR, setI
	}
	enc, err := join.SessionCrypto(setI, setR)
	if err != nil {
		r.t.Fatalf("session crypto: %v", err)
	}
	return wire.SessionSecret(initN, respN, nil, r.s.Config().PSK), enc
}

// peerRole is the role of the scripted peer.
func (r *runner) peerRole() join.Role {
	localInit := r.h.LocalRole != "responder"
	if rl, known := r.s.Role(); known {
		localInit = rl == core.RoleInitiator
	}
	if localInit {
		return join.RoleResponder
	}
	return join.RoleInitiator
}

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
		res, partner := r.pairing.Offer(t, up, "", c)
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
		slot, _, ok := r.pending.Admit(src, nil)
		if !ok {
			c.rejected = true
			return nil
		}
		c.slot, c.deadline = slot, r.pending.Deadline(r.now)
	}
	return nil
}

func (r *runner) carrierInfo(c *rcarrier) core.CarrierInfo {
	return core.CarrierInfo{QueueBound: true, Framing: c.framing, MinRTT: true, Lanes: c.conn != ""}
}

func (r *runner) coreCarrier(c *rcarrier) core.Carrier {
	info := r.carrierInfo(c)
	return core.Carrier{HasIn: c.dir != "out", HasOut: c.dir != "in", In: info, Out: info}
}

// closeCarrier ends a carrier of the listener.
func (r *runner) closeCarrier(c *rcarrier, outcome string) {
	c.rejected, c.outcome = true, outcome
	c.buf, c.msgs, c.heldHello, c.pj = nil, nil, nil, nil
	r.pending.Release(c.slot)
	c.slot = nil
}

// offerHello is the listener's handshake from a parsed Hello on:
// opening and verifying it up to the pending join. A held join stays pending and is offered again.
func (r *runner) offerHello(c *rcarrier, h *join.Hello) {
	if c.hsDeadline == 0 {
		c.hsDeadline = r.now.Add(r.s.Config().HandshakeTimeout)
	}
	if !r.s.AcceptsLocator(h.Locator) {
		r.closeCarrier(c, "reject")
		return
	}
	pj, res := r.s.OpenHello(*h, c.framing, c.binding)
	r.drainEvents()
	switch res {
	case core.JoinHold, core.JoinKeyWait:
		c.heldHello = h
		return
	case core.JoinAccepted:
	default:
		outcome := "reject"
		if r.s.Err() != nil {
			outcome = "fatal"
		}
		r.closeCarrier(c, outcome)
		return
	}
	c.heldHello = nil
	if r.modes != nil && !r.modes[pj.Mode.String()] {
		r.closeCarrier(c, "reject") // local policy: the Pipe ID's bit stays clear
		return
	}
	reply, res := r.s.ClaimHello(pj, r.coreCarrier(c), 0, 1<<20, &shaping.UnitPlan{})
	r.drainEvents()
	switch res {
	case core.JoinAccepted:
		c.reply, c.pj, c.outcome = reply, pj, "accept"
		c.pipe = pj.Pipe.ID
		r.peerReadsReply(c)
		if c.joiner != nil && c.confirm {
			r.writeConfirm(c)
		}
	case core.JoinRefused:
		c.reply = reply
		r.peerReadsReply(c)
		r.closeCarrier(c, "refuse")
	default:
		r.closeCarrier(c, "reject")
	}
}

// peerReadsReply has the scripted peer verify the Reply.
func (r *runner) peerReadsReply(c *rcarrier) {
	if c.joiner == nil {
		return
	}
	pl, _, err := c.joiner.ReadReply(c.reply)
	if err != nil {
		r.t.Fatalf("carrier %s: the peer cannot read the Reply: %v", c.name, err)
	}
	c.decision = pl.Decision
}

// writeConfirm has the scripted peer read the Reply and write its confirmation.
func (r *runner) writeConfirm(c *rcarrier) {
	confirm, err := c.joiner.WriteConfirm(nil, 0)
	if err != nil {
		r.t.Fatalf("carrier %s: confirmation: %v", c.name, err)
	}
	r.readConfirm(c, confirm)
}

// readConfirm feeds confirmation bytes to an Unconfirmed carrier and attaches the
// pipe once it is Confirmed.
func (r *runner) readConfirm(c *rcarrier, b []byte) {
	_, _, done, err := c.pj.ReadConfirm(b)
	if err != nil {
		r.closeCarrier(c, "reject")
		return
	}
	if !done {
		return
	}
	pj := c.pj
	res := r.s.ConfirmJoin(pj, r.coreCarrier(c))
	r.drainEvents()
	if res != core.JoinAccepted {
		r.closeCarrier(c, c.outcome)
		return
	}
	c.pj, c.attached, c.lane = nil, true, 0
	r.pending.Release(c.slot)
	c.slot = nil
	cc := r.coreCarrier(c)
	p := &rpipe{id: pj.Pipe.ID, lanes: map[uint64]*rlane{}, writable: true, framing: c.framing}
	p.in, p.out = cc.HasIn && pj.Pipe.Direction != 1, cc.HasOut && pj.Pipe.Direction != 0
	r.pipes[p.id] = p
	if c.conn != "" {
		r.s.SetCarrierConn(p.id, c.conn)
	}
	r.feedCarrier(c)
}

// retryHeld offers held joins again.
func (r *runner) retryHeld() {
	for _, c := range r.carriers {
		if h := c.heldHello; h != nil && !c.rejected {
			r.offerHello(c, h)
		}
	}
}

// handshakeDeadlines closes the carriers that did not reach Confirmed in time.
func (r *runner) handshakeDeadlines() {
	for _, c := range r.carriers {
		if !c.rejected && !c.attached && c.hsDeadline != 0 && r.now >= c.hsDeadline {
			r.closeCarrier(c, "reject")
		}
	}
}

func (r *runner) feedCarrier(c *rcarrier) {
	if !c.attached {
		return
	}
	for _, m := range c.msgs {
		r.s.ReceiveMessage(c.pipe, 0, &core.Buf{B: m}, m, r.now)
		r.drainEvents()
	}
	c.msgs = nil
	if len(c.buf) == 0 {
		return
	}
	buf := &core.Buf{B: c.buf}
	n := r.s.Receive(c.pipe, c.lane, buf, c.buf, r.now)
	c.buf = append([]byte(nil), c.buf[n:]...)
	r.drainEvents()
}

// peerJoin is the peer_join step.
func (r *runner) peerJoin(st map[string]any) error {
	c := r.carriers[st["carrier"].(string)]
	if c == nil {
		return fmt.Errorf("unknown carrier")
	}
	mode := join.Trusted
	if m, ok := st["mode"].(string); ok {
		mode = modeByName[m]
	}
	su, _ := join.LookupSuite(join.SuiteAES)
	phase := uint8(getU(st, "phase"))
	psk := r.joinPSKOf(r.peerRole(), r.ep.latest(phase))
	if getB(st, "bad_key") {
		psk[0] ^= 0xff
	}
	group, _ := join.LookupGroup(join.GroupX25519MLKEM768)
	if g, ok := st["group"]; ok {
		group, _ = join.LookupGroup(num(g))
	}
	cfg := join.JoinerConfig{Suite: su, Group: group, Phase: phase, Mode: mode, Framing: joinFraming(c.framing), Locator: hexb(st["locator"]), PSK: psk}
	if b, ok := st["binding"].(string); ok {
		cfg.Binding = hexb(b)
	}
	j, err := join.NewJoiner(cfg)
	if err != nil {
		return err
	}
	pl := join.HelloPayload{Pipe: join.Pipe{ID: num(st["pipe"]), Direction: dirByName[st["direction"].(string)],
		FateGroup: getU(st, "fate_group"), Priority: getU(st, "priority"), Standby: getB(st, "standby")}}
	if fs, ok := st["batch"]; ok {
		pl.FirstBatch = r.enc.encodeBody(fs)
	}
	pl.KeyShare = j.KeyShare()
	switch ks, _ := st["key_share"].(string); ks {
	case "":
	case "missing":
		pl.KeyShare = join.KeyShare{}
	case "bad_length":
		pl.KeyShare.Share = pl.KeyShare.Share[:len(pl.KeyShare.Share)-1]
	case "low_order":
		pl.KeyShare.Share = append(make([]byte, 32), pl.KeyShare.Share[32:]...) // the X25519 point 0
	default:
		return fmt.Errorf("key_share %q", ks)
	}
	plain, err := join.AppendHelloPayload(nil, pl)
	if err != nil {
		return err
	}
	hello, err := j.WriteHelloRaw(nil, plain)
	if err != nil {
		return err
	}
	c.joiner, c.confirm = j, true
	if v, ok := st["confirm"].(bool); ok {
		c.confirm = v
	}
	if c.rejected {
		return r.checkOutcome(c, st)
	}
	h, _, err := join.ParseHello(hello, joinFraming(c.framing))
	if err != nil {
		return err
	}
	r.offerHello(c, &h)
	if getB(st, "hold") {
		if c.heldHello == nil {
			return fmt.Errorf("carrier %s: the join was not held", c.name)
		}
		return nil
	}
	return r.checkOutcome(c, st)
}

// checkOutcome compares what the listener did with the step's outcome.
func (r *runner) checkOutcome(c *rcarrier, st map[string]any) error {
	want := st["outcome"].(string)
	if c.outcome != want {
		return fmt.Errorf("carrier %s: outcome %q, want %q (held %v)", c.name, c.outcome, want, c.heldHello != nil)
	}
	switch want {
	case "accept", "refuse":
		if refused := c.decision == join.DecisionRefuse; refused != (want == "refuse") {
			return fmt.Errorf("carrier %s: decision %d, want %s", c.name, c.decision, want)
		}
	case "reject", "fatal":
		if len(c.reply) != 0 {
			return fmt.Errorf("carrier %s: wrote a Reply, want %s", c.name, want)
		}
	}
	return nil
}

// peerJoinFinish is the peer_join_finish step.
func (r *runner) peerJoinFinish(st map[string]any) error {
	c := r.carriers[st["carrier"].(string)]
	if c == nil {
		return fmt.Errorf("unknown carrier")
	}
	r.retryHeld()
	if c.heldHello != nil {
		return fmt.Errorf("carrier %s: the join is still held", c.name)
	}
	return r.checkOutcome(c, st)
}

// peerConfirm is the peer_confirm step.
func (r *runner) peerConfirm(st map[string]any) error {
	c := r.carriers[st["carrier"].(string)]
	if c == nil || c.pj == nil {
		return fmt.Errorf("carrier %v is not Unconfirmed", st["carrier"])
	}
	r.writeConfirm(c)
	return nil
}

// writeHellos has every local join write its Hello once allowed.
func (r *runner) writeHellos() {
	for _, p := range r.pipes {
		if !p.joining || p.hello != nil {
			continue
		}
		_, ready, err := r.s.JoinPrepare(p.id)
		var hello []byte
		if err == nil && ready {
			hello, err = r.s.JoinHello(p.id, p.framing, 0, &shaping.UnitPlan{})
		}
		r.drainEvents()
		if err != nil {
			p.joining = false
			continue
		}
		if ready {
			p.hello = hello
		}
	}
}

// readHello has the scripted peer read the Hello of a local join.
func (r *runner) readHello(p *rpipe) (*join.Hello, *join.HelloPayload, error) {
	if p.hello == nil {
		return nil, nil, fmt.Errorf("pipe %d: no Hello written", p.id)
	}
	h, n, err := join.ParseHello(p.hello, joinFraming(p.framing))
	if err != nil {
		return nil, nil, fmt.Errorf("pipe %d: Hello: %w", p.id, err)
	}
	if n != len(p.hello) {
		return nil, nil, fmt.Errorf("pipe %d: %d bytes after the Hello", p.id, len(p.hello)-n)
	}
	localRole := join.RoleResponder
	if r.peerRole() == join.RoleResponder {
		localRole = join.RoleInitiator
	}
	acc, pl, err := join.Accept(h, joinFraming(p.framing), r.joinPSKOf(localRole, r.ep.latest(h.Phase)), nil, func(uint64) bool { return true })
	if err != nil {
		return nil, nil, fmt.Errorf("pipe %d: Hello does not verify: %w", p.id, err)
	}
	if err := acc.Exchange(nil); err != nil {
		return nil, nil, fmt.Errorf("pipe %d: key share: %w", p.id, err)
	}
	p.acc = acc
	return &h, &pl, nil
}

// expectJoinHello is the expect_join_hello step.
func (r *runner) expectJoinHello(st map[string]any) error {
	p := r.pipes[num(st["pipe"])]
	if p == nil {
		return fmt.Errorf("no pipe %v", st["pipe"])
	}
	h, pl, err := r.readHello(p)
	if err != nil {
		return err
	}
	if l := p.lanes[0]; l != nil && len(l.frames) != 0 {
		return fmt.Errorf("frames besides the Hello:\n%s", dumpFrames(l.frames))
	}
	got := pl.Pipe
	if got.ID != p.id || dirNames[got.Direction] != st["direction"] || got.FateGroup != getU(st, "fate_group") ||
		got.Priority != getU(st, "priority") || got.Standby != getB(st, "standby") {
		return fmt.Errorf("hello fields %+v", got)
	}
	if !bytes.Equal(h.Locator, hexb(st["locator"])) {
		return fmt.Errorf("hello locator %x", h.Locator)
	}
	if h.Mode != modeByName[st["mode"].(string)] {
		return fmt.Errorf("hello mode %v", h.Mode)
	}
	if ph, ok := st["phase"]; ok && uint64(h.Phase) != num(ph) {
		return fmt.Errorf("hello key phase %d, want %v", h.Phase, ph)
	}
	if g, ok := st["group"]; ok && pl.KeyShare.Group != num(g) {
		return fmt.Errorf("hello group %d, want %v", pl.KeyShare.Group, g)
	}
	if fs, ok := st["batch"].([]any); ok {
		var frames, pats []map[string]any
		var f wire.Frame
		for b := pl.FirstBatch; len(b) > 0; {
			n, err := wire.DecodeFrame(b, &f)
			if err != nil {
				return fmt.Errorf("first batch: %w", err)
			}
			frames = append(frames, frameObj(&f))
			b = b[n:]
		}
		for _, x := range fs {
			pats = append(pats, x.(map[string]any))
		}
		if err := matchFrames("exact", pats, frames); err != nil {
			return fmt.Errorf("first batch: %w\nframes:\n%s", err, dumpFrames(frames))
		}
		return nil
	}
	return nil
}

// peerJoinReply is the peer_join_reply step.
func (r *runner) peerJoinReply(st map[string]any) error {
	p := r.pipes[num(st["pipe"])]
	if p == nil {
		return fmt.Errorf("no pipe %v", st["pipe"])
	}
	if p.acc == nil {
		if _, _, err := r.readHello(p); err != nil {
			return err
		}
	}
	reply, err := p.acc.WriteReply(nil, join.ReplyPayload{Decision: join.Decision(getU(st, "decision"))})
	if err != nil {
		return err
	}
	if getB(st, "leading_parameters") {
		block, _ := wire.AppendParams(nil, r.s.LocalParams())
		tp := wire.AppendTransportParams(nil, block)
		if p.framing == core.FramingRecords {
			if err := r.joinerFeed(p, tp); err != nil {
				return err
			}
		} else {
			rec := varint.AppendVarint(nil, uint64(len(tp)))
			reply = append(append(rec, tp...), reply...)
		}
	}
	return r.joinerFeed(p, reply)
}

// joinerRead delivers peer_send bytes to a JOINING carrier.
func (r *runner) joinerRead(p *rpipe, st map[string]any) error {
	var data []byte
	if hx, ok := st["hex"].(string); ok {
		data = hexb(hx)
	} else if recs, ok := st["records"].([]any); ok {
		data = r.enc.encodeRecords(recs)
	}
	return r.joinerFeed(p, data)
}

// joinerFeed runs VerifyReply and JoinFinish over bytes read from a JOINING carrier and checks the
// confirmation of a Reply with decision 0.
func (r *runner) joinerFeed(p *rpipe, data []byte) error {
	if !p.joining {
		return nil
	}
	p.replyIn = append(p.replyIn, data...)
	for p.joining {
		step := r.s.VerifyReply(p.id, p.replyIn, r.now)
		r.drainEvents()
		p.replyIn = p.replyIn[step.N:]
		switch step.Status {
		case core.JoinMore:
			if step.N == 0 {
				return nil
			}
		case core.JoinProven:
			confirm, _, err := r.s.JoinFinish(p.id, 0, &shaping.UnitPlan{})
			r.drainEvents()
			if err != nil {
				return fmt.Errorf("pipe %d: finish: %w", p.id, err)
			}
			p.joining = false
			if p.acc != nil {
				if _, err := p.acc.ReadConfirm(confirm); err != nil {
					return fmt.Errorf("pipe %d: confirmation: %w", p.id, err)
				}
			}
			if len(p.replyIn) > 0 {
				rest := p.replyIn
				p.replyIn = nil
				l := p.lane(0)
				l.in = append(l.in, rest...)
				n := r.s.Receive(p.id, 0, &core.Buf{B: l.in}, l.in, r.now)
				l.in = append([]byte(nil), l.in[n:]...)
				r.drainEvents()
			}
		default:
			p.joining = false
			p.replyIn = nil
		}
	}
	return nil
}

// listenerSend delivers bytes on a listener carrier: its first bytes are classified,
// then the carrier is the handshake's and after Confirmed the attached pipe's.
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
			if c.attached {
				r.s.ReceiveMessage(c.pipe, 0, &core.Buf{B: m}, m, r.now)
				r.drainEvents()
				continue
			}
			if c.pj != nil || c.heldHello != nil {
				c.msgs = append(c.msgs, m)
				continue
			}
			h, _, err := join.ParseHello(m, join.Message)
			if err != nil {
				r.closeCarrier(c, "reject")
				return nil
			}
			r.offerHello(c, &h)
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
	if c.pj != nil || c.heldHello != nil {
		return nil
	}
	cl, n := core.Classify(c.buf, 65535)
	switch cl {
	case core.ClassReject, core.ClassSession:
		r.closeCarrier(c, "reject")
		return nil
	case core.ClassMore:
		return nil
	case core.ClassJoin:
		h, hn, err := join.ParseHello(c.buf, join.Stream)
		if errors.Is(err, join.ErrIncomplete) {
			return nil
		}
		if err != nil {
			r.closeCarrier(c, "reject")
			return nil
		}
		c.buf = append([]byte(nil), c.buf[hn:]...)
		r.offerHello(c, &h)
		return nil
	case core.ClassLane:
	}
	if n == 0 {
		return nil
	}
	if c.slot != nil {
		// The first record is complete: the slot is free again.
		r.pending.Release(c.slot)
		c.slot = nil
	}
	c.via = "lane"
	return r.carrierSend(c, map[string]any{"hex": ""})
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
	o := core.JoinOptions{Dir: map[string]uint8{"out": 0, "in": 1, "bidi": 2}[dir], FateGroup: getU(args, "fate_group"), Mode: join.Trusted}
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
	ci := core.CarrierInfo{QueueBound: true, MinRTT: true}
	o.In, o.Out = ci, ci
	p := &rpipe{id: id, lanes: map[uint64]*rlane{}, writable: true, joining: true}
	p.in, p.out = dir != "out", dir != "in"
	r.pipes[id] = p
	_, res, err := r.s.AddPipe(o)
	if err != nil {
		delete(r.pipes, id)
		return nil, err
	}
	if res == core.AddPipePending {
		return map[string]any{"pending": true}, nil
	}
	return map[string]any{"ok": true}, nil
}

// runnerEpochs is the scripted peer's view of the session epochs: the secrets
// it derived, its own offers and frames, and the local endpoint's last offer and
// frames of each phase and epoch.
type runnerEpochs struct {
	secrets   map[uint64][]byte
	offers    map[uint64]*join.Offer // the peer's offers, until answered
	offerBody map[uint64][]byte
	local     map[[2]uint64][]byte // the local endpoint's last body of a phase and epoch
	localOff  map[uint64]*wire.Frame
	peer      map[[2]uint64]map[string]any // the peer's last frame of a phase and epoch
}

func (e *runnerEpochs) init() {
	if e.secrets == nil {
		e.secrets, e.offers, e.offerBody = map[uint64][]byte{}, map[uint64]*join.Offer{}, map[uint64][]byte{}
		e.local, e.localOff, e.peer = map[[2]uint64][]byte{}, map[uint64]*wire.Frame{}, map[[2]uint64]map[string]any{}
	}
}

// latest is the latest derived epoch of a key phase (0 when none).
func (e *runnerEpochs) latest(phase uint8) uint64 {
	best := uint64(0)
	for n := range e.secrets {
		if uint8(n&1) == phase && n > best {
			best = n
		}
	}
	return best
}

// derive records epoch n from the two bodies and the shared secrets.
func (r *runner) deriveEpoch(n uint64, offerBody, answerBody []byte, ss [][]byte) {
	r.ep.init()
	prev, enc := r.sessionKeys()
	if n > 1 {
		prev = r.ep.secrets[n-1]
	}
	r.ep.secrets[n] = join.NextEpochSecret(prev, n, enc, offerBody, answerBody, ss...)
}

// localEpoch notes a SESSION_EPOCH the local endpoint wrote: whether it repeats
// the previous one of its phase and epoch, and the secret of an answer to the
// peer's offer.
func (r *runner) localEpoch(f *wire.Frame, o map[string]any) {
	r.ep.init()
	key := [2]uint64{f.Status, f.Seq}
	prev, seen := r.ep.local[key]
	o["same_as_previous"] = seen && bytes.Equal(prev, f.Body)
	r.ep.local[key] = bytes.Clone(f.Body)
	switch f.Status {
	case wire.EpochOffer:
		g := *f
		g.Share, g.Body = bytes.Clone(f.Share), bytes.Clone(f.Body)
		r.ep.localOff[f.Seq] = &g
	case wire.EpochAnswer:
		if off := r.ep.offers[f.Seq]; off != nil {
			ss, err := off.Finish(f.Share)
			if err == nil {
				r.deriveEpoch(f.Seq, r.ep.offerBody[f.Seq], f.Body, ss)
			}
			delete(r.ep.offers, f.Seq)
		}
	}
}

// peerEpoch is the peer_epoch step.
func (r *runner) peerEpoch(st map[string]any) error {
	r.ep.init()
	phase := epochPhases[st["phase"].(string)]
	n := num(st["epoch"])
	key := [2]uint64{uint64(phase), n}
	var fr map[string]any
	if getB(st, "repeat") {
		if fr = r.ep.peer[key]; fr == nil {
			return fmt.Errorf("no previous %s of epoch %d to repeat", st["phase"], n)
		}
	} else {
		g, _ := join.LookupGroup(join.GroupX25519MLKEM768)
		loc, _ := st["locator"].(string)
		var share []byte
		switch phase {
		case wire.EpochOffer:
			off, err := g.NewOffer(nil)
			if err != nil {
				return err
			}
			share = off.Share
			body := bodyOf(wire.AppendSessionEpoch(nil, phase, n, share, hexb(loc)))
			r.ep.offers[n], r.ep.offerBody[n] = off, body
		case wire.EpochAnswer:
			if lo := r.ep.localOff[n]; lo != nil {
				var ss [][]byte
				var err error
				if share, ss, err = g.Respond(lo.Share, nil); err != nil {
					return err
				}
				body := bodyOf(wire.AppendSessionEpoch(nil, phase, n, share, hexb(loc)))
				r.deriveEpoch(n, lo.Body, body, ss)
			} else {
				share = make([]byte, g.ShareR)
			}
		default:
			return fmt.Errorf("peer_epoch phase %v", st["phase"])
		}
		if l, ok := st["share_len"]; ok {
			share = make([]byte, num(l))
		}
		fr = map[string]any{"type": "SESSION_EPOCH", "phase": st["phase"], "epoch": json.Number(fmt.Sprint(n)),
			"share": hex.EncodeToString(share), "locator": loc}
		r.ep.peer[key] = fr
	}
	return r.peerSend(map[string]any{"pipe": st["pipe"], "records": []any{[]any{fr}}})
}

// bodyOf is the body of an encoded SESSION_EPOCH: the bytes after its type.
func bodyOf(f []byte) []byte {
	_, n := varint.ConsumeVarint(f)
	return f[n:]
}
