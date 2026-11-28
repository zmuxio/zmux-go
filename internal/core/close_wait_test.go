package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// A virtual-clock simulation of two sessions over their pipes and lanes,
// for the graceful close's wait for PIPE_ACK coverage (SPEC.md Section 6.5
// step 1, MULTIPATH.md Section 13): while CONNECTION_CLOSE waits, each
// side keeps acknowledging and reinjecting, so the wait ends in about one
// RTT, never at its bound, and loses nothing a failed pipe carried.

type cwKey struct {
	from       int
	pipe, lane uint64
}

type cwMsg struct {
	data []byte
	at   Time
}

// cwLink is one direction of one lane: a fixed delay, order kept.
type cwLink struct {
	q    []cwMsg
	rx   []byte // stream framing: bytes of a record not complete yet
	dead bool
}

type cwSim struct {
	conn    [2]any
	end     [2]*Session
	t       *testing.T
	links   map[cwKey]*cwLink
	actions []Action
	info    CarrierInfo
	delay   Duration // one way
	now     Time
}

var errCWDead = errors.New("carrier failed")

func newCWSim(t *testing.T, cfg [2]Config, info CarrierInfo, delay Duration) *cwSim {
	t.Helper()
	m := &cwSim{t: t, links: map[cwKey]*cwLink{}, delay: delay, info: info, now: Time(Second)}
	m.conn = [2]any{new(int), new(int)}
	for i := range 2 {
		s := New(cfg[i], m.now)
		s.AttachBootstrap(info, info, nil)
		s.SetCarrierConn(0, m.conn[i^1]) // the peer's lanes arrive on our carrier
		m.end[i] = s
	}
	return m
}

func (m *cwSim) link(from int, pipe, lane uint64) *cwLink {
	k := cwKey{from, pipe, lane}
	l := m.links[k]
	if l == nil {
		l = &cwLink{}
		m.links[k] = l
	}
	return l
}

// flush writes everything side i wants to write now.
func (m *cwSim) flush(i int) {
	s := m.end[i]
	for _, p := range s.pipes {
		if p.out == nil || p.out.failed {
			continue
		}
		for li := range len(p.out.lanes) {
			lane := p.out.lanes[li].lane
			l := m.link(i, p.id, lane)
			for n := 0; s.WantsWrite(p.id, lane); n++ {
				if n > 1000 {
					m.t.Fatal("writer spins")
				}
				b := GetBatch()
				if !s.PollTransmit(p.id, lane, b, DefaultBatch, m.now) {
					PutBatch(b)
					break
				}
				var err error
				switch {
				case l.dead:
					err = errCWDead
				case m.info.Framing == FramingRecords:
					for _, r := range b.Messages() {
						l.q = append(l.q, cwMsg{data: append([]byte(nil), r...), at: m.now.Add(m.delay)})
					}
				default:
					l.q = append(l.q, cwMsg{data: b.Flat(), at: m.now.Add(m.delay)})
				}
				s.WriteDone(b, 0, err, m.now)
				PutBatch(b)
				if err != nil {
					break
				}
			}
		}
	}
}

// deliver hands side i what reached it by now.
func (m *cwSim) deliver(i int) {
	s := m.end[i]
	for k, l := range m.links {
		if k.from == i^1 {
			for len(l.q) > 0 && l.q[0].at <= m.now {
				d := l.q[0].data
				l.q = l.q[1:]
				if m.info.Framing == FramingRecords {
					receiveMessageTest(s, k.pipe, k.lane, d, m.now)
					continue
				}
				l.rx = append(l.rx, d...)
				n := receiveTest(s, k.pipe, k.lane, l.rx, m.now)
				l.rx = append([]byte(nil), l.rx[n:]...)
			}
		}
	}
}

// carrier runs side i's carrier commands: lanes open at once, the peer
// attaching the new lane.
func (m *cwSim) carrier(i int) {
	s := m.end[i]
	m.actions = s.TakeActions(m.actions[:0])
	for _, a := range m.actions {
		if a.Kind != ActOpenLane {
			continue
		}
		lane, ok := s.LaneOpened(a.Pipe, m.info, nil)
		if !ok {
			continue
		}
		if !m.end[i^1].AcceptLane(m.conn[i], a.Pipe, lane, m.info) {
			m.t.Fatalf("lane %d of pipe %d refused", lane, a.Pipe)
		}
	}
}

// step advances the clock by one millisecond.
func (m *cwSim) step() {
	m.now = m.now.Add(Millisecond)
	for i := range 2 {
		s := m.end[i]
		s.SetNow(m.now)
		if s.NextTimeout() <= m.now {
			s.HandleTimeout(m.now)
		}
		m.deliver(i)
		m.carrier(i)
		m.flush(i)
		m.carrier(i)
	}
}

func (m *cwSim) run(d Duration, until func() bool) bool {
	end := m.now.Add(d)
	for m.now < end {
		if until() {
			return true
		}
		m.step()
	}
	return until()
}

func (m *cwSim) established() {
	m.t.Helper()
	if !m.run(5*Second, func() bool { return m.end[0].Established() && m.end[1].Established() }) {
		m.t.Fatal("not established")
	}
}

// fail ends pipe id's carrier in both directions: what it holds in flight
// is lost, its writes fail and both ends read its failure.
func (m *cwSim) fail(id uint64) {
	for k, l := range m.links {
		if k.pipe == id {
			l.dead, l.q, l.rx = true, nil, nil
		}
	}
	for i := range 2 {
		m.end[i].PipeEnded(id, 0, false, false, errCWDead, nil, m.now)
	}
}

// closeSent reports that side i's CONNECTION_CLOSE was written.
func (m *cwSim) closeSent(i int) bool {
	s := m.end[i]
	return s.state == StateClosed || s.state == StateClosing && !s.closeFr.pending
}

// reader collects what a stream of side i carries.
type cwReader struct {
	m   *cwSim
	st  *Stream
	got []byte
	buf []byte
	i   int
}

func (r *cwReader) read() {
	s := r.m.end[r.i]
	if r.st == nil {
		if r.st, _ = s.AcceptStream(true); r.st == nil {
			return
		}
	}
	if r.buf == nil {
		r.buf = make([]byte, 64<<10)
	}
	for {
		n, _, _ := s.Read(r.st, r.buf)
		if n == 0 {
			return
		}
		r.got = append(r.got, r.buf[:n]...)
	}
}

func cwPayload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

func cwConfigs() [2]Config {
	var cfg [2]Config
	for i := range 2 {
		cfg[i] = DefaultConfig()
		cfg[i].Local.LaneKey[0] = byte(i + 1)
	}
	cfg[1].Role, cfg[1].TransportRole = RoleResponder, RoleResponder
	return cfg
}

func cwMPConfigs() [2]Config {
	cfg := cwConfigs()
	for i := range 2 {
		c := &cfg[i]
		c.SessionPolicy.Limits.SteadyAttachments = 2
		c.PSK = []byte("an example pre-shared key, 32 B!")
		c.Declared = true
		c.Local.Has |= wire.HasMultipath | wire.HasSessionCrypto
		c.Local.SessionCrypto = join.LocalCrypto()
		c.Local.SecretFlags |= wire.SecretPSK
		c.Local.SessionNonce[0] = byte(i + 1)
	}
	cfg[1].Local.Locator = []byte("locator1") // the acceptor's
	return cfg
}

// join joins a second pipe from side 0 through the session's join entries.
func (m *cwSim) join() uint64 {
	m.t.Helper()
	a, b := m.end[0], m.end[1]
	id, res, err := a.AddPipe(JoinOptions{In: m.info, Out: m.info, Dir: 2})
	if err != nil || res != AddPipeOK {
		m.t.Fatalf("AddPipe: %v %v", res, err)
	}
	if _, ready, err := a.JoinPrepare(id, NewSignal()); err != nil || !ready {
		m.t.Fatalf("JoinPrepare: %v %v", ready, err)
	}
	plan := &JoinWrite{}
	hello, _, err := a.JoinHello(id, m.info.Framing, 0, plan)
	if err != nil {
		m.t.Fatal(err)
	}
	h, _, err := join.ParseHelloExact(hello, join.Message)
	if err != nil {
		m.t.Fatal(err)
	}
	pj, jr := b.OpenHello(h, m.info.Framing, nil)
	if jr != JoinAccepted {
		m.t.Fatalf("OpenHello: %v", jr)
	}
	reply, jr, _ := b.ClaimHello(pj, Carrier{HasIn: true, HasOut: true}, 0, len(hello), plan)
	if jr != JoinAccepted {
		m.t.Fatalf("ClaimHello: %v", jr)
	}
	if st := a.VerifyReply(id, reply, m.now); st.Status != JoinProven {
		m.t.Fatalf("VerifyReply: %+v", st)
	}
	confirm, _, err := a.JoinFinish(id, 0, plan)
	if err != nil {
		m.t.Fatal(err)
	}
	if _, _, done, err := pj.ReadConfirm(confirm); err != nil || !done {
		m.t.Fatalf("ReadConfirm: %v %v", done, err)
	}
	if b.ConfirmJoin(pj, Carrier{HasIn: true, HasOut: true, In: m.info, Out: m.info}) != JoinAccepted {
		m.t.Fatal("ConfirmJoin")
	}
	return id
}

// unackedPipe returns a pipe of side i holding unacknowledged records.
func (m *cwSim) unackedPipe(i int) (uint64, bool) {
	for _, p := range m.end[i].pipes {
		if p.out == nil || p.out.failed {
			continue
		}
		for _, l := range p.out.lanes {
			if l.acked < l.pos {
				return p.id, true
			}
		}
	}
	return 0, false
}

// dataPipe returns a live pipe of side i carrying unacknowledged stream
// data.
func (m *cwSim) dataPipe(i int) (uint64, bool) {
	for _, p := range m.end[i].pipes {
		if p.out == nil || p.out.failed {
			continue
		}
		for _, l := range p.out.lanes {
			for _, e := range l.log {
				if e.d.kind == dStream && e.d.n > 0 {
					return p.id, true
				}
			}
		}
	}
	return 0, false
}

// A pipe that fails while CONNECTION_CLOSE waits for its PIPE_ACK: what
// it carried goes again on the other pipe, and the close follows once that
// copy is acknowledged, well within the wait's bound.
func TestCloseWaitReinjectsFailedPipe(t *testing.T) {
	info := CarrierInfo{Framing: FramingRecords}
	m := newCWSim(t, cwMPConfigs(), info, 20*Millisecond)
	m.established()
	if !m.end[0].feat.Multipath {
		t.Fatal("no multipath")
	}
	m.join()
	m.run(500*Millisecond, func() bool { return false }) // both pipes in use and measured
	c := m.end[0]
	st, err := c.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	data := cwPayload(48<<10, 3)
	if n, err := c.Write(st, data, true); err != nil || n != len(data) {
		t.Fatalf("write: %d %v", n, err)
	}
	r := &cwReader{m: m, i: 1}
	m.flush(0)
	if c.Flushable() {
		t.Fatal("the data was not handed to the carriers")
	}
	x, ok := m.dataPipe(0)
	if !ok {
		t.Fatal("no stream data in flight")
	}
	c.Close(0x10, nil)
	if !c.closeFr.waitLanes {
		t.Fatal("CONNECTION_CLOSE does not wait for the data in flight")
	}
	start, bound := m.now, c.timers.laneWait
	m.fail(x)
	if !m.run(2*Second, func() bool { r.read(); return m.closeSent(0) }) {
		t.Fatal("the close did not end")
	}
	if m.now >= bound {
		t.Fatalf("the close waited %v, to its bound", m.now.Sub(start))
	}
	m.run(500*Millisecond, func() bool { r.read(); return len(r.got) == len(data) })
	if !bytes.Equal(r.got, data) {
		t.Fatalf("the peer read %d of %d bytes intact (pipe %d failed in the close's wait)", len(r.got), len(data), x)
	}
}

// cwSimultaneous writes on both sides, closes both at once with the data
// unacknowledged, and checks that each wait ends in about one RTT.
func cwSimultaneous(t *testing.T, m *cwSim) {
	t.Helper()
	var r [2]*cwReader
	var data [2][]byte
	for i := range 2 {
		st, err := m.end[i].OpenStream(true)
		if err != nil {
			t.Fatal(err)
		}
		st.urgency = 3 // a data lane when there are lanes
		data[i] = cwPayload(16<<10, byte(i+5))
		if _, err := m.end[i].Write(st, data[i], true); err != nil {
			t.Fatal(err)
		}
		r[i^1] = &cwReader{m: m, i: i ^ 1}
		m.flush(i)
	}
	for i := range 2 {
		if _, ok := m.unackedPipe(i); !ok {
			t.Fatalf("side %d: nothing unacknowledged", i)
		}
		m.end[i].Close(0x11, nil)
		if !m.end[i].closeFr.waitLanes {
			t.Fatalf("side %d: CONNECTION_CLOSE does not wait", i)
		}
	}
	start, bound := m.now, m.end[0].timers.laneWait
	rtt := 2 * m.delay
	if !m.run(2*Second, func() bool {
		r[0].read()
		r[1].read()
		return m.closeSent(0) && m.closeSent(1)
	}) {
		t.Fatal("the close did not end")
	}
	took := m.now.Sub(start)
	// One RTT plus the peer's acknowledgement delay, far from the bound.
	if lim := rtt + 2*m.end[0].ack.peerDelay + 10*Millisecond; took > lim || m.now >= bound {
		t.Fatalf("simultaneous close took %v, want at most %v (bound %v)", took, lim, bound.Sub(start))
	}
}

func TestCloseWaitSimultaneousMultipath(t *testing.T) {
	m := newCWSim(t, cwMPConfigs(), CarrierInfo{Framing: FramingRecords}, 20*Millisecond)
	m.established()
	m.join()
	m.run(500*Millisecond, func() bool { return false })
	cwSimultaneous(t, m)
}

// Without multipath the wait covers data lanes: their PIPE_ACK is as
// much owed while both sides close.
func TestCloseWaitSimultaneousLanes(t *testing.T) {
	m := newCWSim(t, cwConfigs(), CarrierInfo{Lanes: true}, 20*Millisecond)
	m.established()
	if !m.end[0].feat.LanesOut || !m.end[1].feat.LanesOut {
		t.Fatal("no lanes")
	}
	// Data written once lane 0 is acknowledged opens the data lanes.
	for round := range 2 {
		for i := range 2 {
			st, err := m.end[i].OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			st.urgency = 3
			if _, err := m.end[i].Write(st, cwPayload(16<<10, byte(round)), true); err != nil {
				t.Fatal(err)
			}
		}
		m.run(200*Millisecond, func() bool { return false })
	}
	for i := range 2 {
		if lanes := m.end[i].OutLanes(0, nil); len(lanes) < 2 {
			t.Fatalf("side %d: lanes %v", i, lanes)
		}
	}
	cwSimultaneous(t, m)
}
