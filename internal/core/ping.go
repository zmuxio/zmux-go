package core

import "github.com/zmuxio/zmux-go/internal/wire"

// pingState is QX_PING (SPEC.md 4.5): at most one request outstanding, one
// response owed.
type pingState struct {
	next         uint64 // next request sequence number
	outstanding  uint64 // seq+1 of the outstanding request, 0 = none
	sentAt       Time   // when it was produced
	expires      Time   // stops being outstanding then
	want         bool   // a request should be sent
	requestFirst bool   // the first record of an endpoint without a carrier RTT
	owe          bool   // a response is owed
	oweSeq       uint64 // the largest unanswered request
	reqSeen      uint64 // largest request seq + 1 received
	respSeen     uint64 // largest response seq + 1 received
	maxSent      uint64 // largest request seq + 1 sent
	waiters      []*Signal
	completed    uint64 // responses received (tickets)
	lastRTT      Duration
	fence        uint64 // seq+1 that completes the GOAWAY fence
	pipe         uint64 // pipe that carried the outstanding request
}

func (p *pingState) wants() bool { return p.owe || p.want && p.outstanding == 0 || p.requestFirst }

func (p *pingState) wakeAll(s *Session) {
	for _, w := range p.waiters {
		s.wake(w)
	}
	p.waiters = nil
}

// Ping asks for a QX_PING request, or joins the outstanding one. It returns
// a ticket: the ping completes once PingDone(ticket) is true.
func (s *Session) Ping(sig *Signal) (uint64, error) {
	if s.err != nil {
		return 0, s.err
	}
	if s.ping.outstanding == 0 {
		s.ping.want = true
		s.wakeAllWriters()
	}
	if sig != nil {
		s.ping.waiters = append(s.ping.waiters, sig)
	}
	return s.ping.completed + 1, nil
}

// PingDone reports whether the ticket completed, with the measured RTT.
func (s *Session) PingDone(ticket uint64) (bool, Duration, error) {
	if s.ping.completed >= ticket {
		return true, s.ping.lastRTT, nil
	}
	if s.err != nil {
		return false, 0, s.err
	}
	return false, 0, nil
}

// CancelPingWait removes sig.
func (s *Session) CancelPingWait(sig *Signal) { s.ping.waiters = removeSig(s.ping.waiters, sig) }

// writePing writes an owed response and a wanted request.
func (s *Session) writePing(w *recWriter) {
	s.writePingResponse(w)
	s.writePingRequest(w)
}

// writePingResponse answers the largest pending request.
func (s *Session) writePingResponse(w *recWriter) {
	p := &s.ping
	if p.owe {
		seq := p.oweSeq
		n := 8 + wire.VarintLen(seq)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPing(b, true, seq) }) {
			return
		}
		p.owe = false
		w.onlyPing = false
		w.activity = true
	}
}

// writePingRequest sends a wanted request when none is outstanding.
func (s *Session) writePingRequest(w *recWriter) {
	p := &s.ping
	if (p.want || p.requestFirst) && p.outstanding == 0 {
		seq := p.next
		n := 8 + wire.VarintLen(seq)
		if !w.frame(n, true, func(b []byte) []byte { return wire.AppendPing(b, false, seq) }) {
			return
		}
		p.next++
		p.pipe = w.b.Pipe
		p.outstanding = seq + 1
		p.maxSent = seq + 1
		p.sentAt = s.now
		p.expires = s.now.Add(maxD(Second, MulD(s.srtt(), 3)))
		p.want, p.requestFirst = false, false
		w.activity = true
	}
}

// onPing processes QX_PING.
func (s *Session) onPing(f *wire.Frame, pp *Pipe) *wire.Error {
	p := &s.ping
	if !f.Response {
		if f.Value+1 <= p.reqSeen {
			if !s.feat.Multipath {
				return wire.Errorf(wire.ProtocolViolation, "QX_PING request not increasing", "value", f.Value)
			}
			return nil // outdated: ignored, never chargeable
		}
		p.reqSeen = f.Value + 1
		if p.owe {
			s.charge()
		}
		p.owe, p.oweSeq = true, f.Value
		s.wakeAllWriters()
		return nil
	}
	if f.Value+1 > p.maxSent {
		return wire.Errorf(wire.ProtocolViolation, "QX_PING response above the largest request", "value", f.Value)
	}
	if f.Value+1 <= p.respSeen {
		if !s.feat.Multipath {
			s.charge()
		}
		return nil
	}
	p.respSeen = f.Value + 1
	if p.outstanding != 0 && f.Value+1 >= p.outstanding {
		rtt := s.now.Sub(p.sentAt)
		p.lastRTT = rtt
		p.outstanding = 0
		p.completed++
		s.tunePingSample(rtt)
		s.pacePing(rtt)
		p.wakeAll(s)
		if p.fence != 0 && f.Value+1 >= p.fence {
			s.fenceDone()
		}
	}
	return nil
}

// pingExpired handles an outstanding request that timed out.
func (s *Session) pingExpired() {
	p := &s.ping
	p.outstanding = 0
	if len(p.waiters) > 0 || p.fence != 0 {
		p.want = true
		if p.fence != 0 {
			p.fence = p.next + 1
		}
		s.wakeAllWriters()
	}
}

// ---- no-effect budget (SPEC.md 9.4) ----

const (
	budgetCap    = 4096
	budgetRefill = 32
)

type budget struct {
	tokens int64
	last   Time
}

// charge subtracts one token for a chargeable frame.
func (s *Session) charge() {
	b := &s.budget
	el := s.now.Sub(b.last)
	if el >= Second/budgetRefill {
		add := el / (Second / budgetRefill)
		b.tokens = min(budgetCap, b.tokens+add)
		b.last = b.last.Add(add * (Second / budgetRefill))
		if b.tokens == budgetCap {
			b.last = s.now
		}
	}
	b.tokens--
	if b.tokens < 0 {
		s.failLocal(wire.Errorf(wire.ExcessiveLoad, "no-effect budget exhausted"))
	}
}

// BudgetTokens returns the current tokens (statistic).
func (s *Session) BudgetTokens() int64 { return s.budget.tokens }
