package machine

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
)

type metric uint8

const (
	metricAge metric = iota // microseconds since the member was added
	metricIdle
	metricPipes
	metricBytesSent
	metricBytesReceived
	metricBytes
)

var metricNames = map[string]metric{"age": metricAge, "idle": metricIdle, "pipes": metricPipes,
	"bytes_sent": metricBytesSent, "bytes_received": metricBytesReceived, "bytes": metricBytes}

type retireRule struct {
	metric metric
	above  value
	jitter uint32
	slot   int32 // into a member's kept thresholds
}

// poolDef is a pool policy. Absent members keep the pool's own behaviour.
type poolDef struct {
	count         value
	dialAfter     []value
	dialJitter    uint32
	retire        []retireRule
	probeInterval value
	probeJitter   uint32
	hasProbeJit   bool
}

func parsePool(x any, t *tables) (*poolDef, error) {
	o, err := members(x, "pool", "count", "dial_after", "dial_jitter", "retire", "probe_interval", "probe_jitter")
	if err != nil {
		return nil, err
	}
	p := &poolDef{}
	jit := func(v any, what string) (uint32, error) {
		n, err := integer(v, what)
		if err != nil {
			return 0, err
		}
		if n < 0 || n >= 1_000_000 {
			return 0, fail("range", "%s: %d", what, n)
		}
		return uint32(n), nil
	}
	if v := o.get("count"); v != nil {
		if p.count, err = parseValue(v, famPoolCount, t, false, "count"); err != nil {
			return nil, err
		}
	}
	if v := o.get("dial_after"); v != nil {
		a, ok := v.([]any)
		if !ok {
			return nil, fail("type", "dial_after")
		}
		p.dialAfter = make([]value, len(a))
		for i := range a {
			if p.dialAfter[i], err = parseValue(a[i], famTime, t, false, "dial_after"); err != nil {
				return nil, err
			}
		}
	}
	if v := o.get("dial_jitter"); v != nil {
		if p.dialJitter, err = jit(v, "dial_jitter"); err != nil {
			return nil, err
		}
		if len(p.dialAfter) == 0 {
			return nil, fail("conflict", "dial_jitter without dial_after")
		}
	}
	if v := o.get("retire"); v != nil {
		a, ok := v.([]any)
		if !ok {
			return nil, fail("type", "retire")
		}
		p.retire = make([]retireRule, len(a))
		for i := range a {
			q, err := members(a[i], "retire", "metric", "above", "jitter")
			if err != nil {
				return nil, err
			}
			m, err := required(q, "metric", "retire")
			if err != nil {
				return nil, err
			}
			name, _ := m.(string)
			r := &p.retire[i]
			var known bool
			if r.metric, known = metricNames[name]; !known {
				return nil, fail("type", "metric")
			}
			fam := famCounter
			if r.metric == metricAge || r.metric == metricIdle {
				fam = famTime
			}
			ab, err := required(q, "above", "retire")
			if err != nil {
				return nil, err
			}
			if r.above, err = parseValue(ab, fam, t, true, "above"); err != nil {
				return nil, err
			}
			if v := q.get("jitter"); v != nil {
				if r.jitter, err = jit(v, "jitter"); err != nil {
					return nil, err
				}
			}
			r.slot = int32(i)
		}
	}
	if v := o.get("probe_interval"); v != nil {
		if p.probeInterval, err = parseValue(v, famTime, t, false, "probe_interval"); err != nil {
			return nil, err
		}
	}
	if v := o.get("probe_jitter"); v != nil {
		if p.probeJitter, err = jit(v, "probe_jitter"); err != nil {
			return nil, err
		}
		if !p.probeInterval.set {
			return nil, fail("conflict", "probe_jitter without probe_interval")
		}
		p.hasProbeJit = true
	}
	return p, nil
}

func (p *poolDef) values(f func(*value)) {
	f(&p.count)
	for i := range p.dialAfter {
		f(&p.dialAfter[i])
	}
	for i := range p.retire {
		f(&p.retire[i].above)
	}
	f(&p.probeInterval)
}

// resolved is p with its shared values drawn, in the order of its members.
func (p *poolDef) resolved(src rand.Source, shared *[]uint64) *poolDef {
	q := *p
	q.dialAfter = append([]value(nil), p.dialAfter...)
	q.retire = append([]retireRule(nil), p.retire...)
	q.values(func(v *value) {
		if v.set {
			*v = v.resolved(src, shared)
		}
	})
	return &q
}

// PoolPolicy returns a new pool policy of the candidate, nil without a pool:
// each pool needs its own, since a policy keeps per-member draws. Values of
// scope pipe are kept once per member ID while the member is in the view.
func (c *Compiled) PoolPolicy() zmux.PoolPolicy {
	if c.pool == nil {
		return nil
	}
	return &poolPolicy{s: c.pool, src: c.o.source(), members: map[uint64]*poolMember{}}
}

// poolPolicy decides under its lock: a pool decides serially, so the lock is
// not contended.
type poolPolicy struct {
	mu      sync.Mutex
	s       *poolDef
	src     rand.Source
	members map[uint64]*poolMember
	seen    map[uint64]bool
	// The current dial wait: its draw and the view it began from.
	waiting   bool
	waitU     uint64
	waitFails int
	waitSince int64 // microseconds since the last dial; <0: no dial
}

type poolMember struct {
	above []uint64 // per rule: scope pipe values
	has   []bool
	u     []uint64 // per rule: the jitter draw
}

func metricOf(k metric, m *zmux.PoolMember) uint64 {
	switch k {
	case metricAge:
		return uint64(max(m.Age, 0) / time.Microsecond)
	case metricIdle:
		return uint64(max(m.Idle, 0) / time.Microsecond)
	case metricPipes:
		return uint64(max(m.Pipes, 0))
	case metricBytesSent:
		return m.BytesSent
	case metricBytesReceived:
		return m.BytesReceived
	case metricBytes:
	}
	x := m.BytesSent + m.BytesReceived
	if x < m.BytesSent {
		return ^uint64(0)
	}
	return x
}

// memberKey names member m of v: its fate group in a carrier.ConnPool, its pipe
// ID in a PipePool.
func memberKey(v *zmux.PoolView, m *zmux.PoolMember) uint64 {
	if v.Connections {
		return m.FateGroup
	}
	return uint64(m.PipeID)
}

// Decide sets d as the policy states, and d.Recheck to the earliest of the
// time until an age or idle rule of a member not retired would hold and the
// time until the dial wait after the last dial elapses (0: neither).
func (p *poolPolicy) Decide(v *zmux.PoolView, d *zmux.PoolDecision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen == nil {
		p.seen = map[uint64]bool{}
	}
	clear(p.seen)
	for i := range v.Members {
		p.seen[memberKey(v, &v.Members[i])] = true
	}
	for id := range p.members {
		if !p.seen[id] {
			delete(p.members, id)
		}
	}
	s := p.s
	if s.count.set {
		d.Count = int(s.count.drawTable(p.src))
	}
	if n := len(s.dialAfter); n > 0 {
		k := min(max(v.Failures, 0), n-1)
		x := s.dialAfter[k].drawTable(p.src)
		if s.dialJitter > 0 {
			x = jitterWith(x, s.dialJitter, p.dialDraw(v))
		}
		d.DialAfter = micros(x)
	}
	if s.probeInterval.set {
		d.Liveness.ProbeInterval = micros(s.probeInterval.drawTable(p.src))
		if d.Liveness.ProbeInterval == 0 {
			d.Liveness.ProbeInterval = -1 // off
		}
		if s.hasProbeJit {
			d.Liveness.ProbeJitter = float64(s.probeJitter) / 1e6
			if s.probeJitter == 0 {
				d.Liveness.ProbeJitter = -1 // no jitter
			}
		}
	}
	var recheck time.Duration
	ask := func(t time.Duration) {
		if t > 0 && (recheck == 0 || t < recheck) {
			recheck = t
		}
	}
	if !v.LastDial.IsZero() && d.DialAfter > 0 {
		ask(v.LastDial.Add(d.DialAfter).Sub(v.Now))
	}
	if len(s.retire) > 0 {
		for i := range v.Members {
			m := &v.Members[i]
			key := memberKey(v, m)
			if !p.seen[key] { // a repeated ID, retired already
				continue
			}
			pm := p.members[key]
			if pm == nil {
				pm = &poolMember{above: make([]uint64, len(s.retire)), has: make([]bool, len(s.retire)), u: make([]uint64, len(s.retire))}
				for j := range s.retire {
					if s.retire[j].jitter > 0 {
						pm.u[j] = p.src.Uint64()
					}
				}
				p.members[key] = pm
			}
			var soon time.Duration
			for j := range s.retire {
				r := &s.retire[j]
				x := metricOf(r.metric, m)
				var above uint64
				if r.above.sc == scopePipe && r.above.segs != nil {
					if !pm.has[j] {
						pm.above[j], pm.has[j] = r.above.drawTable(p.src), true
					}
					above = pm.above[j]
				} else {
					above = r.above.drawTable(p.src)
				}
				if r.jitter > 0 {
					above = jitterWith(above, r.jitter, pm.u[j])
				}
				if x > above {
					if v.Connections {
						d.RetireConnections = append(d.RetireConnections, m.FateGroup)
					} else {
						d.RetirePipes = append(d.RetirePipes, m.PipeID)
					}
					p.seen[key] = false
					soon = 0
					break
				}
				if r.metric == metricAge || r.metric == metricIdle {
					// At the threshold, the smallest wait: 0 means none.
					if t := micros(max(above-x, 1)); soon == 0 || t < soon {
						soon = t
					}
				}
			}
			ask(soon)
		}
	}
	d.Recheck = recheck
}

// dialDraw returns the draw of the current dial wait, starting a new wait
// when the failure count differs from the previous Decide's, when the time
// since the last dial got shorter, or when a first dial appeared.
func (p *poolPolicy) dialDraw(v *zmux.PoolView) uint64 {
	since := int64(-1)
	if !v.LastDial.IsZero() {
		since = int64(v.Now.Sub(v.LastDial) / time.Microsecond)
	}
	if !p.waiting || v.Failures != p.waitFails || since >= 0 && (p.waitSince < 0 || since < p.waitSince) {
		p.waiting, p.waitU = true, p.src.Uint64()
	}
	p.waitFails, p.waitSince = v.Failures, since
	return p.waitU
}
