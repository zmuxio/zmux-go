package machine

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
)

// Metric names a quantity of a pool member that a retire rule compares.
type Metric string

const (
	MetricAge           Metric = "age"  // seconds since the member was added
	MetricIdle          Metric = "idle" // seconds the member has been idle
	MetricPipes         Metric = "pipes"
	MetricBytesSent     Metric = "bytes_sent"
	MetricBytesReceived Metric = "bytes_received"
	MetricBytes         Metric = "bytes" // bytes_sent + bytes_received
)

// metrics maps each Metric to its value for a member.
var metrics = map[Metric]func(*zmux.PoolMember) float64{
	MetricAge:           func(m *zmux.PoolMember) float64 { return m.Age.Seconds() },
	MetricIdle:          func(m *zmux.PoolMember) float64 { return m.Idle.Seconds() },
	MetricPipes:         func(m *zmux.PoolMember) float64 { return float64(m.Pipes) },
	MetricBytesSent:     func(m *zmux.PoolMember) float64 { return float64(m.BytesSent) },
	MetricBytesReceived: func(m *zmux.PoolMember) float64 { return float64(m.BytesReceived) },
	MetricBytes: func(m *zmux.PoolMember) float64 {
		return float64(m.BytesSent) + float64(m.BytesReceived)
	},
}

// PoolSpec is a pool policy. Absent fields keep the pool's own behaviour.
type PoolSpec struct {
	// Count is the number of members to keep, rounded half away from zero:
	// at least 1 and at most 2^53 as stated, so a distribution that may
	// draw below 1 needs a min of 1 or more. A draw above 2^53 makes the
	// decision invalid (PoolDecision.Validate).
	Count *Value `json:"count,omitempty"`
	// DialAfter holds the delay in seconds of the next dial after the last
	// one, by consecutive failed dials: entry k for k failures, the last
	// entry for any higher number. The numbers stated lie in [0, 1e9] and
	// a distribution that may draw below 0 needs a min of 0 or more.
	DialAfter []Value `json:"dial_after,omitempty"`
	// DialJitter, 0 to below 1, reduces DialAfter by a random fraction of
	// up to it, drawn once per wait: a wait starts when the failure count
	// differs from the previous Decide, when the time since the last dial
	// got shorter (a new dial), or at the first dial. It needs DialAfter.
	DialJitter *float64 `json:"dial_jitter,omitempty"`
	// Retire lists the rules by which members are retired.
	Retire []RetireRule `json:"retire,omitempty"`
	// ProbeInterval is the probe interval in seconds of the pipes the pool
	// dials (scope shared or each). A sample of 0 turns their idle probes
	// off (PoolDecision.DisableIdleProbe). The numbers stated are not
	// negative, nor is its lower bound; a sample in (0, 1) or above 1e9 is
	// passed on as it is, which makes the decision invalid
	// (PoolDecision.Validate): the pool rejects it.
	ProbeInterval *Value `json:"probe_interval,omitempty"`
	// ProbeJitter, 0 to below 1, is the probe jitter of those pipes. It
	// needs ProbeInterval.
	ProbeJitter *float64 `json:"probe_jitter,omitempty"`
}

// RetireRule retires a member whose Metric is greater than Above reduced
// by a random fraction of up to Jitter (0 to below 1), drawn once per
// member and rule when the member first appears in the view.
type RetireRule struct {
	Metric Metric  `json:"metric"`
	Above  Value   `json:"above"`
	Jitter float64 `json:"jitter,omitempty"`
}

// jitterOK reports a valid jitter: 0 <= j < 1.
func jitterOK(j float64) bool { return j >= 0 && j < 1 }

// ParsePoolSpec reads a JSON PoolSpec and validates it. Parsing is strict
// as for Parse.
func ParsePoolSpec(b []byte) (*PoolSpec, error) {
	var s PoolSpec
	if err := decodeChecked(b, poolShape, &s); err != nil {
		return nil, fmt.Errorf("machine: pool: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate reports the first error of s. Scope pipe (kept per member) is
// accepted only in Retire thresholds.
func (s *PoolSpec) Validate() error {
	free := func(v *Value, what string, f family) error {
		if err := v.checkIn(what, f); err != nil {
			return fmt.Errorf("machine: pool %w", err)
		}
		if scopedPipe(v) {
			return fmt.Errorf("machine: pool %s: scope pipe only in retire", what)
		}
		return nil
	}
	if s.Count != nil {
		if err := free(s.Count, "count", famPoolCount); err != nil {
			return err
		}
	}
	for i := range s.DialAfter {
		if err := free(&s.DialAfter[i], "dial_after", famPoolSeconds); err != nil {
			return err
		}
	}
	if j := s.DialJitter; j != nil {
		if len(s.DialAfter) == 0 {
			return errors.New("machine: pool dial_jitter: without dial_after")
		}
		if !jitterOK(*j) {
			return fmt.Errorf("machine: pool dial_jitter: %v outside [0, 1)", *j)
		}
	}
	for i := range s.Retire {
		r := &s.Retire[i]
		if metrics[r.Metric] == nil {
			return fmt.Errorf("machine: pool retire: unknown metric %q", r.Metric)
		}
		f := famRuleCount
		if r.Metric == MetricAge || r.Metric == MetricIdle {
			f = famRuleSeconds
		}
		if err := r.Above.checkIn("above", f); err != nil {
			return fmt.Errorf("machine: pool retire %s: %w", r.Metric, err)
		}
		if !jitterOK(r.Jitter) {
			return fmt.Errorf("machine: pool retire %s jitter: %v outside [0, 1)", r.Metric, r.Jitter)
		}
	}
	if s.ProbeInterval != nil {
		if err := free(s.ProbeInterval, "probe_interval", famPoolProbe); err != nil {
			return err
		}
	}
	if j := s.ProbeJitter; j != nil {
		if s.ProbeInterval == nil {
			return errors.New("machine: pool probe_jitter: without probe_interval")
		}
		if !jitterOK(*j) {
			return fmt.Errorf("machine: pool probe_jitter: %v outside [0, 1)", *j)
		}
	}
	return nil
}

func scopedPipe(v *Value) bool {
	if v.Scope == ScopePipe {
		return true
	}
	if d := v.Dist; d != nil {
		for i := range d.Of {
			if scopedPipe(&d.Of[i]) {
				return true
			}
		}
	}
	return false
}

func (s *PoolSpec) values(f func(*Value)) {
	var walk func(v *Value)
	walk = func(v *Value) {
		f(v)
		if d := v.Dist; d != nil {
			for i := range d.Of {
				walk(&d.Of[i])
			}
		}
	}
	if s.Count != nil {
		walk(s.Count)
	}
	for i := range s.DialAfter {
		walk(&s.DialAfter[i])
	}
	for i := range s.Retire {
		walk(&s.Retire[i].Above)
	}
	if s.ProbeInterval != nil {
		walk(s.ProbeInterval)
	}
}

// PoolPolicy parses a JSON PoolSpec and returns a zmux.PoolPolicy running
// it, for a PipePool or a CarrierPool. Values are drawn from o's random
// source: scope shared once per policy, scope pipe once per member ID while
// the member is in the view, any other scope at each Decide. o.Limits
// bound the input bytes and the points of each distribution.
func PoolPolicy(b []byte, o Options) (zmux.PoolPolicy, error) {
	if m := o.Limits.MaxBytes; m > 0 && len(b) > m {
		return nil, fmt.Errorf("machine: pool: %d bytes, limit %d", len(b), m)
	}
	s, err := ParsePoolSpec(b)
	if err != nil {
		return nil, err
	}
	return s.Policy(o)
}

// Policy returns a zmux.PoolPolicy running s; see PoolPolicy. s is
// validated and must not be modified afterwards.
func (s *PoolSpec) Policy(o Options) (zmux.PoolPolicy, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if m := o.Limits.MaxPoints; m > 0 {
		var bad bool
		s.values(func(v *Value) {
			if d := v.Dist; d != nil && (len(d.Points) > m || len(d.Of) > m) {
				bad = true
			}
		})
		if bad {
			return nil, errors.New("machine: pool: distribution over limit")
		}
	}
	p := &poolPolicy{s: s, shared: map[*Value]float64{}, members: map[uint64]*poolMember{}}
	p.drawer = drawer{r: newRand(o.source()), keeper: p}
	return p, nil
}

type poolPolicy struct {
	mu sync.Mutex
	s  *PoolSpec
	drawer
	shared  map[*Value]float64
	members map[uint64]*poolMember // by member ID, while in the view
	cur     map[*Value]float64     // scope pipe values of the member being decided; nil: none
	seen    map[uint64]bool
	// The current dial wait (DialJitter): its draw and the view it began
	// from.
	waiting   bool
	waitU     float64
	waitFails int
	waitSince time.Duration // since the last dial; <0: no dial
}

// poolMember is what the policy keeps of a member while it is in the
// view.
type poolMember struct {
	vals map[*Value]float64 // scope pipe values
	u    []float64          // per retire rule: the jitter draw (0 without jitter)
}

func (p *poolPolicy) kept(v *Value) (float64, bool) {
	var x float64
	var ok bool
	switch v.Scope {
	case ScopeShared:
		x, ok = p.shared[v]
	case ScopePipe:
		x, ok = p.cur[v]
	}
	return x, ok
}

func (p *poolPolicy) keep(v *Value, x float64) {
	switch v.Scope {
	case ScopeShared:
		p.shared[v] = x
	case ScopePipe:
		if p.cur != nil {
			p.cur[v] = x
		}
	}
}

// Decide sets d.Count, d.DialAfter, d.Retire, d.ProbeInterval and
// d.ProbeJitter as s states, and d.Recheck to the earliest of the time
// until an age or idle rule of a member not retired would hold, at its
// jittered threshold, and the time until DialAfter after the last dial
// elapses (0: neither); it does not block.
func (p *poolPolicy) Decide(v *zmux.PoolView, d *zmux.PoolDecision) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seen == nil {
		p.seen = map[uint64]bool{}
	}
	clear(p.seen)
	for i := range v.Members {
		p.seen[v.Members[i].ID] = true
	}
	for id := range p.members {
		if !p.seen[id] {
			delete(p.members, id)
		}
	}
	p.cur = nil
	if c := p.s.Count; c != nil {
		// At least 1 (Validate); a draw above 2^53 is passed on as an
		// invalid count, which the pool rejects.
		if x := math.Round(p.sample(c)); x <= maxCount {
			d.Count = int(min(x, math.MaxInt))
		} else {
			d.Count = -1
		}
	}
	if n := len(p.s.DialAfter); n > 0 {
		k := min(max(v.Failures, 0), n-1)
		x := p.sample(&p.s.DialAfter[k])
		if j := p.s.DialJitter; j != nil && *j > 0 {
			x *= 1 - *j*p.dialDraw(v)
		}
		d.DialAfter = seconds(x)
	}
	if pi := p.s.ProbeInterval; pi != nil {
		// A sample below 1 s is not raised: the decision fails Validate
		// and the pool rejects it.
		d.ProbeInterval = seconds(p.sample(pi))
		d.DisableIdleProbe = d.ProbeInterval == 0
		if j := p.s.ProbeJitter; j != nil {
			d.ProbeJitter = *j
			d.NoProbeJitter = *j == 0
		}
	}
	var recheck time.Duration // 0: none yet
	ask := func(d time.Duration) {
		if d > 0 && (recheck == 0 || d < recheck) {
			recheck = d
		}
	}
	if !v.LastDial.IsZero() && d.DialAfter > 0 {
		ask(v.LastDial.Add(d.DialAfter).Sub(v.Now))
	}
	for i := range v.Members {
		m := &v.Members[i]
		if !p.seen[m.ID] || len(p.s.Retire) == 0 { // retired already in this decision
			continue
		}
		pm := p.members[m.ID]
		if pm == nil {
			pm = &poolMember{vals: map[*Value]float64{}, u: make([]float64, len(p.s.Retire))}
			for j := range p.s.Retire {
				if p.s.Retire[j].Jitter > 0 {
					pm.u[j] = p.r.Float64()
				}
			}
			p.members[m.ID] = pm
		}
		p.cur = pm.vals
		var soon time.Duration
		for j := range p.s.Retire {
			r := &p.s.Retire[j]
			x, above := metrics[r.Metric](m), p.sample(&r.Above)
			above *= 1 - r.Jitter*pm.u[j]
			if x > above {
				d.Retire = append(d.Retire, m.ID)
				p.seen[m.ID] = false
				soon = 0
				break
			}
			if r.Metric == MetricAge || r.Metric == MetricIdle {
				// The metric reaches the threshold after this; at the
				// threshold, the smallest wait, so that 0 keeps meaning none.
				if t := max(seconds(above-x), time.Nanosecond); soon == 0 || t < soon {
					soon = t
				}
			}
		}
		ask(soon)
	}
	d.Recheck = recheck
	p.cur = nil
}

// dialDraw returns the draw of the current dial wait, starting a new wait
// when v's failure count differs from the previous Decide's, when the
// time since the last dial got shorter, or when a first dial appeared.
func (p *poolPolicy) dialDraw(v *zmux.PoolView) float64 {
	since := time.Duration(-1)
	if !v.LastDial.IsZero() {
		since = v.Now.Sub(v.LastDial)
	}
	if !p.waiting || v.Failures != p.waitFails || since >= 0 && (p.waitSince < 0 || since < p.waitSince) {
		p.waiting, p.waitU = true, p.r.Float64()
	}
	p.waitFails, p.waitSince = v.Failures, since
	return p.waitU
}
