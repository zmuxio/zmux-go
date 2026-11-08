package machine

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/zmuxio/zmux-go/shaping"
)

// DefaultMaxSlots is the bound on a candidate's counters and timers when
// SizeLimits.MaxSlots is 0.
const DefaultMaxSlots = 4096

// SizeLimits bound a candidate. Zero values: unbounded, except MaxSlots.
type SizeLimits struct {
	MaxBytes       int // JSON bytes of the candidate and the tables it references
	MaxMachines    int
	MaxStates      int // per machine
	MaxActions     int // per state
	MaxTransitions int // per state
	MaxSegments    int // per value or table
	// MaxSlots bounds the counters and timers of all machines together. They
	// are declared by a number and allocated for each pipe, so a short document
	// could otherwise ask for any amount of memory. 0 is DefaultMaxSlots and a
	// negative value is no bound.
	MaxSlots int
}

func (l SizeLimits) maxSlots() int64 {
	switch {
	case l.MaxSlots == 0:
		return DefaultMaxSlots
	case l.MaxSlots < 0:
		return -1
	}
	return int64(l.MaxSlots)
}

// Options configure compilation and what a compiled candidate makes. The
// zero value is valid.
type Options struct {
	// NewSource returns a random source: one for compilation, one per pipe,
	// one per pool policy and one for the handshake shaper, which guards it
	// with a lock. nil: generators seeded from crypto/rand, and the runtime's
	// own generator in the handshake shaper.
	NewSource func() rand.Source
}

func (o *Options) source() rand.Source {
	if o.NewSource != nil {
		return o.NewSource()
	}
	return newSource()
}

// Handshake names, in the order values are drawn when compiling.
var handshakeOrder = [...]struct{ hs, msg string }{
	{"establish", "finish"}, {"establish", "hello"}, {"establish", "reply"},
	{"join", "confirm"}, {"join", "hello"}, {"join", "hello_early"}, {"join", "reply"},
}

func handshakeIndex(hs, msg string) int {
	for i := range handshakeOrder {
		if handshakeOrder[i].hs == hs && handshakeOrder[i].msg == msg {
			return i
		}
	}
	return -1
}

func knownHandshake(hs string) bool {
	for i := range handshakeOrder {
		if handshakeOrder[i].hs == hs {
			return true
		}
	}
	return false
}

// unitValues are the values of one handshake message.
type unitValues struct {
	set                     bool
	padTo, padMul, pad, dly value
}

// Candidate is a validated candidate of a kind: handshakes, machines, limits
// and pool, each possibly absent. It is immutable.
type Candidate struct {
	hs       [len(handshakeOrder)]unitValues
	machines []machineDef
	budget   *budgetDef
	pool     *poolDef
}

// ParseCandidate reads a candidate's JSON. A member named in requires is not
// its concern: the reader of a bundle chooses the candidate. t holds the
// bundle's tables by name, each parsed once, at its first reference.
func ParseCandidate(b []byte, t map[string]json.RawMessage, l SizeLimits) (*Candidate, error) {
	tt := &tables{raw: t, l: l}
	if err := tt.useBytes(len(b)); err != nil {
		return nil, err
	}
	x, err := decode(b)
	if err != nil {
		return nil, err
	}
	o, err := members(x, "candidate", "requires", "handshakes", "machines", "budget", "pool")
	if err != nil {
		return nil, err
	}
	c := &Candidate{}
	if v := o.get("handshakes"); v != nil {
		if err := c.parseHandshakes(v, tt); err != nil {
			return nil, err
		}
	}
	if v := o.get("machines"); v != nil {
		if c.machines, err = parseMachines(v, tt); err != nil {
			return nil, err
		}
	}
	if v := o.get("budget"); v != nil {
		if c.budget, err = parseBudget(v, tt); err != nil {
			return nil, err
		}
	}
	if v := o.get("pool"); v != nil {
		if c.pool, err = parsePool(v, tt); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Candidate) parseHandshakes(x any, t *tables) error {
	o, ok := x.(*jobject)
	if !ok {
		return fail("type", "handshakes")
	}
	for _, hs := range o.keys {
		if !knownHandshake(hs) || o.vals[hs] == nil {
			continue // a handshake not known here is data
		}
		msgs, ok := o.vals[hs].(*jobject)
		if !ok {
			return fail("type", "%s", hs)
		}
		for _, msg := range msgs.keys {
			i := handshakeIndex(hs, msg)
			if i < 0 || msgs.vals[msg] == nil {
				continue
			}
			u, err := members(msgs.vals[msg], "message", "pad_to", "pad_multiple", "pad", "delay")
			if err != nil {
				return err
			}
			e := &c.hs[i]
			e.set = true
			for _, f := range []struct {
				k   string
				to  *value
				fam family
			}{{"pad_to", &e.padTo, famLength}, {"pad_multiple", &e.padMul, famLength}, {"pad", &e.pad, famLength}, {"delay", &e.dly, famTime}} {
				if v := u.get(f.k); v != nil {
					if *f.to, err = parseValue(v, f.fam, t, false, f.k); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// HasMachines reports whether the candidate shapes records.
func (c *Candidate) HasMachines() bool { return len(c.machines) > 0 }

// HasPool reports whether the candidate has a pool policy.
func (c *Candidate) HasPool() bool { return c.pool != nil }

// Compiled is a Candidate with its shared values drawn. It is immutable and
// safe for concurrent use.
type Compiled struct {
	c        *Candidate
	o        Options
	hs       [len(handshakeOrder)]unitValues
	machines *compiledMachines
	pool     *poolDef
	shared   []uint64 // in the order drawn
}

// Compile draws the shared values of c from one source, in a fixed order.
func (c *Candidate) Compile(o Options) *Compiled {
	src := o.source()
	cc := &Compiled{c: c, o: o}
	for i := range c.hs {
		e := c.hs[i]
		e.padTo = e.padTo.resolved(src, &cc.shared)
		e.padMul = e.padMul.resolved(src, &cc.shared)
		e.pad = e.pad.resolved(src, &cc.shared)
		e.dly = e.dly.resolved(src, &cc.shared)
		cc.hs[i] = e
	}
	if len(c.machines) > 0 {
		cc.machines = compileMachines(c.machines, src, &cc.shared, &o)
	}
	if c.pool != nil {
		cc.pool = c.pool.resolved(src, &cc.shared)
	}
	return cc
}

// Budget returns the function for zmux.Config.ShapingBudget, which draws the
// values of scope session at each call; nil without a budget.
func (c *Compiled) Budget() func() shaping.Budget {
	b := c.c.budget
	if b == nil {
		return nil
	}
	var mu sync.Mutex
	src := c.o.source()
	return func() shaping.Budget {
		mu.Lock()
		defer mu.Unlock()
		return b.draw(src)
	}
}

// Handshake returns the shaper of handshake messages. Values of scope each
// are drawn at each call; a message without an entry leaves the plan zero.
func (c *Compiled) Handshake() shaping.HandshakeShaper {
	draw := func(v *value) uint64 { return v.drawTable(runtimeSource{}) }
	if c.o.NewSource != nil {
		var mu sync.Mutex
		src := c.o.NewSource()
		draw = func(v *value) uint64 {
			mu.Lock()
			defer mu.Unlock()
			return v.drawTable(src)
		}
	}
	return func(v *shaping.HandshakeView, p *shaping.UnitPlan) bool {
		i := handshakeIndex(v.Handshake, v.Message)
		if i < 0 || !c.hs[i].set {
			return false
		}
		e := &c.hs[i]
		if e.padTo.set {
			p.PadTo = int(draw(&e.padTo))
		}
		if e.padMul.set {
			p.PadMultiple = int(draw(&e.padMul))
		}
		if e.pad.set {
			p.Pad = int(draw(&e.pad))
		}
		if e.dly.set {
			p.Delay = micros(draw(&e.dly))
			if b := c.c.budget; b != nil && b.maxDelay > 0 {
				p.Delay = min(p.Delay, b.maxDelay)
			}
		}
		return true
	}
}

// runtimeSource is the runtime's per-thread generator; it takes no lock.
type runtimeSource struct{}

func (runtimeSource) Uint64() uint64 { return rand.Uint64() }

// compile-time assertion that the names stay in byte order.
var _ = func() bool {
	for i := 1; i < len(handshakeOrder); i++ {
		a, b := handshakeOrder[i-1], handshakeOrder[i]
		if a.hs > b.hs || a.hs == b.hs && a.msg >= b.msg {
			panic(fmt.Sprintf("handshake order: %v", b))
		}
	}
	return true
}()
