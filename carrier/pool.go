package carrier

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/poolhost"
	"github.com/zmuxio/zmux-go/varint"
)

// PoolLimits bound how a carrier pool uses each underlying connection. The
// zero value selects the defaults.
type PoolLimits struct {
	// Policy, when set, is asked when a pipe is taken or released, when a
	// connection joins, after the decision's Recheck and every 100 ms. It
	// is asked on a goroutine of the pool, one look at a time, started
	// when a look is due and ended when none is pending; Get and Release
	// never wait for it: they use the latest decision, which a look
	// still under way delivers to the calls after it, and the events that
	// come meanwhile coalesce into one more look. Count is the connections to spread pipes over,
	// dialled as pipes are taken (0: a connection is dialled only when a
	// pipe finds none with room), connections above it retire gracefully,
	// the longest idle first, and RetireConnections retires connections by
	// fate group. Its ProbeInterval and ProbeJitter are not used (the
	// pool dials no pipes), but a look whose Decide returns an error or
	// panics, or whose decision fails zmux.PoolDecision.Validate, is
	// rejected as a whole, as by MaintainPipes: the pool goes on as with
	// the zero decision and reports the error to DecisionRejected, else
	// logs it to Logger.
	Policy zmux.PoolPolicy
	// DecisionRejected, when set, receives the error of each look of
	// Policy the pool rejected, outside the pool's lock, on the goroutine
	// that asked the policy; it must not block, and a panic in it is
	// contained (logged to Logger).
	DecisionRejected func(err error)
	// Logger, when set, receives at warning level the rejected looks of
	// Policy when DecisionRejected is unset. A pool belongs to no session:
	// unset, nothing is logged.
	Logger *slog.Logger
	// MaxPipesPerConnection bounds the pipes on a connection at once
	// (0: 8).
	MaxPipesPerConnection int
	// MaxPipesPerConnectionLifetime bounds the pipes a connection carries
	// over its life (0: unlimited).
	MaxPipesPerConnectionLifetime int
	// MaxConnectionAge stops giving pipes to a connection this old (0:
	// unlimited).
	MaxConnectionAge time.Duration
}

// ConnPool shares underlying connections between pipes, for adapters
// that carry several pipes over one connection (HTTP/2 and HTTP/3 request
// streams, QUIC streams). A new pipe goes to the open
// connection with the most room within the limits, else to a new one; a
// connection that reached its lifetime limits takes no new pipe and is
// closed once its last pipe ends. The pipes of one connection get one
// fate group: they fail together and are never counted as independent
// paths. The zero value with Dial set is ready; it is safe for concurrent
// use.
type ConnPool[C comparable] struct {
	// PoolLimits bound the use of each connection; set them before the
	// first Get.
	PoolLimits
	// Dial opens an underlying connection.
	Dial func(ctx context.Context) (C, error)
	// Close closes a connection the pool retired once no pipe uses it.
	Close func(C)

	view     zmux.PoolView     // owned by the decide goroutine: what the policy is asked about
	next     zmux.PoolDecision // owned by the decide goroutine: the decision being asked for
	lastDial time.Time
	recheck  *time.Timer // asks the policy again after its Recheck
	conns    []*pooledConn[C]
	rejected []error // errors of rejected decisions, reported after unlocking
	dec      zmux.PoolDecision
	failures int
	dials    []*poolDial // dials of Get under way, oldest first; the growth dials count as connections against the decision
	mu       sync.Mutex
	closed   bool
	pending  bool      // under mu: an event came that no decision looked at yet
	asking   bool      // under mu: the decide goroutine runs
	ended    sync.Cond // signalled under mu when the decide goroutine ends; L is mu, set by the first wait
}

// poolDial is a dial of Get under way. A growth dial counts in
// PoolView.Dialing and toward the size a decision's Count is compared
// with, and a decision that shrinks the pool cancels it; a dial on demand
// (demand: no connection had room) does neither.
type poolDial struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled bool // under mu: by a decision
	demand    bool
}

// takeMode is what take does when the policy and the connections disagree.
type takeMode uint8

const (
	// takeByPolicy dials first when the policy wants another connection.
	takeByPolicy takeMode = iota
	// takeRoomOnly leases a connection with room, else nothing.
	takeRoomOnly
	// takeOrDemand leases a connection with room, else dials on demand.
	takeOrDemand
)

type pooledConn[C comparable] struct {
	born    time.Time
	idle    time.Time // when its last pipe ended; zero until one did
	c       C
	fate    uint64
	active  int
	total   int
	retired bool
}

// Lease is one pipe's use of a pooled connection; the adapter releases it
// when the pipe ends.
type Lease[C comparable] struct {
	// Conn is the connection to open the pipe on.
	Conn C
	p    *ConnPool[C]
	pc   *pooledConn[C]
	// FateGroup is the connection's fate group (set it as the zmux.PipeIO's).
	FateGroup uint64
	once      sync.Once
}

// Release ends the lease when its pipe ended; the last lease of a retired
// connection closes it. Release is idempotent.
func (l *Lease[C]) Release() { l.once.Do(func() { l.p.release(l.pc) }) }

// Retire stops giving new pipes to the lease's connection (for example
// after it failed); it closes once its pipes are released.
func (l *Lease[C]) Retire() { l.p.retire(l.pc) }

var errPoolClosed = errors.New("zmux: carrier pool closed")

// Get returns a lease on a connection with room, dialling one when none
// has; the dial runs outside the pool's lock. The lease is non-nil exactly
// when the error is nil.
//
// A dial Get makes because no connection has room is on demand: the pool
// never counts it in PoolView.Dialing or in the size a decision's Count is
// compared with, and never cancels it. A dial made because the policy
// wants more connections than there are, while one has room, is a growth
// dial: a decision that shrinks the pool cancels it, and Get then takes a
// connection with room, or dials on demand.
func (p *ConnPool[C]) Get(ctx context.Context) (*Lease[C], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mode := takeByPolicy
	for {
		l, pd, err := p.take(ctx, mode)
		if err != nil || l != nil {
			return l, err
		}
		l, shrunk, err := p.dialFor(ctx, pd)
		if !shrunk {
			return l, err
		}
		mode = takeOrDemand
	}
}

// dialFor dials for the caller of Get under pd, which take counted with
// its time (lastDial): calls that find the pool short meanwhile do not dial
// past the decision. shrunk reports a growth dial that a decision
// cancelled, with no lease and no error.
func (p *ConnPool[C]) dialFor(ctx context.Context, pd *poolDial) (l *Lease[C], shrunk bool, err error) {
	defer pd.cancel()
	dialed := false
	defer func() {
		if !dialed {
			p.mu.Lock()
			p.dropDial(pd)
			p.unlockReport() // the dial ended without a connection: look again
		}
	}()
	// An expired-connection Close callback may have stopped this pool or context.
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, false, errPoolClosed
	}
	c, err := p.Dial(pd.ctx)
	p.mu.Lock()
	shrunk = pd.cancelled
	switch {
	case shrunk:
	case err != nil:
		p.failures++
	default:
		p.failures = 0
	}
	p.mu.Unlock()
	if shrunk {
		// A decision gave this dial up: the caller takes a connection with
		// room or dials again, and one dialled all the same is closed.
		if err == nil && p.Close != nil {
			p.Close(c)
		}
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, true, nil
	}
	if err != nil {
		// A connection with room still serves (a growth dial was the
		// policy's wish for another): the failure costs the caller nothing
		// then, unless the caller gave up, as when the dial succeeds.
		if ctx.Err() != nil {
			return nil, false, err
		}
		if l, _, terr := p.take(ctx, takeRoomOnly); terr == nil && l != nil {
			return l, false, nil
		}
		return nil, false, err
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	pc := &pooledConn[C]{c: c, fate: binary.BigEndian.Uint64(b[:])&varint.MaxVarint | 1, born: time.Now(), active: 1, total: 1}
	cancelled := ctx.Err()
	p.mu.Lock()
	if p.closed || cancelled != nil || pd.cancelled {
		stale := pd.cancelled && !p.closed && cancelled == nil
		if p.closed {
			cancelled = errPoolClosed
		}
		p.mu.Unlock()
		if p.Close != nil {
			p.Close(c)
		}
		return nil, stale, cancelled
	}
	p.conns = append(p.conns, pc)
	p.dropDial(pd)
	dialed = true
	p.mu.Unlock()
	p.lockConsulted()
	p.retireLocked(pc)
	p.unlockReport() // a member joined: ask the policy
	return &Lease[C]{Conn: c, FateGroup: pc.fate, p: p, pc: pc}, false, nil
}

// dropDial forgets a dial that ended; under mu.
func (p *ConnPool[C]) dropDial(pd *poolDial) {
	if i := slices.Index(p.dials, pd); i >= 0 {
		p.dials = slices.Delete(p.dials, i, i+1)
	}
}

func (p *ConnPool[C]) maxActive() int {
	if p.MaxPipesPerConnection > 0 {
		return p.MaxPipesPerConnection
	}
	return 8
}

// take leases the open connection with the most room, if any, unless the
// policy asks for a new connection first (takeByPolicy); a nil lease with
// a dial counts the dial its caller makes (dials, lastDial), under a
// context of ctx that a decision shrinking the pool cancels when the dial
// is a growth dial.
func (p *ConnPool[C]) take(ctx context.Context, mode takeMode) (*Lease[C], *poolDial, error) {
	dial := p.lockConsulted()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, errPoolClosed
	}
	var best *pooledConn[C]
	var idle []C
	live := p.conns[:0]
	for _, pc := range p.conns {
		p.retireLocked(pc)
		if pc.retired && pc.active == 0 {
			idle = append(idle, pc.c)
			continue
		}
		live = append(live, pc)
		if pc.retired || pc.active >= p.maxActive() {
			continue
		}
		if best == nil || p.room(pc) > p.room(best) {
			best = pc
		}
	}
	clear(p.conns[len(live):])
	p.conns = live
	var lease *Lease[C]
	var pd *poolDial
	if best != nil && (!dial || mode != takeByPolicy) {
		best.active++
		best.total++
		p.retireLocked(best)
		lease = &Lease[C]{Conn: best.c, FateGroup: best.fate, p: p, pc: best}
	} else if mode != takeRoomOnly {
		pd = &poolDial{demand: best == nil}
		pd.ctx, pd.cancel = context.WithCancel(ctx)
		p.dials = append(p.dials, pd)
		p.lastDial = time.Now()
	}
	p.unlockReport()
	if p.Close != nil {
		for _, c := range idle {
			p.Close(c)
		}
	}
	return lease, pd, nil
}

func (p *ConnPool[C]) room(pc *pooledConn[C]) int {
	r := p.maxActive() - pc.active
	if m := p.MaxPipesPerConnectionLifetime; m > 0 {
		r = min(r, m-pc.total)
	}
	return r
}

// retireLocked retires a connection that reached a lifetime limit.
func (p *ConnPool[C]) retireLocked(pc *pooledConn[C]) {
	if m := p.MaxPipesPerConnectionLifetime; m > 0 && pc.total >= m {
		pc.retired = true
	}
	if a := p.MaxConnectionAge; a > 0 && time.Since(pc.born) >= a {
		pc.retired = true
	}
}

func (p *ConnPool[C]) release(pc *pooledConn[C]) {
	p.lockConsulted()
	pc.active--
	if pc.active == 0 {
		pc.idle = time.Now()
	}
	p.retireLocked(pc)
	var idle []C
	for _, x := range slices.Backward(p.conns) {
		if (x.retired || p.closed) && x.active == 0 && p.drop(x) {
			idle = append(idle, x.c)
		}
	}
	p.unlockReport()
	if p.Close != nil {
		for _, c := range idle {
			p.Close(c)
		}
	}
}

// RetireConn retires connection c (for example when it failed): it takes
// no new pipe and closes once its pipes are released.
func (p *ConnPool[C]) RetireConn(c C) {
	p.mu.Lock()
	var pc *pooledConn[C]
	for _, x := range p.conns {
		if x.c == c {
			pc = x
		}
	}
	p.mu.Unlock()
	if pc != nil {
		p.retire(pc)
	}
}

func (p *ConnPool[C]) retire(pc *pooledConn[C]) {
	p.mu.Lock()
	pc.retired = true
	done := pc.active == 0
	if done {
		done = p.drop(pc)
	}
	p.mu.Unlock()
	if done && p.Close != nil {
		p.Close(pc.c)
	}
}

func (p *ConnPool[C]) drop(pc *pooledConn[C]) bool {
	for i, x := range p.conns {
		if x == pc {
			copy(p.conns[i:], p.conns[i+1:])
			p.conns[len(p.conns)-1] = nil
			p.conns = p.conns[:len(p.conns)-1]
			return true
		}
	}
	return false
}

// Shutdown retires every connection: idle ones close now, the others when
// their last pipe ends; Get fails afterwards. It also waits for a look
// of Policy under way, so that neither Policy.Decide nor DecisionRejected
// is called after it returns; a pool must be shut down when it is no
// longer used, or its connections and the recheck timer outlive it.
// Shutdown must not be called from Policy.Decide, DecisionRejected, or a
// Close the pool calls for a connection a decision retired, which it
// would wait for: start it on its own goroutine there.
func (p *ConnPool[C]) Shutdown() {
	p.mu.Lock()
	p.closed = true
	if p.recheck != nil {
		p.recheck.Stop()
	}
	p.awaitIdleLocked()
	var idle []C
	for _, pc := range p.conns {
		pc.retired = true
		if pc.active == 0 {
			idle = append(idle, pc.c)
		}
	}
	p.conns = keepActive(p.conns)
	p.mu.Unlock()
	if p.Close != nil {
		for _, c := range idle {
			p.Close(c)
		}
	}
}

func keepActive[C comparable](cs []*pooledConn[C]) []*pooledConn[C] {
	out := cs[:0]
	for _, pc := range cs {
		if pc.active > 0 {
			out = append(out, pc)
		}
	}
	clear(cs[len(out):])
	return out
}

// recheckNow asks the policy again when nothing else did within the last
// decision's Recheck; the decide goroutine applies the answer.
func (p *ConnPool[C]) recheckNow() {
	p.lockConsulted()
	p.unlockReport()
}

// lockConsulted locks the pool and reports whether the latest decision
// asks for a new connection now. It never asks the policy: the callers ask
// by unlocking with unlockReport, after their change to the pool, and the
// decide goroutine answers meanwhile, so a slow policy delays no Get or
// Release and a decision reaches the calls after it.
func (p *ConnPool[C]) lockConsulted() bool {
	p.mu.Lock()
	return !p.closed && p.Policy != nil && p.dueLocked(time.Now(), p.openLocked())
}

// awaitIdleLocked waits until the decide goroutine has ended; under mu,
// which the wait releases meanwhile.
func (p *ConnPool[C]) awaitIdleLocked() {
	p.ended.L = &p.mu
	for p.asking {
		p.ended.Wait()
	}
}

// askSoonLocked asks the policy for another decision on the decide
// goroutine, started when none runs; events that come while a decision is
// under way coalesce into one more look. Under mu.
func (p *ConnPool[C]) askSoonLocked() {
	if p.Policy == nil || p.closed {
		return
	}
	p.pending = true
	if !p.asking {
		p.asking = true
		go p.decideLoop()
	}
}

// decideLoop asks the policy while events are pending, one decision at a
// time and outside mu (the policy is host code and may call back into the
// pool), applies each decision, closes the idle connections it retired
// and ends when nothing is pending, so that an idle pool holds no
// goroutine.
func (p *ConnPool[C]) decideLoop() {
	for {
		p.mu.Lock()
		if !p.pending || p.closed {
			p.asking = false
			p.ended.Broadcast()
			p.mu.Unlock()
			return
		}
		p.askLocked()
		var idle []C
		for _, x := range slices.Backward(p.conns) {
			p.retireLocked(x)
			if x.retired && x.active == 0 && p.drop(x) {
				idle = append(idle, x.c)
			}
		}
		errs := p.rejected
		p.rejected = nil
		f, l := p.DecisionRejected, p.Logger
		p.mu.Unlock()
		for _, err := range errs {
			poolhost.ReportRejected(l, f, err)
		}
		if p.Close != nil {
			for _, c := range idle {
				p.Close(c)
			}
		}
	}
}

// askLocked asks the policy, outside mu, and applies its decision; on the
// decide goroutine, under mu.
func (p *ConnPool[C]) askLocked() time.Time {
	p.pending = false
	now := time.Now()
	if p.closed {
		return now
	}
	v := &p.view
	v.Failures, v.Members, v.Dialing = p.failures, v.Members[:0], p.growthDialsLocked()
	v.SinceLastDial = -1
	if !p.lastDial.IsZero() {
		v.SinceLastDial = now.Sub(p.lastDial)
	}
	for _, pc := range p.conns {
		// A retired connection is reported, not usable, until it closes.
		p.retireLocked(pc)
		m := zmux.PoolMember{FateGroup: pc.fate, Age: now.Sub(pc.born), Pipes: pc.active,
			Usable: !pc.retired && pc.active < p.maxActive() && p.room(pc) > 0}
		m.Idle = p.idleLocked(pc, now)
		v.Members = append(v.Members, m)
	}
	p.mu.Unlock()
	err := p.next.Ask(p.Policy, v)
	p.mu.Lock()
	if err != nil {
		p.next = zmux.PoolDecision{RetirePipes: p.next.RetirePipes[:0], RetireConnections: p.next.RetireConnections[:0]} // rejected, as by MaintainPipes
		if p.DecisionRejected != nil || p.Logger != nil {
			p.rejected = append(p.rejected, err)
		}
	}
	p.dec, p.next = p.next, p.dec
	if r := p.dec.RecheckAfter(); !p.closed {
		if p.recheck == nil {
			p.recheck = time.AfterFunc(r, p.recheckNow)
		} else {
			p.recheck.Reset(r)
		}
	}
	for _, id := range p.dec.RetireConnections {
		for _, pc := range p.conns {
			if pc.fate == id && !pc.retired {
				pc.retired = true
			}
		}
	}
	p.trimLocked(now)
	return now
}

// trimLocked shrinks the pool to the decision's Count, the connections
// not retired and the growth dials under way together: it cancels the
// growth dials first, the most recently started first, then retires connections, the
// longest idle first (one carrying pipes is idle 0), ties broken by the
// fewest pipes, the least age, then the highest fate group. A retired
// connection takes no new pipe, no longer counts and closes once its last
// pipe ends. The connections RetireConnections names are retired already.
func (p *ConnPool[C]) trimLocked(now time.Time) {
	if p.dec.Count <= 0 {
		return
	}
	var live []*pooledConn[C]
	var ms []poolhost.Member
	for _, pc := range p.conns {
		if !pc.retired {
			live = append(live, pc)
			ms = append(ms, poolhost.Member{Key: pc.fate, Idle: p.idleLocked(pc, now), Pipes: pc.active, Age: now.Sub(pc.born)})
		}
	}
	cancel, retire := poolhost.Plan(p.growthDialsLocked(), ms, p.dec.Count)
	for i := len(p.dials) - 1; i >= 0 && cancel > 0; i-- {
		if pd := p.dials[i]; !pd.demand {
			p.dials = slices.Delete(p.dials, i, i+1)
			pd.cancelled = true
			pd.cancel()
			cancel--
		}
	}
	for _, i := range retire {
		live[i].retired = true
	}
}

// idleLocked is PoolMember.Idle of a connection.
func (p *ConnPool[C]) idleLocked(pc *pooledConn[C], now time.Time) time.Duration {
	if pc.active > 0 {
		return 0
	}
	since := pc.idle
	if since.IsZero() {
		since = pc.born
	}
	return now.Sub(since)
}

// growthDialsLocked counts the growth dials under way.
func (p *ConnPool[C]) growthDialsLocked() int {
	n := 0
	for _, pd := range p.dials {
		if !pd.demand {
			n++
		}
	}
	return n
}

// openLocked counts the connections not retired, and the growth dials
// under way; a dial on demand is not counted.
func (p *ConnPool[C]) openLocked() int {
	open := p.growthDialsLocked()
	for _, pc := range p.conns {
		if !pc.retired {
			open++
		}
	}
	return open
}

// dueLocked reports whether the decision asks for another connection now.
func (p *ConnPool[C]) dueLocked(now time.Time, open int) bool {
	return open < p.dec.Count && (p.lastDial.IsZero() || now.Sub(p.lastDial) >= p.dec.DialAfter)
}

// unlockReport asks the policy for a decision on the change just made,
// unlocks the pool and reports the decisions rejected under the lock to
// DecisionRejected, else to Logger.
func (p *ConnPool[C]) unlockReport() {
	p.askSoonLocked()
	errs := p.rejected
	p.rejected = nil
	f, l := p.DecisionRejected, p.Logger
	p.mu.Unlock()
	for _, err := range errs {
		poolhost.ReportRejected(l, f, err)
	}
}
