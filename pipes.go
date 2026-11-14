package zmux

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// PipeOptions are the options of an attachment. The zero value is a
// bidirectional pipe that follows the peer's priority requests and the
// session's defaults.
//
// The fields of PipeUpdate (SendPriority, ReceivePriority, SendStandby,
// ReceiveStandby, Weight, Probation, Liveness, QualityBand) and RecordMode
// are Change values of the same types as in PipeUpdate: at AddPipe the zero
// Change and Clear both mean the default and Set gives a value, the zero
// value included, as in SendStandby: Set(true) or Weight: Set(uint64(0)).
// A set Send field wins over the peer's request (PIPE_STATUS), which can
// change at any time; the Receive fields are always sent: the Hello
// carries a request for the peer's outbound direction.
type PipeOptions struct {
	// Label names the pipe in statistics.
	Label string
	// Liveness are the pipe's own liveness settings; a zero field follows
	// the session's (Config.Liveness). Default: the session's.
	Liveness Change[PipeLiveness]
	// QualityBand is the pipe's own quality band for EventPipeQualityChanged
	// (its outbound direction). Default: the session's (Conn.SetQualityBand).
	// A receive-only pipe has none: setting it is an ErrInvalidConfig error.
	QualityBand Change[QualityBand]
	// SendPriority is the tier of the local outbound direction (lower is
	// preferred). Default: the peer's request, else tier 0.
	SendPriority Change[uint64]
	// ReceivePriority is the tier requested for the peer's outbound
	// direction. Default: tier 0.
	ReceivePriority Change[uint64]
	// Weight is the share for SchedulerSpreadRoundRobin and the factor of
	// SchedulerSpreadRateWeighted. Default 1; 0 excludes the pipe unless
	// every candidate has 0.
	Weight Change[uint64]
	// Probation is the base of the pipe's probation. Default (and Set(0)):
	// the session's (SchedulerParams.Probation); a negative value turns
	// the pipe's probation off.
	Probation Change[time.Duration]
	// FateGroup groups pipes that share a path; a pipe that replaces a
	// failed one on the same path reuses its fate group. 0: the carrier's
	// PipeIO.FateGroup, else none (an independent path).
	FateGroup uint64
	// KeyGroup is the key-exchange group of the Join's key share. Zero:
	// group 1 when it is usable, otherwise group 2. A group that is not
	// usable in the session fails AddPipe with ErrInvalidConfig.
	KeyGroup uint64
	// SendStandby makes the local outbound direction carry data only on
	// failover. Default: the peer's request, else active.
	SendStandby Change[bool]
	// ReceiveStandby asks the peer to make its outbound direction standby.
	// Default: active.
	ReceiveStandby Change[bool]
	// RecordMode is the record mode of this carrier. Default:
	// Config.RecordPolicy, else RecordSealed.
	RecordMode Change[RecordMode]
	// Direction from the local endpoint: DirectionBidirectional (the zero
	// value), DirectionSendOnly (an uplink) or DirectionReceiveOnly (a downlink).
	Direction Direction
	// ReturnOnReply makes AddPipe return at the verified Reply instead of
	// once the peer has attached the pipe. The caller then keeps the
	// session's other pipes until Pipe.WaitAttached returns.
	ReturnOnReply bool
}

func (o *PipeOptions) join(dir Direction) (core.JoinOptions, error) {
	var j core.JoinOptions
	j.Dir = 2 // DirectionBidirectional, and any value out of range
	switch dir {
	case DirectionSendOnly:
		j.Dir = 0
	case DirectionReceiveOnly:
		j.Dir = 1
	case DirectionBidirectional:
	}
	live, _ := o.Liveness.get()
	var err error
	if j.Live, err = live.core(); err != nil {
		return j, err
	}
	if _, ok := o.QualityBand.get(); ok {
		if dir == DirectionReceiveOnly {
			return j, fmt.Errorf("%w: QualityBand on a receive-only pipe", ErrInvalidConfig)
		}
		if _, err = o.QualityBand.v.core(); err != nil {
			return j, err
		}
	}
	j.FateGroup, j.Group = o.FateGroup, o.KeyGroup
	if v, ok := o.SendPriority.get(); ok {
		j.SendPriority = &v
	}
	if v, ok := o.SendStandby.get(); ok {
		j.SendStandby = &v
	}
	j.RecvPriority, _ = o.ReceivePriority.get()
	j.RecvStandby, _ = o.ReceiveStandby.get()
	j.Weight = 1
	if v, ok := o.Weight.get(); ok {
		j.Weight = v
	}
	if v, ok := o.Probation.get(); ok && v != 0 {
		d := core.Duration(max(v, 0)) // 0: off
		j.Probation = &d
	}
	return j, nil
}

// PipeID identifies a pipe for the life of its session; the bootstrap pipe
// is 0. It is an unsigned 62-bit value.
type PipeID uint64

// Pipe is one attachment of a session. Its methods are safe for concurrent
// use; they fail with ErrPipeGone once the attachment is retired.
type Pipe struct {
	c   *Conn
	end *pipeEnd // never nil
	id  uint64
}

// ID returns the pipe ID.
func (p *Pipe) ID() PipeID { return PipeID(p.id) }

// admittedValue is the Decision.Value of the admission that accepted the
// pipe's carrier (AdmittedAs).
func (p *Pipe) admittedValue() any {
	if p.id == 0 {
		return p.c.admitted
	}
	p.c.lock()
	defer p.c.unlock()
	if pd := p.c.pipes[p.id]; pd != nil {
		return pd.admitted
	}
	return nil
}

// AddPipe joins a new carrier to the session with the Join handshake and returns
// once the peer has attached the pipe (Pipe.WaitAttached), or with
// PipeOptions.ReturnOnReply once the Reply has been verified. ctx bounds the call,
// and otherwise Config.JoinTimeout. The carrier belongs to zmux from the call on:
// when AddPipe fails, ErrInvalidConfig included, it has been closed. A refusal returns ErrJoinRefused and starts the
// backoff. A join that the attachment cap or the refusal backoff holds waits for room.
// A pipe that the peer has not attached when ctx or the timeout ends is aborted, and
// ctx's error returned; one that fails or goes first returns ErrPipeGone. A send-only
// pipe is attached by an acknowledgement, which the peer sends on an inbound
// direction: without another pipe that receives, the default wait lasts until the
// deadline. In Bound, a carrier whose handshake (a *tls.Conn half, or a Handshaker)
// has not completed is first driven through it with ctx; a failed handshake closes it.
// On a session that returned before establishment (EarlyData), AddPipe first waits for it.
func (c *Conn) AddPipe(ctx context.Context, p PipeIO, o PipeOptions) (*Pipe, error) {
	pp, taken, err := c.addPipeCall(ctx, p, o)
	if err != nil && !taken {
		closePipeIO(p)
	}
	return pp, err
}

// addPipeCall is AddPipe; taken reports that the session took p (and
// closes it when it fails).
func (c *Conn) addPipeCall(ctx context.Context, p PipeIO, o PipeOptions) (pp *Pipe, taken bool, err error) {
	dir := o.Direction
	if p.W == nil || p.R == nil {
		return nil, false, fmt.Errorf("%w: a join needs both directions of the carrier; pair two with PairedPipe", ErrInvalidConfig)
	}
	if err := p.checkOverhead(); err != nil {
		return nil, false, err
	}
	if o.FateGroup == 0 {
		o.FateGroup = p.FateGroup
	}
	j, err := o.join(dir)
	if err != nil {
		return nil, false, err
	}
	mode := RecordSealed
	rm, rmSet := o.RecordMode.get()
	switch {
	case rmSet:
		mode = rm
	case c.cfg.RecordPolicy != nil:
		mode = c.cfg.RecordPolicy(joinCarrierInfo(p, false))
	}
	if mode > RecordTrusted {
		return nil, false, fmt.Errorf("%w: record mode %v for a join", ErrInvalidConfig, mode)
	}
	j.Mode = join.Mode(mode)
	if mode == RecordBound {
		if p.Carrier == CarrierSplit {
			return nil, false, fmt.Errorf("%w: Bound on a split carrier", ErrInvalidConfig)
		}
		// The handshake first: a carrier may name its connection only then.
		if handshake := pendingHandshake(p); handshake != nil {
			if err := handshake(ctx); err != nil {
				return nil, false, fmt.Errorf("zmux: carrier handshake: %w", err)
			}
		}
		if !IsCarrierSecure(p) {
			return nil, false, fmt.Errorf("%w: Bound on a carrier without a Secure declaration", ErrInvalidConfig)
		}
		if j.Binding, err = joinBinding(p); err != nil {
			return nil, false, err
		}
	}
	// A session that returned early (EarlyData) learns from the peer's
	// parameters whether it has multipath at all.
	if err := c.waitEstablished(ctx); err != nil {
		return nil, false, err
	}
	pp, pd, err := c.addPipe(p, o, j)
	if err != nil {
		return nil, false, err
	}
	if b, ok := o.QualityBand.get(); ok {
		cb, _ := b.core() // checked by join
		c.lock()
		_ = c.s.SetQualityBand(int64(pp.id), &cb) // join refused it on a receive-only pipe
		c.unlock()
	}
	jctx, cancel := timeshim.WithTimeout(ctx, c.clk.source(), c.cfg.joinTimeout())
	defer cancel()
	if err := c.joinCarrier(jctx, pd, p, dir); err != nil {
		if !errors.Is(err, ErrJoinRefused) {
			_ = pp.Abort(err)
		}
		return nil, true, err
	}
	if o.ReturnOnReply {
		return pp, true, nil
	}
	if err := pp.WaitAttached(jctx); err != nil {
		_ = pp.Abort(err)
		if jctx.Err() != nil && c.ctx.Err() == nil {
			return nil, true, fmt.Errorf("zmux: pipe %d not attached by the peer: %w", pp.id, err)
		}
		return nil, true, err
	}
	return pp, true, nil
}

// WaitAttached waits until the peer has attached the pipe, returning nil at once for a pipe it
// already has: the bootstrap pipe, a pipe the peer joined, or one this
// endpoint joined that has received a PIPE_ACK acknowledging a byte (one
// with only an inbound direction: a record). AddPipe waits for it unless
// PipeOptions.ReturnOnReply is set; until then the peer may have no state
// of the pipe, and closing the session's other pipes would leave it none. It returns
// ctx's error, ErrPipeGone once the pipe failed, closed or is gone before
// it is attached, or the session's error once the session ends.
func (p *Pipe) WaitAttached(ctx context.Context) error {
	c := p.c
	sig := core.NewSignal()
	defer c.unwatchPipes(sig)
	for {
		c.lock()
		sig.Drain()
		c.watchPipes(sig)
		pi, ok := c.s.PipeInfo(p.id)
		c.unlock()
		switch {
		case c.isDone():
			return c.closedErr()
		case !ok:
			return ErrPipeGone
		case pi.PeerAttached:
			return nil
		case pi.HasOut && (pi.OutState == core.OutFailed || pi.OutState == core.OutClosed || pi.OutState == core.OutDraining),
			!pi.HasOut && pi.InState == core.InEnded:
			return ErrPipeGone
		}
		select {
		case <-sig.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
		}
	}
}

func (c *Conn) addPipe(p PipeIO, o PipeOptions, j core.JoinOptions) (*Pipe, *pipeDrv, error) {
	c.lock()
	defer c.unlock()
	if c.closing {
		return nil, nil, c.errLocked()
	}
	outer := outerOf(p)
	if err := nestPipe(c, outer); err != nil {
		return nil, nil, err
	}
	pd := c.newPipeDrv(c.s.NextPipeID(), p)
	pd.outer = outer
	low := pd.tcp.setLowWater(multiLowWater)
	info := carrierInfo(p, pd.tcp, low)
	if j.Mode == join.Sealed {
		overhead := p.WriteOverhead + 16
		if p.Framing != FramingRecords {
			overhead += sealedLen
		}
		info = core.CarrierInfo{Framing: core.FramingRecords, MinRTT: info.MinRTT, QueueBound: low,
			WriteOverhead: overhead, MaxRecord: SealedMaxRecord}
		pd.opener, pd.ch = nil, nil
	}
	j.In, j.Out, j.OutSig, j.Kind = info, info, pd.wsig, uint8(p.Carrier)
	id, _, err := c.s.AddPipe(j)
	if err != nil {
		unnestPipe(c, pd.outer)
		return nil, nil, apiErr(err)
	}
	pd.id = id
	c.pipes[id] = pd
	c.labels[id] = o.Label
	return &Pipe{c: c, id: id, end: pd.end}, pd, nil
}

// JoinReady reports whether the maintenance target and join admission permit
// dialing another pipe now: for a pool or an own redial loop, which dials
// only when it may. Explicit AddPipe still follows the physical cap.
func (a *ConnAdapter) JoinReady() bool { return (*Conn)(a).joinReady() }

func (c *Conn) joinReady() bool {
	if c == nil {
		return false
	}
	c.lock()
	defer c.unlock()
	return c.s.JoinReady()
}

// ---- settings ----

// Change is one field of a PipeUpdate, in one of three states: the zero
// value keeps the setting, Set(v) sets it to v and Clear returns it to its
// default.
type Change[T any] struct {
	v     T
	state uint8 // 0 keep, 1 set, 2 clear
}

// get returns the value of a Set change.
func (c Change[T]) get() (T, bool) { return c.v, c.state == 1 }

// Set returns a Change that sets a setting to v.
func Set[T any](v T) Change[T] { return Change[T]{v: v, state: 1} }

// Clear returns a Change that returns a setting to its default.
func Clear[T any]() Change[T] { return Change[T]{state: 2} }

// PipeUpdate changes a live pipe's runtime settings in one call
// (Pipe.Update). Every field is a Change; a field left at its zero value
// keeps its setting. Set and Clear mean, per field:
//
//   - SendPriority, SendStandby (the local outbound direction): set wins
//     over the peer's request; clear follows the peer's latest PIPE_STATUS
//     request, else the default (tier 0, active). Nothing is sent.
//   - ReceivePriority, ReceiveStandby (the request for the peer's outbound
//     direction): set requests the value, clear the default (tier 0,
//     active); a PIPE_STATUS is sent when the request differs from the one
//     in force, at once while the pipe is ACTIVE and when it becomes
//     ACTIVE otherwise. One call that changes both sends one PIPE_STATUS.
//   - Weight: set to v (0 excludes the pipe); clear is 1.
//   - Probation: set to v in the forms of PipeOptions.Probation (negative:
//     off; zero: the session's); clear follows the session's.
//   - Liveness: set replaces the pipe's own PipeLiveness as a whole; clear
//     follows the session's. A pipe already suspect keeps the SuspectFloor
//     and DeadTimeout it turned suspect under.
//   - QualityBand: set replaces the pipe's own band; clear follows the
//     session's (Conn.SetQualityBand). Nothing is sent. A pipe without an
//     outbound direction refuses either with ErrInvalidConfig.
//
// The fields fixed when the pipe is added (Direction, RecordMode, KeyGroup,
// Label, FateGroup, ReturnOnReply) are not in PipeUpdate.
type PipeUpdate struct {
	SendPriority    Change[uint64]
	ReceivePriority Change[uint64]
	SendStandby     Change[bool]
	ReceiveStandby  Change[bool]
	Weight          Change[uint64]
	Probation       Change[time.Duration]
	Liveness        Change[PipeLiveness]
	QualityBand     Change[QualityBand]
}

// Update applies u to the pipe, all its fields together, from the next
// scheduling decision. An invalid value is an ErrInvalidConfig error that
// changes nothing; a pipe that is gone fails with ErrPipeGone. The settings
// in force are in the pipe statistics.
func (p *Pipe) Update(u PipeUpdate) error {
	var o core.PipeSettings
	switch u.SendPriority.state {
	case 1:
		o.SendPriority = &u.SendPriority.v
	case 2:
		o.ClearSendPriority = true
	}
	switch u.SendStandby.state {
	case 1:
		o.SendStandby = &u.SendStandby.v
	case 2:
		o.ClearSendStandby = true
	}
	var zp uint64
	var zs bool
	switch u.ReceivePriority.state {
	case 1:
		o.RecvPriority = &u.ReceivePriority.v
	case 2:
		o.RecvPriority = &zp
	}
	switch u.ReceiveStandby.state {
	case 1:
		o.RecvStandby = &u.ReceiveStandby.v
	case 2:
		o.RecvStandby = &zs
	}
	one := uint64(1)
	switch u.Weight.state {
	case 1:
		o.Weight = &u.Weight.v
	case 2:
		o.Weight = &one
	}
	var prob core.Duration
	switch u.Probation.state {
	case 1:
		switch d := u.Probation.v; {
		case d < 0:
			prob = 0 // off
		case d == 0:
			prob = -1 // the session's
		default:
			prob = core.Duration(d)
		}
		o.Probation = &prob
	case 2:
		prob = -1
		o.Probation = &prob
	}
	var live core.PipeLiveness
	switch u.Liveness.state {
	case 1:
		var err error
		if live, err = u.Liveness.v.core(); err != nil {
			return err
		}
		o.Liveness = &live
	case 2:
		o.Liveness = &live
	}
	var band *core.QualityBand
	if u.QualityBand.state == 1 {
		cb, err := u.QualityBand.v.core()
		if err != nil {
			return err
		}
		band = &cb
	}
	other := u.SendPriority.state|u.SendStandby.state|u.ReceivePriority.state|u.ReceiveStandby.state|
		u.Weight.state|u.Probation.state|u.Liveness.state != 0
	c := p.c
	c.lock()
	defer c.unlock()
	if u.QualityBand.state != 0 {
		// Checked before anything applies: a band needs an outbound direction.
		hasOut, ok := c.s.PipeHasOut(p.id)
		if !ok {
			return ErrPipeGone
		}
		if !hasOut {
			return fmt.Errorf("%w: QualityBand on a pipe without an outbound direction", ErrInvalidConfig)
		}
	}
	if other {
		if err := c.s.SetPipe(p.id, o); err != nil {
			if errors.Is(err, core.ErrInvalid) {
				return ErrPipeGone
			}
			return apiErr(err)
		}
	}
	if u.QualityBand.state != 0 {
		return pipeErr(c.s.SetQualityBand(int64(p.id), band))
	}
	return nil
}

// Done returns a channel that is closed once the pipe has been removed and
// its carrier's work in the library has ended, or the session has ended
// (Wait then returns the session's error). It is closed at once for a pipe
// that is gone. A select on it bounds the wait with a timer or a context.
func (p *Pipe) Done() <-chan struct{} {
	e, c := p.end, p.c
	if e == goneEnd {
		return closedChan
	}
	e.once.Do(func() {
		e.waited = make(chan struct{})
		labels.Go(e.drv.Load().lctx, "done", func() {
			defer close(e.waited)
			select {
			case <-e.retired:
				<-e.drv.Load().gs.wait()
			case <-c.done:
			}
		})
	})
	return e.waited
}

// Wait blocks until Done is closed and returns how the pipe ended: nil for a
// graceful removal (Close, or the peer's) and for a pipe that is gone, the
// cause for a failed or aborted one (ErrPipeGone without a known cause),
// and the session's error when the session ended first. Wait does not
// remove the pipe: it waits for Close, Abort or the pipe's own end.
func (p *Pipe) Wait() error {
	e, c := p.end, p.c
	<-p.Done()
	if e == goneEnd {
		return nil
	}
	select {
	case <-e.retired:
	default:
		return c.closedErr()
	}
	c.lock()
	defer c.unlock()
	return e.err
}

// Revalidate sends a PIPE_PING at once; the pipe takes no new stream data
// until it is acknowledged, while another pipe is usable.
func (p *Pipe) Revalidate() error {
	c := p.c
	c.lock()
	defer c.unlock()
	return pipeErr(c.s.RevalidatePipes(p.id, false, c.clk.now()))
}

// RevalidatePipes revalidates every live outbound pipe (after a network
// change).
func (c *Conn) RevalidatePipes() error {
	c.lock()
	defer c.unlock()
	return apiErr(c.s.RevalidatePipes(0, true, c.clk.now()))
}

// Close removes the pipe gracefully: PIPE_ABANDON(NO_ERROR) and drain.
// It does not block; the pipe's end is reported by
// NextEvent.
func (p *Pipe) Close() error {
	c := p.c
	c.lock()
	defer c.unlock()
	c.warnLastPipeLocked(p.id)
	return pipeErr(c.s.RemovePipe(p.id, true))
}

// warnLastPipeLocked logs a warning when the application removes pipe id,
// the last one of a direction, while the session has open streams: the
// session suspends (resume) or ends with PipesLost.
func (c *Conn) warnLastPipeLocked(id uint64) {
	if c.hk.logger() != nil && !c.closing && c.s.HasStreams() && c.s.RemovesLast(id) {
		c.hk.warn("zmux: the application removed the last pipe of a direction while streams are open: the session suspends (resume) or ends with PipesLost")
	}
}

// unattendedLocked reports whether a suspension has nothing in the
// library that will dial: the session can rejoin (the peer gave a Session
// Locator) but has no Config.Redial and no running MaintainPipes.
func (c *Conn) unattendedLocked() bool {
	if c.cfg.Redial != nil || c.pools.Load() > 0 {
		return false
	}
	p, ok := c.s.PeerParams()
	return ok && len(p.Locator) > 0
}

// Abort removes the pipe abruptly: failure, reinjection and
// PIPE_ABANDON(ZMUX_PIPE_FAILED).
func (p *Pipe) Abort(cause error) error {
	c := p.c
	c.lock()
	defer c.unlock()
	c.s.SetPipeCause(p.id, cause) // reported back with the retirement
	c.warnLastPipeLocked(p.id)
	return pipeErr(c.s.RemovePipe(p.id, false))
}

func pipeErr(err error) error {
	if errors.Is(err, core.ErrInvalid) {
		return ErrPipeGone
	}
	return apiErr(err)
}

// SetScheduler changes the session's scheduling policy; it applies from
// the next chunk.
func (c *Conn) SetScheduler(s Scheduler) {
	c.lock()
	defer c.unlock()
	if s == nil {
		s = SchedulerAffinity()
	}
	if cp, ok := s.(customPolicy); ok {
		if c.hk == nil {
			c.hk = &hooks{c: c, kick: make(chan struct{}, 1)}
		}
		c.hk.mu.Lock()
		c.hk.pick = cp.p
		c.hk.mu.Unlock()
	}
	c.s.SetPolicy(s.policy())
}

// ---- statistics ----

// OutboundState is the state of a pipe's outbound direction in its life
// cycle; OutboundNone when the pipe has none.
type OutboundState uint8

const (
	OutboundNone     = OutboundState(core.OutNone)
	OutboundJoining  = OutboundState(core.OutJoining)
	OutboundActive   = OutboundState(core.OutActive)
	OutboundSuspect  = OutboundState(core.OutSuspect)
	OutboundDraining = OutboundState(core.OutDraining)
	OutboundFailed   = OutboundState(core.OutFailed)
	OutboundClosed   = OutboundState(core.OutClosed)
)

// String returns the state's name in lower case ("active").
func (s OutboundState) String() string {
	switch s {
	case OutboundNone:
		return "none"
	case OutboundJoining:
		return "joining"
	case OutboundActive:
		return "active"
	case OutboundSuspect:
		return "suspect"
	case OutboundDraining:
		return "draining"
	case OutboundFailed:
		return "failed"
	case OutboundClosed:
		return "closed"
	}
	return "OutboundState(?)"
}

// InboundState is the state of a pipe's inbound direction in its life
// cycle; InboundNone when the pipe has none.
type InboundState uint8

const (
	InboundNone    = InboundState(core.InNone)
	InboundJoining = InboundState(core.InJoining)
	InboundOpen    = InboundState(core.InOpen)
	InboundEnded   = InboundState(core.InEnded)
)

// String returns the state's name in lower case ("open").
func (s InboundState) String() string {
	switch s {
	case InboundNone:
		return "none"
	case InboundJoining:
		return "joining"
	case InboundOpen:
		return "open"
	case InboundEnded:
		return "ended"
	}
	return "InboundState(?)"
}

// PipeStats is a snapshot of one attachment. Fields marked
// estimate are the sender's estimators and change as
// samples arrive; a gauge is a current value; the other counts only
// increase. Each field holds a value it had during the call; the state
// fields (the direction states, flags, priorities and their sources,
// probation, the lanes present, Pipe) are read at one point, while
// relations between counts hold for their final values, not in every
// snapshot. An absent field is its zero value: an estimate or duration
// before the first sample, the fields that need acknowledgements when
// ConnectionState shows pipe acknowledgement not in use, and every field
// of a direction the attachment does not have, whose state is then
// OutboundNone or InboundNone.
type PipeStats struct {
	PipeID     PipeID
	FateGroup  uint64
	Label      string
	Variant    string     // PipeIO.Variant
	RecordMode RecordMode // who protects its records
	Local      bool       // this endpoint joined the attachment
	// PeerAttached reports that the peer has attached the pipe
	// (WaitAttached); once true it stays true.
	PeerAttached bool
	Carrier      CarrierKind // PipeIO.Carrier
	// Outbound and Inbound are the states of the two directions;
	// OutboundNone and InboundNone for one the attachment lacks.
	Outbound     OutboundState
	Inbound      InboundState
	AttachedFor  time.Duration
	SuspectCount uint64 // times the outbound pipe became suspect
	FailureCount uint64 // 0 or 1: whether the attachment failed
	Failure      error  // why it failed, once it did

	// The effective priority and standby flag (Standby, below) of the
	// outbound direction (lower priority preferred) and where they come
	// from: "local", "peer" or "default".
	SendPriority       uint64
	SendPrioritySource string
	StandbySource      string
	// ReceivePriority and ReceiveStandby (below) are requested of the peer
	// for its outbound direction; RequestSeq is the sequence number of the
	// last PIPE_STATUS request sent.
	ReceivePriority uint64
	RequestSeq      uint64
	PeerRequest     *PeerRequest // the peer's latest request; nil before one
	Weight          uint64
	// ProbationUntil is the end of the outbound pipe's probation, zero
	// outside probation or for a base so large that it never ends.
	ProbationUntil time.Time
	Standby        bool
	ReceiveStandby bool
	InProbation    bool
	Revalidating   bool // awaiting revalidation
	Eligible       bool // eligible under tier gating now
	Writable       bool // the outbound carrier takes bytes now
	Saturated      bool // the outbound pipe's tier is saturated

	RTTSource    string        // "ack", "kernel" or "ping"; "" before the first sample
	SmoothedRTT  time.Duration // estimate
	MinRTT       time.Duration // estimate
	RTTVar       time.Duration // estimate
	OneWayDelay  time.Duration // estimate; needs acknowledgements
	DeliveryRate uint64        // R_p in bytes per second, estimate; needs acknowledgements

	// QueuedBytes is a gauge of the bytes the outbound carrier holds
	// unsent, the adapter's unsent count summed over the lanes; 0 where
	// the adapter reports none. QueueDelay is the estimate QueuedBytes /
	// DeliveryRate, 0 when either is.
	QueuedBytes   uint64
	QueueDelay    time.Duration
	BytesInFlight uint64 // gauge: sent and not acknowledged

	// BytesSent and BytesReceived are the outbound and inbound positions
	// over all lanes; BytesAcked the acknowledged outbound position.
	BytesSent       uint64
	BytesReceived   uint64
	BytesAcked      uint64
	RecordsSent     uint64
	RecordsReceived uint64
	// BytesReinjected counts stream bytes sent again elsewhere because
	// this pipe failed or became suspect.
	BytesReinjected uint64
	// BytesLost is the outbound bytes sent and not acknowledged when the
	// pipe failed, over all lanes (BytesSent - BytesAcked at that moment);
	// 0 before a failure and without acknowledgements. They are carrier
	// bytes with frame overhead, sent again elsewhere by reinjection,
	// unlike BytesReinjected, the stream payload sent again. RecordsLost is
	// its estimate in records, BytesLost x RecordsSent / BytesSent rounded
	// up.
	BytesLost   uint64
	RecordsLost uint64
	// BytesRetransmitted is the bytes the carrier itself retransmitted, as
	// its adapter reports them (PipeIO: Retransmitted), sampled at most once
	// a second; 0 when it reports none. Pipes sharing one carrier
	// connection (QUIC streams of one connection) report the connection's
	// count, the same on each. LossRate is the smoothed carrier loss rate:
	// an EWMA (gain 1/8) of the retransmitted bytes per byte sent between
	// samples at least 1 s apart; 0 before the first sample.
	BytesRetransmitted     uint64
	LossRate               float64
	BytesOpportunistic     uint64 // opportunistic copies sent on the outbound pipe
	BytesRedundant         uint64 // redundant copies sent on the outbound pipe
	BytesDuplicateReceived uint64 // duplicate stream bytes received on the inbound pipe
	PingsSent              uint64
	PingsAnswered          uint64 // PIPE_PINGs answered by an acknowledgement

	SinceLastAck     time.Duration
	SinceLastReceive time.Duration
	// SinceLastTraffic is the time since a record carrying a traffic frame
	// (STREAM, RESET_STREAM, RESET_STREAM_AT, STOP_SENDING or DATAGRAM) was
	// sent on the pipe or completely received on it, or since the pipe
	// became live. Probes, acknowledgements, other control frames and
	// padding do not count.
	SinceLastTraffic time.Duration

	// LanesOut and LanesIn are gauges of the lanes of each direction, lane
	// 0 included; LanesRefused counts own outbound lanes refused.
	LanesOut, LanesIn int
	LanesRefused      uint64
	// Lanes are the positions of each lane of a pipe with lanes; nil
	// without, lane 0 then being the pipe itself.
	Lanes []LaneStats
}

// LaneStats are the positions of one lane; a direction the lane does not
// have reports 0.
type LaneStats struct {
	Lane             uint64
	PositionSent     uint64 // bytes written on the outbound lane, its first record included
	PositionAcked    uint64 // the position the peer acknowledged on it
	PositionReceived uint64 // the position of the inbound lane
}

// PeerRequest is the peer's latest PIPE_STATUS request for the local
// outbound direction.
type PeerRequest struct {
	Priority uint64
	Standby  bool
}

// Stats returns the pipe's snapshot; false once the attachment is retired
// (or the session that ended released it): a read reports the absence
// rather than an error.
func (p *Pipe) Stats() (PipeStats, bool) {
	c := p.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now()) // the durations since an event are current
	pi, ok := c.s.PipeInfo(p.id)
	if !ok {
		return PipeStats{}, false
	}
	return c.pipeStats(pi), true
}

// endLocked is the end record of attachment id (under the lock).
func (c *Conn) endLocked(id uint64) *pipeEnd {
	if pd := c.pipes[id]; pd != nil {
		return pd.end
	}
	return goneEnd
}

// Pipes returns the handles of the session's current attachments. A
// session without multipath has pipe 0.
func (c *Conn) Pipes() []*Pipe {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now()) // the durations since an event are current
	ids := c.s.PipeIDs(nil)
	out := make([]*Pipe, len(ids))
	for i, id := range ids {
		out[i] = &Pipe{c: c, id: id, end: c.endLocked(id)}
	}
	return out
}

// PipeStats returns a snapshot of every attachment. A session without
// multipath reports pipe 0.
func (c *Conn) PipeStats() []PipeStats {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now()) // the durations since an event are current
	ids := c.s.PipeIDs(nil)
	out := make([]PipeStats, 0, len(ids))
	for _, id := range ids {
		if pi, ok := c.s.PipeInfo(id); ok {
			out = append(out, c.pipeStats(pi))
		}
	}
	return out
}

// Pipe returns the handle of attachment id while the session knows it.
func (c *Conn) Pipe(id PipeID) (*Pipe, bool) {
	c.lock()
	defer c.unlock()
	if _, ok := c.s.PipeInfo(uint64(id)); !ok {
		return nil, false
	}
	return &Pipe{c: c, id: uint64(id), end: c.endLocked(uint64(id))}, true
}

func (c *Conn) pipeStats(pi core.PipeInfo) PipeStats {
	st := PipeStats{PipeID: PipeID(pi.ID), RecordMode: c.pipeModeLocked(pi.ID), Local: pi.Local, PeerAttached: pi.PeerAttached, FateGroup: pi.FateGroup, Label: c.labels[pi.ID],
		Carrier: CarrierKind(pi.Carrier), Variant: c.variantLocked(pi.ID), Outbound: OutboundState(pi.OutState), Inbound: InboundState(pi.InState),
		AttachedFor: time.Duration(pi.AttachedFor), SuspectCount: pi.SuspectCount, FailureCount: pi.FailureCount,
		Failure: pi.Failure, SendPriority: pi.SendPriority, SendPrioritySource: pi.PrioritySource,
		Standby: pi.Standby, StandbySource: pi.StandbySource, ReceivePriority: pi.ReceivePriority,
		ReceiveStandby: pi.ReceiveStandby, RequestSeq: pi.StatusSeq, Weight: pi.Weight,
		InProbation: pi.InProbation, Revalidating: pi.Revalidating, Eligible: pi.Eligible,
		Writable: pi.Writable, Saturated: pi.Saturated, RTTSource: pi.RTTSource,
		SmoothedRTT: time.Duration(pi.SmoothedRTT), MinRTT: time.Duration(pi.MinRTT),
		RTTVar: time.Duration(pi.RTTVar), OneWayDelay: time.Duration(pi.OWD),
		DeliveryRate: rateBytes(pi.DeliveryRate), QueuedBytes: pi.QueuedBytes, BytesInFlight: pi.BytesInFlight,
		BytesSent: pi.BytesSent, BytesReceived: pi.BytesReceived, BytesAcked: pi.BytesAcked,
		RecordsSent: pi.RecordsSent, RecordsReceived: pi.RecordsReceived, BytesReinjected: pi.BytesReinjected,
		RecordsLost: pi.RecordsLost, BytesLost: pi.BytesLost, BytesRetransmitted: pi.BytesRetransmitted,
		LossRate:           pi.LossRate,
		BytesOpportunistic: pi.BytesOpportunistic, BytesRedundant: pi.BytesRedundant,
		BytesDuplicateReceived: pi.BytesDuplicateRecv, PingsSent: pi.PingsSent, PingsAnswered: pi.PingsAnswered,
		SinceLastAck: time.Duration(pi.SinceLastAck), SinceLastReceive: time.Duration(pi.SinceLastReceive),
		SinceLastTraffic: time.Duration(pi.SinceLastTraffic), LanesOut: pi.LanesOut, LanesIn: pi.LanesIn,
		LanesRefused: uint64(pi.LanesRefused)}
	for _, l := range pi.Lanes {
		st.Lanes = append(st.Lanes, LaneStats{Lane: l.Lane, PositionSent: l.PositionSent,
			PositionAcked: l.PositionAcked, PositionReceived: l.PositionReceived})
	}
	if pi.PeerRequest {
		st.PeerRequest = &PeerRequest{Priority: pi.PeerPriority, Standby: pi.PeerStandby}
	}
	if pi.InProbation && pi.ProbationUntil > 0 {
		st.ProbationUntil = c.clk.wall(pi.ProbationUntil)
	}
	if pi.DeliveryRate > 0 {
		st.QueueDelay = time.Duration(float64(pi.QueuedBytes) / pi.DeliveryRate * 1e9)
	}
	if st.Outbound == OutboundNone || !pi.HasOut {
		st.clearOutbound()
	}
	if st.Inbound == InboundNone || !pi.HasIn {
		st.clearInbound()
	}
	return st
}

// clearOutbound zeroes the fields of the outbound direction, which an
// attachment without one reports as absent: its state is
// OutboundNone and every other field of the direction is 0.
func (st *PipeStats) clearOutbound() {
	st.Outbound = OutboundNone
	st.SuspectCount = 0
	st.SendPriority, st.SendPrioritySource, st.Standby, st.StandbySource = 0, "", false, ""
	st.PeerRequest, st.Weight = nil, 0
	st.ProbationUntil, st.InProbation, st.Revalidating = time.Time{}, false, false
	st.Eligible, st.Writable, st.Saturated = false, false, false
	st.RTTSource, st.SmoothedRTT, st.MinRTT, st.RTTVar, st.OneWayDelay, st.DeliveryRate = "", 0, 0, 0, 0, 0
	st.QueuedBytes, st.QueueDelay, st.BytesInFlight = 0, 0, 0
	st.BytesSent, st.BytesAcked, st.RecordsSent = 0, 0, 0
	st.BytesReinjected, st.BytesOpportunistic, st.BytesRedundant = 0, 0, 0
	st.RecordsLost, st.BytesLost, st.BytesRetransmitted, st.LossRate = 0, 0, 0, 0
	st.PingsSent, st.PingsAnswered, st.SinceLastAck = 0, 0, 0
	st.LanesOut, st.LanesRefused = 0, 0
}

// clearInbound zeroes the fields of the inbound direction (clearOutbound).
func (st *PipeStats) clearInbound() {
	st.Inbound = InboundNone
	st.ReceivePriority, st.ReceiveStandby, st.RequestSeq = 0, false, 0
	st.BytesReceived, st.RecordsReceived, st.BytesDuplicateReceived = 0, 0, 0
	st.SinceLastReceive, st.LanesIn = 0, 0
}

// pipeModeLocked is the record mode of an attachment.
func (c *Conn) pipeModeLocked(id uint64) RecordMode {
	if id == 0 && !c.cc.SecureFirstPipe {
		return RecordUnmanaged
	}
	return RecordMode(c.s.PipeMode(id))
}

// WeakestRecordMode is the session's protection: that of its weakest pipe, Unmanaged
// included. A host can authorize by it.
func (c *Conn) WeakestRecordMode() RecordMode {
	c.lock()
	defer c.unlock()
	m, unmanaged := c.s.WeakestMode()
	if unmanaged {
		return RecordUnmanaged
	}
	return RecordMode(m)
}

// DeclareEndToEnd states that the host has proven pipe 0 end to end, for example with a
// pinned certificate or an attestation over an exporter of its own label. It is local
// and sends nothing. Until then a session whose first pipe is not on the secure carrier
// has no join keys: local joins fail with ErrJoinPending and the peer's are held. The
// library stops a host from forgetting or joining too early, not from declaring falsely.
// A session on a secure first pipe needs no call, and one is a no-op.
func (c *Conn) DeclareEndToEnd() {
	if c == nil {
		return
	}
	c.lock()
	defer c.unlock()
	c.s.DeclareEndToEnd()
}
