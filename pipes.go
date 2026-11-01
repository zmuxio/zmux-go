package zmux

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// PipeOptions are the options of an attachment. The zero value is a
// bidirectional pipe that follows the peer's priority requests and the
// session's defaults.
//
// The Send fields are pointers because nil has its own meaning: the local
// outbound direction then follows the peer's request (PIPE_STATUS), which
// can change at any time; a set value, the zero value included, wins over
// it. Weight is a pointer because 0 is a weight of its own (excluded).
// Set a pointer field with new, as in SendStandby: new(true) or
// Weight: new(uint64(0)). The Receive fields are plain values because
// they are always sent: the PIPE_JOIN carries a request for the peer's outbound
// direction, so the zero values ask for tier 0, not standby.
type PipeOptions struct {
	// SendPriority is the tier of the local outbound direction (lower is
	// preferred). nil: the peer's request, else tier 0.
	SendPriority *uint64
	// SendStandby makes the local outbound direction carry data only on
	// failover. nil: the peer's request, else active.
	SendStandby *bool
	// Weight is the share for SpreadRoundRobin and the factor of
	// SpreadRateWeighted. nil: 1; 0 excludes the pipe unless every
	// candidate has 0.
	Weight *uint64
	// Probation is the base of the pipe's probation. Zero is the
	// session's; a negative value is invalid.
	Probation time.Duration
	// Label names the pipe in statistics.
	Label string
	// ReceivePriority is the tier requested for the peer's outbound
	// direction (zero: tier 0).
	ReceivePriority uint64
	// FateGroup groups pipes that share a path; a pipe that replaces a
	// failed one on the same path reuses its fate group. 0: the carrier's
	// PipeIO.FateGroup, else none (an independent path).
	FateGroup uint64
	// SuspectFloor (at least 50 ms) and DeadTimeout (at least 1 s) of the
	// pipe. Zero uses the session's.
	SuspectFloor time.Duration
	DeadTimeout  time.Duration
	// ProbeInterval is the probe interval k of the pipe while it is idle
	// (at least 1 s), measured to the carrier: with a shaping delay of up
	// to H, probes wait k_eff = max(1 s, k - H). Zero uses the session's;
	// a negative value turns the idle probes of the pipe off, as
	// SchedulerParams.ProbeInterval does; otherwise at least 1 s. On an
	// attachment without an outbound direction at the peer at most 15 s
	// takes effect, and the probes stay.
	ProbeInterval time.Duration
	// ProbeJitter is the probe jitter j of the pipe, from 0 to below 1:
	// each probe waits a uniform draw from [k_eff x (1 - j), k_eff]. Zero
	// uses the session's; a jitter of 0 takes NoProbeJitter.
	ProbeJitter float64
	// NoProbeJitter makes the pipe's j 0, the fixed schedule.
	NoProbeJitter bool
	// NoProbation turns the pipe's probation off; Probation must then be
	// zero.
	NoProbation bool
	// Direction from the local endpoint: DirBidirectional (the zero
	// value), DirSendOnly (an uplink) or DirReceiveOnly (a downlink).
	Direction Direction
	// ReceiveStandby asks the peer to make its outbound direction standby
	// (zero: active).
	ReceiveStandby bool
	// CarrierBound ties the join token to the new carrier's innermost
	// end-to-end handshake (MULTIPATH.md 10.5): its exporter (Exporter),
	// read when AddPipe is called, so the carrier's handshake must have
	// completed by then; the listener reads its own side when the join
	// arrives.
	CarrierBound bool
}

func (o *PipeOptions) join(dir Direction) (core.JoinOptions, error) {
	var j core.JoinOptions
	j.Dir = 2 // DirBidirectional, and any value out of range
	switch dir {
	case DirSendOnly:
		j.Dir = 0
	case DirReceiveOnly:
		j.Dir = 1
	case DirBidirectional:
	}
	live := PipeLiveness{SuspectFloor: o.SuspectFloor, DeadTimeout: o.DeadTimeout, ProbeInterval: o.ProbeInterval,
		ProbeJitter: o.ProbeJitter, NoProbeJitter: o.NoProbeJitter}
	var err error
	if j.Live, err = live.core(); err != nil {
		return j, err
	}
	if o.Probation < 0 {
		return j, fmt.Errorf("%w: negative Probation", ErrInvalidConfig)
	}
	if o.NoProbation && o.Probation != 0 {
		return j, fmt.Errorf("%w: Probation with NoProbation", ErrInvalidConfig)
	}
	j.FateGroup, j.SendPriority, j.SendStandby = o.FateGroup, o.SendPriority, o.SendStandby
	j.RecvPriority, j.RecvStandby = o.ReceivePriority, o.ReceiveStandby
	j.Weight = 1
	if o.Weight != nil {
		j.Weight = *o.Weight
	}
	if o.NoProbation || o.Probation != 0 {
		d := core.Duration(o.Probation)
		j.Probation = &d
	}
	return j, nil
}

// Pipe is one attachment of a session. Its methods are safe for concurrent
// use; they fail with ErrPipeGone once the attachment is retired.
type Pipe struct {
	c  *Conn
	id uint64
}

// ID returns the pipe ID.
func (p *Pipe) ID() uint64 { return p.id }

// Admitted returns the Decision.Value of the admission that accepted the
// pipe's carrier, the one that carried its PIPE_JOIN (or the session's
// value for the bootstrap pipe, Conn.Admitted). It is nil without one, on
// the dialing side, and once the session no longer knows the pipe. zmux
// does not compare it with the session's: a host that requires a join to
// come from the session's client compares them, in WrapPipe or here.
func (p *Pipe) Admitted() any {
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

// AddPipe joins a new carrier to the session in band (MULTIPATH.md 5.7) and
// waits until the join is confirmed, the pipe fails, or ctx ends (the pipe
// is then removed). A join that the attachment cap or the refusal backoff
// holds waits for room. With CarrierBound, a carrier whose TLS handshake
// (a *tls.Conn half, or a Handshaker) has not completed is first driven
// through it with ctx; a failed handshake closes it and is returned.
func (c *Conn) AddPipe(ctx context.Context, p PipeIO, o PipeOptions) (*Pipe, error) {
	if o.CarrierBound {
		// The binding is the carrier's exporter, read once its own
		// handshake completed: a TLS half (or Handshaker) not yet past it
		// is driven through it with ctx first.
		if handshake := pendingHandshake(p); handshake != nil {
			if err := handshake(ctx); err != nil {
				closePipeIO(p)
				return nil, fmt.Errorf("zmux: carrier handshake: %w", err)
			}
		}
	}
	pp, err := c.addPipe(p, o, false)
	if err != nil {
		return nil, err
	}
	if err := pp.waitJoined(ctx); err != nil {
		_ = pp.Abort(err)
		return nil, err
	}
	return pp, nil
}

func (c *Conn) addPipe(p PipeIO, o PipeOptions, oob bool) (*Pipe, error) {
	if r, ok := p.R.(*httpRespReader); ok && r.peer != nil {
		// Its pairing token makes pipe pair 0 only (SPEC.md 6.1.1).
		return nil, fmt.Errorf("%w: a split bootstrap carrier cannot join a session; join an uplink and a downlink (HTTPSplitJoin)", ErrInvalidConfig)
	}
	dir := o.Direction
	if (dir != DirReceiveOnly && p.W == nil) || (dir != DirSendOnly && p.R == nil && !oob) {
		return nil, fmt.Errorf("%w: the carrier lacks a half the direction needs", ErrInvalidConfig)
	}
	if err := p.checkOverhead(); err != nil {
		return nil, err
	}
	if o.FateGroup == 0 {
		o.FateGroup = p.FateGroup
	}
	j, err := o.join(dir)
	if err != nil {
		return nil, err
	}
	j.OOB = oob
	if o.CarrierBound {
		e, ok := Exporter(p)
		if !ok {
			return nil, fmt.Errorf("%w: CarrierBound on a carrier without an exporter", ErrInvalidConfig)
		}
		b, err := e.ExportKeyingMaterial(wire.ExporterPipe, nil, 32)
		if err != nil {
			return nil, fmt.Errorf("zmux: carrier exporter: %w", err)
		}
		j.CarrierBound, j.Binding = true, b
	}
	c.lock()
	defer c.unlock()
	if c.closing {
		return nil, c.errLocked()
	}
	outer := outerOf(p)
	if err := nestPipe(c, outer); err != nil {
		return nil, err
	}
	pd := c.newPipeDrv(c.s.NextPipeID(), p)
	pd.outer = outer
	low := !oob && pd.tcp.setLowWater(multiLowWater)
	info := carrierInfo(p, pd.tcp, low)
	j.In, j.Out, j.OutSig, j.Kind = info, info, pd.wsig, uint8(p.Carrier)
	id, _, err := c.s.AddPipe(j)
	if err != nil {
		unnestPipe(c, pd.outer)
		return nil, apiErr(err)
	}
	pd.id = id
	c.labels[id] = o.Label
	if !oob {
		c.startPipeLocked(pd, dir != DirSendOnly, dir != DirReceiveOnly)
	} else {
		c.pipes[id] = pd
	}
	return &Pipe{c: c, id: id}, nil
}

// waitJoined waits until the outbound direction (or, for a downlink, the
// inbound one) is ACTIVE/OPEN, or the pipe failed.
func (p *Pipe) waitJoined(ctx context.Context) error {
	c := p.c
	sig := core.NewSignal()
	defer c.unwatchPipes(sig)
	for {
		c.lock()
		out, in, ok := c.s.PipeState(p.id)
		if ok && (out == core.OutJoining || out == core.OutNone && in == core.InJoining) {
			sig.Drain()
			c.watchPipes(sig)
		}
		c.unlock()
		switch {
		case !ok || out == core.OutFailed || out == core.OutClosed || out == core.OutRetired ||
			out == core.OutNone && in != core.InJoining && in != core.InOpen:
			return fmt.Errorf("zmux: pipe %d not joined: %w", p.id, ErrPipeGone)
		case out == core.OutActive || out == core.OutSuspect || out == core.OutNone && in == core.InOpen:
			return nil
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

// JoinReady reports whether the maintenance target and join admission permit
// dialing another pipe now. Explicit AddPipe still follows the physical cap.
func (c *Conn) JoinReady() bool {
	if c == nil {
		return false
	}
	c.lock()
	defer c.unlock()
	return c.s.JoinReady()
}

// Reservation is a pipe reserved for an out-of-band join (MULTIPATH.md
// 5.3): send Header() with the carrier's request as the Zmux-Pipe-Join
// header, then Attach the carrier.
type Reservation struct {
	p      *Pipe
	header string
	dir    Direction
}

// ReservePipe reserves a pipe for an out-of-band join.
func (c *Conn) ReservePipe(dir Direction, o PipeOptions) (*Reservation, error) {
	o.Direction = dir
	pp, err := c.addPipe(PipeIO{W: nopWriteCloser{}, R: nil}, o, true)
	if err != nil {
		return nil, err
	}
	c.lock()
	h := c.oobHeaders[pp.id]
	delete(c.oobHeaders, pp.id)
	c.unlock()
	return &Reservation{p: pp, header: h, dir: dir}, nil
}

// Header returns the Zmux-Pipe-Join header value (base64url of the
// PIPE_JOIN frame, without padding).
func (r *Reservation) Header() string { return r.header }

// Attach binds the carrier of the reserved pipe and waits until the join
// is confirmed (see AddPipe).
func (r *Reservation) Attach(ctx context.Context, p PipeIO) (*Pipe, error) {
	c := r.p.c
	c.lock()
	pd := c.pipes[r.p.id]
	if pd == nil || c.closing {
		c.unlock()
		return nil, ErrPipeGone
	}
	pd.hc = newHalfCloser(p)
	pd.framing = p.Framing
	pd.tcp = tcpOf(p.W)
	if o, ok := p.W.(LaneOpener); ok {
		pd.opener = o
	}
	// The reservation's placeholder said nothing of the carrier.
	c.s.AttachCarrier(r.p.id, carrierInfo(p, pd.tcp, false), uint8(p.Carrier))
	c.startPipeLocked(pd, r.dir != DirSendOnly, r.dir != DirReceiveOnly)
	c.unlock()
	if err := r.p.waitJoined(ctx); err != nil {
		_ = r.p.Abort(err)
		return nil, err
	}
	return r.p, nil
}

// Cancel abandons the reservation.
func (r *Reservation) Cancel() { _ = r.p.Abort(errors.New("zmux: reservation cancelled")) }

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

func encodeJoinHeader(frame []byte) string { return base64.RawURLEncoding.EncodeToString(frame) }

// ---- settings ----

func (p *Pipe) set(o core.PipeSettings) error {
	c := p.c
	c.lock()
	defer c.unlock()
	if err := c.s.SetPipe(p.id, o); err != nil {
		if errors.Is(err, core.ErrInvalid) {
			return ErrPipeGone
		}
		return apiErr(err)
	}
	return nil
}

// SetSendPriority sets the tier of the local outbound direction; it wins
// over the peer's request. Nothing is sent.
func (p *Pipe) SetSendPriority(v uint64) error { return p.set(core.PipeSettings{SendPriority: &v}) }

// ClearSendPriority returns the outbound tier to the peer's request.
func (p *Pipe) ClearSendPriority() error { return p.set(core.PipeSettings{ClearSendPriority: true}) }

// SetReceivePriority asks the peer to use v for its outbound direction;
// the request is sent at once as PIPE_STATUS (once the pipe is ACTIVE).
func (p *Pipe) SetReceivePriority(v uint64) error { return p.set(core.PipeSettings{RecvPriority: &v}) }

// SetSendStandby makes the local outbound direction standby (or not).
func (p *Pipe) SetSendStandby(b bool) error { return p.set(core.PipeSettings{SendStandby: &b}) }

// ClearSendStandby returns the standby flag to the peer's request.
func (p *Pipe) ClearSendStandby() error { return p.set(core.PipeSettings{ClearSendStandby: true}) }

// SetReceiveStandby asks the peer to treat its outbound direction as
// standby (or not).
func (p *Pipe) SetReceiveStandby(b bool) error { return p.set(core.PipeSettings{RecvStandby: &b}) }

// SetWeight sets the pipe's weight for the round-robin policies (local).
func (p *Pipe) SetWeight(w uint64) error { return p.set(core.PipeSettings{Weight: &w}) }

// SetProbation sets the base of the pipe's probation; 0 disables it, and
// a negative value is invalid (ErrInvalidConfig).
func (p *Pipe) SetProbation(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("%w: negative Probation", ErrInvalidConfig)
	}
	v := core.Duration(d)
	return p.set(core.PipeSettings{Probation: &v})
}

// ClearProbation returns the base of the pipe's probation to the
// session's.
func (p *Pipe) ClearProbation() error {
	v := core.Duration(-1)
	return p.set(core.PipeSettings{Probation: &v})
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

// Close removes the pipe gracefully: PIPE_ABANDON(NO_ERROR) and drain
// (MULTIPATH.md 8.2). It does not block; the pipe's end is reported by
// NextEvent.
func (p *Pipe) Close() error {
	c := p.c
	c.lock()
	defer c.unlock()
	return pipeErr(c.s.RemovePipe(p.id, true))
}

// Abort removes the pipe abruptly: failure, reinjection and
// PIPE_ABANDON(ZMUX_PIPE_FAILED).
func (p *Pipe) Abort(cause error) error {
	c := p.c
	c.lock()
	defer c.unlock()
	c.s.SetPipeCause(p.id, cause)
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
		s = Affinity()
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

// OutboundState is the state of a pipe's outbound direction (STATE_MACHINE.md
// 3.1); OutboundNone when the pipe has none.
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

// InboundState is the state of a pipe's inbound direction (STATE_MACHINE.md
// 3.2); InboundNone when the pipe has none.
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

// PipeStats is a snapshot of one attachment (API.md 7.2). Fields marked
// estimate are the sender's estimators (SCHEDULER.md 2) and change as
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
	Pipe      uint64 // the pipe ID
	FateGroup uint64
	Label     string
	Local     bool        // this endpoint joined the attachment
	Carrier   CarrierKind // PipeIO.Carrier
	Variant   string      // PipeIO.Variant
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

// Pipes returns the handles of the session's current attachments. A
// session without multipath has pipe 0.
func (c *Conn) Pipes() []*Pipe {
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now()) // the durations since an event are current
	ids := c.s.PipeIDs(nil)
	out := make([]*Pipe, len(ids))
	for i, id := range ids {
		out[i] = &Pipe{c: c, id: id}
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
func (c *Conn) Pipe(id uint64) (*Pipe, bool) {
	c.lock()
	defer c.unlock()
	if _, ok := c.s.PipeInfo(id); !ok {
		return nil, false
	}
	return &Pipe{c: c, id: id}, true
}

func (c *Conn) pipeStats(pi core.PipeInfo) PipeStats {
	st := PipeStats{Pipe: pi.ID, Local: pi.Local, FateGroup: pi.FateGroup, Label: c.labels[pi.ID],
		Carrier: CarrierKind(pi.Carrier), Variant: c.variantLocked(pi.ID), Outbound: OutboundState(pi.OutState), Inbound: InboundState(pi.InState),
		AttachedFor: time.Duration(pi.AttachedFor), SuspectCount: pi.SuspectCount, FailureCount: pi.FailureCount,
		Failure: pi.Failure, SendPriority: pi.SendPriority, SendPrioritySource: pi.PrioritySource,
		Standby: pi.Standby, StandbySource: pi.StandbySource, ReceivePriority: pi.ReceivePriority,
		ReceiveStandby: pi.ReceiveStandby, RequestSeq: pi.StatusSeq, Weight: pi.Weight,
		InProbation: pi.InProbation, Revalidating: pi.Revalidating, Eligible: pi.Eligible,
		Writable: pi.Writable, Saturated: pi.Saturated, RTTSource: pi.RTTSource,
		SmoothedRTT: time.Duration(pi.SmoothedRTT), MinRTT: time.Duration(pi.MinRTT),
		RTTVar: time.Duration(pi.RTTVar), OneWayDelay: time.Duration(pi.OWD),
		DeliveryRate: uint64(pi.DeliveryRate), QueuedBytes: pi.QueuedBytes, BytesInFlight: pi.BytesInFlight,
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
// attachment without one reports as absent (API.md 7.2): its state is
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
