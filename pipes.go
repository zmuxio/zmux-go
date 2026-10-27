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
// outbound direction then follows the peer's request (PIPE_STATUS); a set
// value wins over it. The Receive fields are plain values because they
// are always sent: the PIPE_JOIN carries a request for the peer's outbound
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
	// Probation is the base of the pipe's probation. nil: the session's;
	// 0 disables it.
	Probation *time.Duration
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
	// pipe; 0 uses the session's.
	SuspectFloor time.Duration
	DeadTimeout  time.Duration
	// Direction from the local endpoint: DirBidirectional (the zero
	// value), DirSendOnly (an uplink) or DirReceiveOnly (a downlink).
	Direction Direction
	// ReceiveStandby asks the peer to make its outbound direction standby
	// (zero: active).
	ReceiveStandby bool
	// CarrierBound ties the join token to the new carrier's TLS 1.3
	// handshake (MULTIPATH.md 10.5); the carrier must be a TLS connection
	// that terminates at the peer.
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
	if o.SuspectFloor != 0 && o.SuspectFloor < 50*time.Millisecond {
		return j, fmt.Errorf("%w: SuspectFloor below 50 ms", ErrInvalidConfig)
	}
	if o.DeadTimeout != 0 && o.DeadTimeout < time.Second {
		return j, fmt.Errorf("%w: DeadTimeout below 1 s", ErrInvalidConfig)
	}
	j.FateGroup, j.SendPriority, j.SendStandby = o.FateGroup, o.SendPriority, o.SendStandby
	j.RecvPriority, j.RecvStandby = o.ReceivePriority, o.ReceiveStandby
	j.Weight = 1
	if o.Weight != nil {
		j.Weight = *o.Weight
	}
	if o.Probation != nil {
		d := core.Duration(*o.Probation)
		j.Probation = &d
	}
	j.SuspectFloor, j.DeadTimeout = core.Duration(o.SuspectFloor), core.Duration(o.DeadTimeout)
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

// AddPipe joins a new carrier to the session in band (MULTIPATH.md 5.7) and
// waits until the join is confirmed, the pipe fails, or ctx ends (the pipe
// is then removed). A join that the attachment cap or the refusal backoff
// holds waits for room.
func (c *Conn) AddPipe(ctx context.Context, p PipeIO, o PipeOptions) (*Pipe, error) {
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
	dir := o.Direction
	if (dir != DirReceiveOnly && p.W == nil) || (dir != DirSendOnly && p.R == nil && !oob) {
		return nil, fmt.Errorf("%w: the carrier lacks a half the direction needs", ErrInvalidConfig)
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
		st, ok := carrierIsTLS(p)
		if !ok {
			return nil, fmt.Errorf("%w: CarrierBound on a carrier without TLS", ErrInvalidConfig)
		}
		b, err := st.ExportKeyingMaterial(wire.ExporterPipe, nil, 32)
		if err != nil {
			return nil, fmt.Errorf("zmux: TLS exporter: %w", err)
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
	j.In, j.Out, j.OutSig = info, info, pd.wsig
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
	for {
		c.lock()
		out, in, ok := c.s.PipeState(p.id)
		if ok && (out == "JOINING" || out == "NONE" && in == "JOINING") {
			sig.Drain()
			c.pipeWaiters = append(c.pipeWaiters, sig)
		}
		c.unlock()
		switch {
		case !ok || out == "FAILED" || out == "CLOSED" || out == "RETIRED" || out == "NONE" && in != "JOINING" && in != "OPEN":
			return fmt.Errorf("zmux: pipe %d not joined: %w", p.id, ErrPipeGone)
		case out == "ACTIVE" || out == "SUSPECT" || out == "NONE" && in == "OPEN":
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

// JoinReady reports whether a pipe added now would join at once rather than
// wait for the attachment cap or the refusal backoff: dial a carrier only
// then, so that none is held idle.
func (c *Conn) JoinReady() bool {
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
	if o, ok := p.W.(laneOpener); ok {
		pd.opener = o
	}
	c.startPipeLocked(pd, r.dir != DirSendOnly, r.dir != DirReceiveOnly)
	c.s.WakePipe(r.p.id)
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

// SetProbation sets the base of the pipe's probation; 0 disables it, a
// negative value returns to the session's default.
func (p *Pipe) SetProbation(d time.Duration) error {
	v := core.Duration(d)
	if d < 0 {
		v = -1
	}
	return p.set(core.PipeSettings{Probation: &v})
}

// Revalidate sends a PIPE_PING at once; the pipe takes no new stream data
// until it is acknowledged, while another pipe is usable.
func (p *Pipe) Revalidate() error {
	c := p.c
	c.lock()
	defer c.unlock()
	return apiErr(c.s.RevalidatePipes(p.id, false, c.clk.now()))
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

// PipeStats is a snapshot of one attachment, taken under the session lock.
type PipeStats struct {
	ID           uint64
	Label        string
	State        string // joining, active, suspect, draining, failed, closed
	InState      string // opening, open, ended
	AttachedFor  time.Duration
	SuspectCount uint64
	FailureCount uint64

	SendPriority       uint64
	SendPrioritySource string // local, peer, default
	StandbySource      string
	ReceivePriority    uint64
	StatusSequence     uint64
	PeerRequest        *PeerRequest
	Weight             uint64
	ProbationUntil     time.Time // zero outside probation

	SmoothedRTT  time.Duration // estimate
	MinRTT       time.Duration // estimate
	RTTVar       time.Duration // estimate
	OWD          time.Duration // estimate
	DeliveryRate float64       // bytes/s, estimate
	RTTSource    string        // ack, kernel, ping

	QueuedBytes   uint64
	QueueDelay    time.Duration // estimate
	BytesInFlight uint64

	BytesSent          uint64
	BytesReceived      uint64
	BytesAcked         uint64
	RecordsSent        uint64
	RecordsReceived    uint64
	BytesReinjected    uint64
	BytesOpportunistic uint64
	PingsSent          uint64

	SinceLastAck     time.Duration
	SinceLastReceive time.Duration
	Lanes            int

	Direction      Direction
	Revalidating   bool
	Standby        bool // with StandbySource
	ReceiveStandby bool
	Eligible       bool
	Writable       bool
	Saturated      bool
}

// PeerRequest is the peer's latest PIPE_STATUS request for the local
// outbound direction.
type PeerRequest struct {
	Priority uint64
	Standby  bool
}

// Stats returns the pipe's snapshot.
func (p *Pipe) Stats() (PipeStats, error) {
	c := p.c
	c.lock()
	defer c.unlock()
	pi, ok := c.s.PipeInfo(p.id)
	if !ok {
		return PipeStats{}, ErrPipeGone
	}
	return c.pipeStats(pi), nil
}

// Pipes returns the handles of the session's current attachments. A
// session without multipath has pipe 0.
func (c *Conn) Pipes() []*Pipe {
	c.lock()
	defer c.unlock()
	ids := c.s.PipeIDs(nil)
	out := make([]*Pipe, len(ids))
	for i, id := range ids {
		out[i] = &Pipe{c: c, id: id}
	}
	return out
}

// PipeStats returns a snapshot of every attachment, taken in one critical
// section. A session without multipath reports pipe 0.
func (c *Conn) PipeStats() []PipeStats {
	c.lock()
	defer c.unlock()
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
	low := func(s string) string {
		b := []byte(s)
		for i, x := range b {
			if x >= 'A' && x <= 'Z' {
				b[i] = x + 'a' - 'A'
			}
		}
		return string(b)
	}
	st := PipeStats{ID: pi.ID, Label: c.labels[pi.ID], State: low(pi.OutState), InState: low(pi.InState),
		Revalidating: pi.Revalidating, AttachedFor: time.Duration(pi.AttachedFor),
		SuspectCount: pi.SuspectCount, FailureCount: pi.FailureCount, SendPriority: pi.SendPriority,
		SendPrioritySource: pi.PrioritySource, Standby: pi.Standby, StandbySource: pi.StandbySource,
		ReceivePriority: pi.ReceivePriority, ReceiveStandby: pi.ReceiveStandby, StatusSequence: pi.StatusSeq,
		Weight: pi.Weight, Eligible: pi.Eligible, SmoothedRTT: time.Duration(pi.SmoothedRTT),
		MinRTT: time.Duration(pi.MinRTT), RTTVar: time.Duration(pi.RTTVar), OWD: time.Duration(pi.OWD),
		DeliveryRate: pi.DeliveryRate, RTTSource: pi.RTTSource, QueuedBytes: pi.QueuedBytes,
		Writable: pi.Writable, Saturated: pi.Saturated, BytesInFlight: pi.BytesInFlight,
		BytesSent: pi.BytesSent, BytesReceived: pi.BytesReceived, BytesAcked: pi.BytesAcked,
		RecordsSent: pi.RecordsSent, RecordsReceived: pi.RecordsReceived, BytesReinjected: pi.BytesReinjected,
		BytesOpportunistic: pi.BytesOpportunistic, PingsSent: pi.PingsSent,
		SinceLastAck: time.Duration(pi.SinceLastAck), SinceLastReceive: time.Duration(pi.SinceLastReceive),
		Lanes: pi.Lanes}
	switch {
	case pi.HasOut && pi.HasIn:
		st.Direction = DirBidirectional
	case pi.HasOut:
		st.Direction = DirSendOnly
	default:
		st.Direction = DirReceiveOnly
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
	return st
}
