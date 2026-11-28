package zmux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/shaping"
)

// RecordMode says who protects a carrier's records. The order is
// from strongest to weakest.
type RecordMode uint8

// The record modes; the values are those of the Mode field of a Join
// (secure-carrier Section 11).
const (
	// RecordSealed protects records with the handshake's keys.
	RecordSealed RecordMode = 0
	// RecordBound leaves records to the carrier and binds its exporter into
	// the handshake.
	RecordBound RecordMode = 1
	// RecordTrusted leaves records to the deployment; both ends configure it
	// explicitly.
	RecordTrusted RecordMode = 2
	// RecordUnmanaged is a first pipe without the secure carrier. It is only
	// reported: no Join has this mode.
	RecordUnmanaged RecordMode = 3
)

// The record modes share the values of the join's modes.
var (
	_ = [1]struct{}{}[int(join.Sealed)-int(RecordSealed)]
	_ = [1]struct{}{}[int(join.Bound)-int(RecordBound)]
	_ = [1]struct{}{}[int(join.Trusted)-int(RecordTrusted)]
)

// String returns "sealed", "bound", "trusted" or "unmanaged".
func (m RecordMode) String() string {
	if m == RecordUnmanaged {
		return "unmanaged"
	}
	return join.Mode(m).String()
}

// errRefusalSent is a join refused with a Reply that was written: its
// carrier is closed gracefully so that the joiner reads the refusal.
var (
	errRefusalSent      = fmt.Errorf("%w: refusal sent", errRefused)
	errJoinDenied       = fmt.Errorf("%w: join denied by policy", errRejected)
	errPeerJoinCapacity = fmt.Errorf("%w: pending peer Join capacity", errRefused)
)

// RecordPolicyInput describes a carrier to RecordPolicy and AllowRecordMode.
type RecordPolicyInput struct {
	// Label is the carrier's PipeIO.Label.
	Label string
	// HasExporter reports that the carrier has a keying material exporter.
	HasExporter bool
	// Secure reports that the carrier declares the Bound contract
	// (CarrierSecure).
	Secure bool
	// IsFirstPipe reports that the carrier is the session's first pipe.
	IsFirstPipe bool
}

// RecordPolicy chooses the record mode of a carrier the local endpoint joins.
type RecordPolicy func(c RecordPolicyInput) RecordMode

// AllowRecordMode is the acceptor's check of a Hello's mode; peer is the session's
// host value (AdmittedAs).
type AllowRecordMode func(c RecordPolicyInput, m RecordMode, peer any) bool

// JoinAuthorization is a value snapshot of an authenticated Hello. Direction,
// ReceiveSource and SendSource are relative to this endpoint.
type JoinAuthorization struct {
	Principal                 any
	ReceiveSource, SendSource CarrierHalfSource
	FirstBatch                JoinFirstBatch
	AuthGeneration            uint64
	Session                   SessionID
	PipeID                    PipeID
	Epoch                     uint64
	Direction                 Direction
	RecordMode                RecordMode
}

// JoinFirstBatch describes the structure of an authenticated Hello's first
// batch. The protocol checks flow control and stream state before delivery.
type JoinFirstBatch struct {
	EncodedBytes, StreamFrames, StreamBytes int
	Malformed                               bool
}

// JoinVerdict is the host's decision on an authenticated Join.
type JoinVerdict uint8

const (
	// JoinDeny denies the Join. It is the zero verdict.
	JoinDeny JoinVerdict = iota
	// JoinPermit allows the Join to proceed under its captured generation.
	JoinPermit
)

// JoinAuthorizer runs once per authenticated Join outside session locks.
// An error or an unrecognized verdict denies the Join.
type JoinAuthorizer func(context.Context, JoinAuthorization) (JoinVerdict, error)

// CarrierSecure is implemented by a carrier half whose carrier meets the Bound
// contract (the carrier protects the records end to end: confidentiality and
// integrity). An exporter alone is no such declaration.
type CarrierSecure interface{ CarrierSecure() bool }

// CarrierRecordLimit is implemented by a carrier half that bounds a record's
// plaintext. It is local and applied beside the peer's max_record_size.
type CarrierRecordLimit interface{ MaxRecord() int }

// CarrierRecordMode is implemented by the halves of a carrier transform that ran an
// Establish: the first pipe is then on the secure carrier with this mode, and the
// session's secret comes from the half's exporter.
type CarrierRecordMode interface{ RecordMode() RecordMode }

// CarrierPeer is implemented by the halves of a carrier transform that authenticated the
// peer: its value is the session's host value in place of Decision.Value (AdmittedAs).
type CarrierPeer interface{ CarrierPeer() any }

func carrierPeer(p PipeIO) any {
	for _, x := range []any{p.W, p.R} {
		if s, ok := x.(CarrierPeer); ok {
			if v := s.CarrierPeer(); v != nil {
				return v
			}
		}
	}
	return nil
}

// IsCarrierSecure reports the Secure declaration of a carrier; a split carrier
// needs it on both halves.
func IsCarrierSecure(p PipeIO) bool {
	decl := func(x any) bool {
		s, ok := unprefixed(x).(CarrierSecure)
		return ok && s.CarrierSecure()
	}
	if p.Carrier == CarrierSplit {
		return decl(p.R) && decl(p.W)
	}
	return decl(p.R) || decl(p.W)
}

func secureFirstMode(p PipeIO) (RecordMode, bool) {
	for _, x := range []any{p.W, p.R} {
		if s, ok := x.(CarrierRecordMode); ok {
			return s.RecordMode(), true
		}
	}
	return 0, false
}

func carrierRecordLimit(p PipeIO) int {
	lim := p.MaxRecord
	for _, x := range []any{p.W, p.R} {
		if s, ok := x.(CarrierRecordLimit); ok {
			if n := s.MaxRecord(); n > 0 && (lim == 0 || n < lim) {
				lim = n
			}
		}
	}
	return lim
}

// joinCarrierInfo is the description AllowRecordMode and RecordPolicy see.
func joinCarrierInfo(p PipeIO, first bool) RecordPolicyInput {
	_, exp := Exporter(p)
	return RecordPolicyInput{HasExporter: exp, Secure: IsCarrierSecure(p), Label: p.Label, IsFirstPipe: first}
}

// joinBinding reads the Binding of a Bound carrier; a
// split carrier cannot use Bound at present.
func joinBinding(p PipeIO) ([]byte, error) {
	if p.Carrier == CarrierSplit {
		return nil, fmt.Errorf("%w: Bound on a split carrier", ErrInvalidConfig)
	}
	return ExportBoundCarrier(p, join.ExporterLabel)
}

// ExportBoundCarrier reads a protocol's binding exporter from a secure
// carrier. A split protocol calls it for each half it binds.
func ExportBoundCarrier(p PipeIO, label string) ([]byte, error) {
	if !IsCarrierSecure(p) {
		return nil, fmt.Errorf("%w: %w: Bound on a carrier without a Secure declaration", ErrInvalidConfig, join.ErrInvalidConfig)
	}
	e, ok := Exporter(p)
	if !ok {
		return nil, fmt.Errorf("%w: %w: Bound on a carrier without an exporter", ErrInvalidConfig, join.ErrInvalidConfig)
	}
	b, err := e.ExportKeyingMaterial(label, nil, join.BindingLen)
	if err != nil {
		return nil, fmt.Errorf("zmux: carrier exporter: %w", err)
	}
	if len(b) != join.BindingLen {
		return nil, fmt.Errorf("%w: %w: invalid carrier exporter length", ErrInvalidConfig, join.ErrInvalidConfig)
	}
	return b, nil
}

// writeJoin submits one final, immutable unit and settles its reservation
// at the carrier's reported prefix. The caller owns the unit until return.
func (c *Conn) writeJoin(p PipeIO, b []byte, plan *core.JoinWrite) error {
	var n int
	var err error
	if len(b) != 0 {
		if p.Framing == FramingRecords {
			err = observeWrite(func() error { return carrierio.WriteUnit(p.W, true, b) })
			if err == nil {
				n = len(b)
			}
		} else {
			n, err = writeSuffix(p.W, b, nil)
		}
	}
	_, unknown := errors.AsType[*unknownWriteError](err)
	c.locked(func() { c.s.SetNow(c.clk.now()); plan.Complete(n, unknown) })
	return err
}

// unitReader reads what a handshake step asks for from a carrier and keeps the rest.
type unitReader struct {
	end  error
	buf  []byte
	p    PipeIO
	used int
}

// more reads at least once and appends. A message carrier gives one unit.
func (u *unitReader) more() error {
	if u.end != nil {
		return u.end
	}
	var err error
	if u.p.Framing == FramingRecords {
		var msg ReceivedMessage
		msg, err = readMessage(u.p.R, MaxRecordSize)
		u.end = msg.end
		u.buf = msg.TakeBytes()
	} else {
		before := len(u.buf)
		u.buf, err = carrierio.ReadStreamUnit(u.p.R, u.buf, 2*MaxUntakenRead, 4096)
		if len(u.buf) > before {
			u.end, err = err, nil
		}
	}
	return err
}

func (u *unitReader) handover(used int) PipeIO {
	if u.p.Framing == FramingRecords {
		return withPrefix(withMessageEnd(u.p, u.end), u.buf[used:])
	}
	return withBytePrefix(u.p, u.buf, u.used+used, u.end)
}

// joinCarrier runs the joiner's handshake on a JOINING attachment and starts the pipe.
// Nothing read from the carrier reaches the session before the
// Reply verifies; a failure fails only the attachment.
func (c *Conn) joinCarrier(ctx context.Context, pd *pipeDrv, p PipeIO, dir Direction) error {
	flight, err := c.reserveReceiveFlight(p.R)
	if err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	defer func() { flight.release() }()
	ctx, cancel := context.WithCancel(ctx) // AddPipe set the join deadline
	defer cancel()
	stopEnd := context.AfterFunc(c.ctx, cancel) // the session's end ends the handshake
	defer stopEnd()
	stop := context.AfterFunc(ctx, func() { closePipe(p) })
	defer stop()
	var msg string
	held := core.NewSignal() // woken when what holds the join changes
	defer func() {
		c.locked(func() {
			c.s.CancelJoinWait(held)
		})
	}()
	for {
		var m string
		var ready bool
		var err, closed error
		c.locked(func() {
			if c.closing {
				closed = c.errLocked()
				return
			}
			c.s.SetNow(c.clk.now())
			held.Drain()
			m, ready, err = c.s.JoinPrepare(pd.id, held)
		})
		if closed != nil {
			return closed
		}
		if err != nil {
			return apiErr(err)
		}
		if ready {
			msg = m
			break
		}
		// The cap or the refusal backoff holds the join.
		select {
		case <-held.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
		}
	}
	plan, err := c.joinPlan(ctx, p, msg)
	if err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	var hello []byte
	var helloOwn uint64
	var reader *core.JoinReplyReader
	var closed error
	c.locked(func() {
		if c.closing {
			closed = c.errLocked()
			return
		}
		c.s.SetNow(c.clk.now())
		hello, helloOwn, err = c.s.JoinHello(pd.id, core.Framing(p.Framing), p.WriteOverhead, &plan)
		reader = c.s.JoinReplyReader(pd.id)
	})
	if closed != nil {
		return closed
	}
	if err != nil {
		return apiErr(err)
	}
	if err := c.writeJoin(p, hello, &plan); err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	u := &unitReader{p: p}
	if err := u.more(); err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	for {
		var st core.JoinStep
		var mode join.Mode
		var terminal bool
		c.locked(func() {
			now := c.clk.now()
			o := pd.end.join
			terminal = o.attempt != nil && o.attempt.done
			if terminal {
				st = reader.Observe(u.buf[u.used:])
			} else {
				st = c.s.VerifyReply(pd.id, u.buf[u.used:], now)
			}
			if o.attempt != nil && (st.Status == core.JoinProven || st.Status == core.JoinDeclined) {
				o.attempt.view.Attachments[o.index].ReplyAt = c.clk.wall(now)
				if st.Status == core.JoinProven {
					o.attempt.view.Attachments[o.index].ReplyAccepted = true
					o.attempt.observeHandshakeLocked()
				}
				c.s.NotifyCapacity()
			}
			if terminal {
				return
			}
			if st.Status == core.JoinDeclined {
				_ = pd.end.join.settleLocked(ErrJoinRefused)
			}
			mode = c.s.JoinMode(pd.id)
		})
		if terminal && st.Status != core.JoinMore {
			return context.Canceled
		}
		switch st.Status {
		case core.JoinProven:
			var plan core.JoinWrite
			if mode != join.Bound {
				var err error
				if plan, err = c.joinPlan(ctx, p, core.JoinMsgConfirm); err != nil {
					c.locked(func() {
						c.s.FailJoin(pd.id, err)
					})
					return c.joinIOFailed(ctx, pd.id, err)
				}
			}
			var confirm []byte
			var res *join.Result
			var err error
			c.locked(func() {
				c.s.SetNow(c.clk.now())
				confirm, res, err = c.s.JoinFinish(pd.id, p.WriteOverhead, &plan)
			})
			if err != nil {
				closePipe(p)
				return fmt.Errorf("zmux: pipe %d not joined: %w", pd.id, ErrPipeGone)
			}
			// The confirmation goes before any record: the writer has not started.
			if err := c.writeJoin(p, confirm, &plan); err != nil {
				return c.joinIOFailed(ctx, pd.id, err)
			}
			q := u.handover(st.N)
			if res.Mode == join.Sealed {
				// The acceptor learns the limit is too small only here, after its
				// Reply went out: the dialer checks before it sends its Hello
				// (the pipe is refused, which the dialer sees as a failed Join).
				if err := sealedLimitErr(p, sealOverhead(p.Framing)); err != nil {
					return c.joinIOFailed(ctx, pd.id, err)
				}
				q = sealResult(q, res)
			}
			var carrier any
			var maxRec int
			if !safeLog(c.cfg.Logger, "carrier properties", func() { carrier, _ = carrierIdentity(q); maxRec = carrierRecordLimit(q) }) {
				return c.joinIOFailed(ctx, pd.id, errRejected)
			}
			hc := newHalfCloser(q)
			// The handover: the bound must not close the carrier once the
			// session has it; one that already did failed the join.
			if !stop() {
				return c.joinIOFailed(ctx, pd.id, ctx.Err())
			}
			// The session may have ended, or the attachment failed, while the
			// Confirm was written.
			var gone error
			c.locked(func() {
				if err := ctx.Err(); err != nil {
					gone = err
					return
				}
				if ended := c.closing || c.s.State() >= core.StateClosing; ended || pd.retiring || pd.dead || c.pipes[pd.id] != pd || !c.joinedAlive(pd.id) {
					gone = fmt.Errorf("zmux: pipe %d not joined: %w", pd.id, ErrPipeGone)
					if ended {
						gone = c.errLocked()
					}
					return
				}
				pd.hc, pd.framing, pd.maxRec = hc, q.Framing, maxRec
				c.s.ConfirmJoinOwnBytes(helloOwn, c.clk.now())
				c.startPipeLocked(pd, dir != DirectionSendOnly, dir != DirectionReceiveOnly, carrier, &flight)
			})
			if gone != nil {
				closePipe(q)
				return gone
			}
			return nil
		case core.JoinDeclined:
			closePipe(p)
			return ErrJoinRefused
		case core.JoinFailedStep:
			closePipe(p)
			return fmt.Errorf("zmux: pipe %d not joined: %w", pd.id, ErrPipeGone)
		case core.JoinMore:
			if u.p.Framing == FramingRecords {
				u.buf = u.buf[:0]
			} else {
				u.used += st.N
			}
		}
		if err := u.more(); err != nil {
			return c.joinIOFailed(ctx, pd.id, err)
		}
	}
}

// joinedAlive reports that a pipe whose Join finished is still attached (under the
// lock).
func (c *Conn) joinedAlive(id uint64) bool {
	out, in, ok := c.s.PipeState(id)
	if !ok || out == core.OutFailed || out == core.OutClosed || out == core.OutRetired {
		return false
	}
	return in != core.InEnded && in != core.InRetired
}

// joinPlan asks the handshake shaper for the plan of a Join message, applies the
// session's limits and waits out the delay, outside every lock. The wait ends with
// ctx or the session.
func (c *Conn) joinPlan(ctx context.Context, p PipeIO, msg string) (core.JoinWrite, error) {
	var plan core.JoinWrite
	c.lock()
	hook := c.handshakeShaper
	if hook != nil && c.handshakeChange == nil {
		c.handshakeChange = make(chan struct{})
	}
	changed := c.handshakeChange
	c.unlockPeek()
	if hook == nil {
		return plan, nil
	}
	kind := ""
	if p.Carrier != 0 {
		kind = p.Carrier.String()
	}
	planned, ok := shaping.Ask(hook, &shaping.HandshakeView{Kind: kind, Handshake: "join", Message: msg}, &plan.UnitPlan)
	var wait *core.HandshakeWait
	c.locked(func() {
		c.s.SetNow(c.clk.now())
		if c.handshakeChange != changed {
			plan.UnitPlan = shaping.UnitPlan{}
			return
		}
		plan.Changed = changed
		if !ok {
			c.s.JoinHookFault()
		} else if !planned {
			c.s.JoinUnplanned()
		}
		l := c.s.ShapeLimitsInForce()
		shaping.ApplyLimits(&plan.UnitPlan, &l)
		if plan.Delay > 0 {
			wait = c.s.BeginHandshakeWait(plan.Delay)
		}
	})
	if wait != nil {
		defer c.locked(func() { c.s.FinishHandshakeWait(wait, c.clk.now()) })
		ready := make(chan struct{})
		timer := c.clk.afterFunc(c.clk.until(wait.Deadline()), func() { close(ready) })
		defer timer.Stop()
		select {
		case <-ready:
		case <-changed:
			plan.UnitPlan = shaping.UnitPlan{}
		case <-ctx.Done():
			return plan, ctx.Err()
		case <-c.done:
			return plan, c.closedErr()
		}
	}
	return plan, nil
}

// joinIOFailed fails a JOINING attachment after an error of its carrier.
func (c *Conn) joinIOFailed(ctx context.Context, id uint64, err error) error {
	if c.ctx.Err() != nil {
		return c.closedErr()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return fmt.Errorf("zmux: pipe %d not joined: %w: %w", id, ErrPipeGone, err)
}

func joinFraming(p PipeIO) join.Framing {
	if p.Framing == FramingRecords {
		return join.Message
	}
	return join.Stream
}

// helloWithoutBinding counts a Hello in Bound on a carrier that has no Binding.
func (c *Conn) helloWithoutBinding() {
	c.lock()
	defer c.unlock()
	c.s.HelloWithoutBinding()
}

// heldJoin is an Unconfirmed carrier as the session keeps it, for the
// eviction by a newer Hello.
type heldJoin struct {
	out    *joinOutcome
	cancel context.CancelFunc // of the join's own bound
	p      PipeIO
}

// evict ends the join as one the session refused to keep: the bound's
// end is not the peer's fault (errRefused, counted in ListenerStats.Refused),
// and the carrier is closed at once.
func (h *heldJoin) evict() {
	h.cancel()
	closePipe(h.p)
}

// acceptHello authenticates and confirms a Hello that named this session.
func (c *Conn) acceptHello(ctx context.Context, p PipeIO, h join.Hello, binding []byte, admitted any, wait *attachment) error {
	// Waiting for establishment or the join keys is bounded even when the
	// handshake is not (Config.joinWaitTimeout).
	parent := ctx
	deadline := c.clk.wall(c.clk.now().Add(core.Duration(c.cfg.joinTimeout())))
	ctx, cancel := timeshim.WithDeadline(ctx, c.clk.source(), deadline)
	defer cancel()
	stopEnd := context.AfterFunc(c.ctx, cancel) // the session's end ends the handshake
	defer stopEnd()
	stopCarrier := context.AfterFunc(ctx, func() { closePipe(p) })
	defer stopCarrier()
	// The setup owner watches the listener and parent. This bound also
	// interrupts the provisional carrier during the confirmation handshake.
	var pj *core.PeerJoin
	var authorize JoinAuthorizer
	var authorizedWork bool
	for pj == nil {
		var res core.JoinResult
		closing := false
		c.locked(func() {
			if closing = c.closing; closing {
				return
			}
			c.s.SetNow(c.clk.now())
			pj, res = c.s.OpenHello(h, core.Framing(p.Framing), binding)
			if res == core.JoinAccepted {
				authorize = c.authorizeJoin
				authorizedWork = c.s.BeginPeerAuthorization(pj)
			}
		})
		if closing {
			return errRefused
		}
		switch res {
		case core.JoinAccepted:
		case core.JoinHold:
			if err := c.waitEstablished(ctx, nil); err != nil {
				return err
			}
		case core.JoinKeyWait:
			if err := c.waitJoinKeys(ctx); err != nil {
				return err // the caller closes p silently
			}
		case core.JoinUnknownSuite:
			return errUnknownSuite
		case core.JoinClose, core.JoinRefused, core.JoinAuthorizationChanged:
			return errRejected
		}
	}
	if !authorizedWork {
		return errPeerJoinCapacity
	}
	defer c.locked(func() { c.s.EndPeerAuthorization(pj) })
	authInput := JoinAuthorization{
		AuthGeneration: pj.AuthGeneration(), Session: c.id, Principal: c.admitted,
		PipeID: PipeID(pj.Pipe.ID), Epoch: pj.Epoch(), RecordMode: RecordMode(pj.Mode),
		ReceiveSource: p.ReceiveSource, SendSource: p.SendSource,
	}
	switch pj.Pipe.Direction {
	case 0:
		authInput.Direction = DirectionReceiveOnly
	case 1:
		authInput.Direction = DirectionSendOnly
	}
	authInput.FirstBatch = JoinFirstBatch(pj.FirstBatchInfo())
	if err := c.authorizeHello(ctx, p, authorize, authInput); err != nil {
		return err
	}
	out := c.newJoinOutcome(parent, authInput.PipeID, true, deadline)
	out.cancellation = func() error { return wait.cancellation(parent) }
	held := &heldJoin{out: out, p: p, cancel: cancel}
	stop := interruptOnCancel(ctx, func() { c.locked(func() { _ = out.settleLocked(ctx.Err()) }) })
	defer stop()
	err := hostCall(c.cfg.Logger, "authorized join", func() error {
		return c.confirmHello(ctx, p, pj, admitted, wait, held, out, h, stopCarrier)
	})
	c.locked(func() {
		if !out.settled {
			reason := err
			if err != nil {
				reason = fmt.Errorf("%w: %w", ErrJoinRefused, err)
			}
			_ = out.settleLocked(reason)
		}
	})
	return err
}

func (c *Conn) authorizeHello(ctx context.Context, p PipeIO, authorize JoinAuthorizer, req JoinAuthorization) error {
	if p.R == nil || p.W == nil {
		return errRejected
	}
	mode := req.RecordMode
	allowed := mode == RecordSealed
	if allow := c.cfg.AllowRecordMode; allow != nil {
		allowed = false
		info := joinCarrierInfo(p, false)
		safeLog(c.cfg.Logger, "Config.AllowRecordMode", func() { allowed = allow(info, mode, req.Principal) })
	}
	if !allowed || mode == RecordSealed && sealedLimitErr(p, sealOverhead(p.Framing)) != nil {
		return errRejected
	}
	if ctx.Err() != nil {
		return errRefused
	}
	if authorize == nil {
		return nil
	}
	var allowedJoin bool
	safeLog(c.cfg.Logger, "SessionPolicy.AuthorizeJoin", func() {
		verdict, err := authorize(ctx, req)
		allowedJoin = err == nil && verdict == JoinPermit
	})
	if !allowedJoin {
		return errJoinDenied
	}
	return nil
}

func (c *Conn) confirmHello(ctx context.Context, p PipeIO, pj *core.PeerJoin, admitted any, wait *attachment, held *heldJoin, out *joinOutcome, h join.Hello, stopCarrier func() bool) error {
	flight, err := c.reserveReceiveFlight(p.R)
	if err != nil {
		return err
	}
	defer func() { flight.release() }()
	var edge *nestEdge
	defer func() { edge.drop() }()
	ended := func(err error) error {
		if ctx.Err() != nil {
			return errRefused
		}
		return err
	}
	dirIn, dirOut := pj.Pipe.Direction != 1, pj.Pipe.Direction != 0
	plan, err := c.joinPlan(ctx, p, core.JoinMsgReply)
	if err != nil {
		return errRefused // the wait was cancelled or the session ended
	}
	var outer outerRef
	var physical core.CarrierKeys
	if !safeLog(c.cfg.Logger, "CarrierConn", func() {
		_, outer = carrierIdentity(p)
		physical = physicalCarrierKeys(p)
	}) {
		return errRejected
	}
	var reply []byte
	var res core.JoinResult
	var evicted []any
	closing := false
	c.locked(func() {
		l := wait.listener
		l.mu.RLock()
		defer l.mu.RUnlock()
		c.s.SetNow(c.clk.now())
		if !out.settled {
			if reason := out.reasonAtLocked(nil, c.clk.wall(c.s.Now())); reason != nil {
				out.publishLocked(reason)
			}
		}
		if closing = out.settled; closing {
			return
		}
		if pj.AuthGeneration() != c.s.SessionPolicy().AuthGeneration {
			res = core.JoinAuthorizationChanged
			return
		}
		edge, err = nestPipe(c, outer)
		reply, res, evicted = c.s.ClaimHello(pj, core.Carrier{HasIn: p.R != nil, HasOut: p.W != nil,
			Handoff: held, Refuse: err != nil || !c.s.CanAcquireCarriers(physical)}, p.WriteOverhead, h.WireLen(joinFraming(p)), &plan)
		for _, token := range evicted {
			_ = token.(*heldJoin).out.settleLocked(ErrJoinEvicted)
		}
		if res == core.JoinRefused {
			if wait.commitLocked(ctx, setupPending, setupRejecting) != nil {
				closing = true
				return
			}
			_ = out.settleLocked(ErrJoinRefused)
		}
	})
	defer c.locked(func() { plan.Complete(0, false) })
	if res == core.JoinAuthorizationChanged {
		return ErrJoinAuthorizationChanged
	}
	if closing {
		return errRefused
	}
	for _, token := range evicted {
		token.(*heldJoin).evict()
	}
	if res == core.JoinAccepted {
		defer c.locked(func() { c.s.DropPeerJoin(pj) })
	}
	if err := wait.acceptHead(ctx); err != nil {
		return err
	}
	markTaken(p)
	if res == core.JoinClose {
		return errRejected
	}
	if err := c.writeJoin(p, reply, &plan); err != nil {
		return ended(errRejected)
	}
	if res == core.JoinRefused {
		return errRefusalSent
	}
	// The acceptor's records wait for Confirmed.
	u := &unitReader{p: p}
	var used int
	if pj.Mode != join.Bound {
		if err := u.more(); err != nil {
			return ended(errRejected)
		}
	}
	for {
		n, _, done, err := pj.ReadConfirm(u.buf)
		if err != nil {
			return errRejected
		}
		if done {
			used = n
			break
		}
		if err := u.more(); err != nil {
			return ended(errRejected)
		}
	}
	result, err := pj.Result()
	if err != nil {
		return errRejected
	}
	q := u.handover(used)
	if pj.Mode == join.Sealed {
		q = sealResult(q, result)
	}
	id := pj.Pipe.ID
	confirmed := false
	attachment, err := c.preparePipe(id, q, p, multiLowWater)
	if err != nil {
		return errRejected
	}
	pd, ci := attachment.pd, attachment.info
	// The handover: from here p is the session's, so the bound must not
	// close it any more; one that already did failed the join.
	if !stopCarrier() {
		return errRefused
	}
	if err := c.waitPublication(ctx); err != nil {
		return errRefused
	}
	c.locked(func() {
		if out.settled {
			return
		}
		if !wait.commitCarrier(ctx, func() bool {
			c.s.SetNow(c.clk.now())
			if reason := out.reasonAtLocked(nil, c.clk.wall(c.s.Now())); reason != nil {
				out.publishLocked(reason)
				return false
			}
			res = c.s.ConfirmJoin(pj, core.Carrier{HasIn: dirIn, HasOut: dirOut, In: ci, Out: ci,
				OutSig: &pd.writer.poll, Kind: uint8(p.Carrier), Refuse: !c.s.CanAcquireCarriers(physical)})
			return res == core.JoinAccepted
		}) {
			if res == core.JoinAuthorizationChanged {
				_ = out.settleLocked(ErrJoinAuthorizationChanged)
			} else {
				_ = out.settleLocked(ErrJoinRefused)
			}
			return
		}
		pd.admitted = admitted
		out.holdLocked(id, pd.end)
		pd.nest, edge = edge, nil // the session's now: its pipe's end drops the edge
		wait.attach(pd)
		c.startPipeLocked(pd, dirIn, dirOut, attachment.carrier, &flight)
		out.publishLocked(nil)
		confirmed = true
	})
	if res == core.JoinAuthorizationChanged {
		return fmt.Errorf("%w: %w", errRefused, ErrJoinAuthorizationChanged)
	}
	if !confirmed {
		return errRefused
	}
	return nil
}

// PairedPipe makes one carrier of two unidirectional ones, the local endpoint's
// uplink and downlink. The attachment fails as one.
func PairedPipe(up, down PipeIO) PipeIO {
	return PipeIO{R: down.R, W: up.W, Framing: up.Framing, Carrier: CarrierSplit, Label: up.Label, FateGroup: up.FateGroup,
		WriteOverhead: up.WriteOverhead, MaxRecord: up.MaxRecord, Variant: up.Variant,
		ReceiveSource: down.ReceiveSource, SendSource: up.SendSource}
}

// ---- Sealed records ----

const sealedLen = 2

// SealedMaxRecord is the largest Sealed record plaintext.
const SealedMaxRecord = join.MaxMessage - 16

// sealResult wraps a carrier with the records of a finished Sealed Join.
func sealResult(p PipeIO, r *join.Result) PipeIO {
	q := SealRecords(p, r.Sealer, r.Opener, RecordSealed, exportResult{r})
	q.own = p.own // the records are a view of p: p's closer ends them
	return q
}

type exportResult struct{ r *join.Result }

func (e exportResult) ExportKeyingMaterial(label string, ctx []byte, n int) ([]byte, error) {
	return e.r.Export(label, ctx, n)
}

// RecordSealer seals the records one direction of a carrier sends, one AEAD
// message per record, with nonces of its own.
type RecordSealer interface {
	// Overhead is the bytes Seal adds to a plaintext.
	Overhead() int
	// Seal appends the sealed plaintext to dst.
	Seal(dst, plaintext []byte) ([]byte, error)
}

// RecordOpener opens the records one direction of a carrier receives, as a
// RecordSealer of the peer sealed them.
type RecordOpener interface {
	// OpenInPlace authenticates a ciphertext and leaves its plaintext in
	// the first len(ciphertext)-16 bytes. An error invalidates the carrier.
	OpenInPlace(ciphertext []byte) error
}

// SealRecords returns p as a carrier of Sealed records: a message carrier
// with one zmux record in each AEAD message, sealed by s and opened by o.
// mode is what the carrier reports as its RecordMode and exp its exporter
// (nil: none). It offers only what its halves implement here, so the
// carrier's lanes, datagram channel and exporter are hidden. It is the one
// implementation of Sealed records, for a Join and for an Establish of the
// secure carrier. It does not check the carrier's record limit against the
// Sealed floor: CheckSealedCarrier does, before the handshake.
func SealRecords(p PipeIO, s RecordSealer, o RecordOpener, mode RecordMode, exp KeyingMaterialExporter) PipeIO {
	r := &sealedReader{lower: p, o: o, msgs: p.Framing == FramingRecords, exp: exp, mode: mode}
	w := &sealedWriter{lower: p, s: s, msgs: p.Framing == FramingRecords, exp: exp, mode: mode}
	overhead := p.WriteOverhead + s.Overhead()
	if p.Framing != FramingRecords {
		overhead += sealedLen
	}
	return PipeIO{R: r, W: w, Framing: FramingRecords, Carrier: p.Carrier, Protocol: p.Protocol, Label: p.Label, FateGroup: p.FateGroup,
		WriteOverhead: overhead, MaxRecord: sealedRecordLimit(p, overhead-p.WriteOverhead), Variant: p.Variant, HandshakeWrites: p.HandshakeWrites,
		ReceiveSource: p.ReceiveSource, SendSource: p.SendSource}
}

// sealedRecordLimit is the record plaintext limit of p's records sealed
// with seal bytes added to each: a record limit of the lower carrier bounds
// a sealed message, so the plaintext is that less the sealing; 0: none.
func sealedRecordLimit(p PipeIO, seal int) int {
	lim := carrierRecordLimit(p)
	if lim == 0 {
		return 0
	}
	return max(lim-seal, 1)
}

// sealOverhead is the bytes a Sealed record adds to its plaintext on a
// carrier of framing: the AEAD tag, and a Size field on a byte stream.
func sealOverhead(f Framing) int {
	if f == FramingRecords {
		return sealTag
	}
	return sealTag + sealedLen
}

// sealTag is the AEAD tag every sealed message ends with.
const sealTag = 16

// CheckSealedCarrier reports whether p, the carrier of a Sealed pipe, has the
// record limit Sealed needs once the AEAD sealing is added to each record
// (ErrInvalidConfig naming the limit). A handshake that ends in SealRecords
// calls it before it begins, so that a carrier too small for Sealed fails
// before any handshake byte is written, not after.
//
//goland:noinspection GoUnusedExportedFunction // used by the seal module
func CheckSealedCarrier(p PipeIO) error { return sealedLimitErr(p, sealOverhead(p.Framing)) }

// sealedLimitErr reports a lower carrier whose record limit, less seal bytes
// of sealing, falls below MinMaxRecord (ErrInvalidConfig).
func sealedLimitErr(p PipeIO, seal int) error {
	if lim := carrierRecordLimit(p); lim != 0 && lim-seal < MinMaxRecord {
		return fmt.Errorf("%w: the carrier's record limit %d is too small for a Sealed pipe, which needs at least %d (%d plus %d bytes of sealing)",
			ErrInvalidConfig, lim, MinMaxRecord+seal, MinMaxRecord, seal)
	}
	return nil
}

type sealedReader struct {
	err   error
	o     RecordOpener
	exp   KeyingMaterialExporter
	lower PipeIO
	head  [sealedLen]byte
	msgs  bool
	mode  RecordMode
}

func (r *sealedReader) Read(_ []byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (r *sealedReader) Close() error {
	if r.lower.R != nil && !carrierio.SameHalf(r.lower.R, r.lower.W) {
		return r.lower.R.Close()
	}
	return nil
}

// ReadMessage authenticates a complete record in its final backing.
func (r *sealedReader) ReadMessage(limit int) (ReceivedMessage, error) {
	if r.err != nil {
		return ReceivedMessage{}, r.err
	}
	limit = min(limit, join.MaxMessage-sealTag)
	var record ReceivedMessage
	var raw *core.Buf
	defer func() {
		if raw != nil {
			raw.Unref()
		}
		if record.Present() {
			record.Release()
		}
	}()
	var ciphertext []byte
	var end error
	var framed int
	if r.msgs {
		m, err := readMessage(r.lower.R, limit+sealTag)
		if err != nil {
			return r.failRead(err)
		}
		record, ciphertext, end = m, m.Bytes(), m.end
		framed = len(ciphertext)
		if err := checkCiphertextLen(len(ciphertext), limit); err != nil {
			return r.failRead(&recordReadError{cause: err, bytes: uint64(framed)})
		}
	} else {
		n, err := r.ciphertextLen(limit)
		if err != nil {
			return r.failRead(err)
		}
		raw = core.GetRecvBuf(n)
		ciphertext = raw.B[:n:n]
		read, err := readSealedBytes(r.lower.R, ciphertext)
		if read < n {
			return r.failRead(&recordReadError{cause: truncatedCiphertext(err), bytes: uint64(sealedLen + read)})
		}
		framed = sealedLen + read
		end = err
	}
	if err := r.o.OpenInPlace(ciphertext); err != nil {
		return r.failRead(&recordReadError{cause: err, bytes: uint64(framed), badTag: true})
	}
	if end != nil {
		r.err = end
	}
	if raw != nil {
		span := raw.Span(0, len(ciphertext)-sealTag)
		raw = nil
		result := ownMessageSpan(span)
		result.framedBytes = uint32(framed)
		return result, end
	}
	result := record
	result.span.End -= sealTag
	result.framedBytes = uint32(framed)
	result.end = nil
	record = ReceivedMessage{}
	return result, end
}

func (r *sealedReader) failRead(err error) (ReceivedMessage, error) {
	r.err = err
	return ReceivedMessage{}, err
}

func (r *sealedReader) ciphertextLen(limit int) (int, error) {
	headed, err := readSealedBytes(r.lower.R, r.head[:])
	if headed < len(r.head) {
		if headed != 0 {
			err = &recordReadError{cause: truncatedCiphertext(err), bytes: uint64(headed)}
		}
		return 0, err
	}
	n := int(binary.BigEndian.Uint16(r.head[:]))
	if check := checkCiphertextLen(n, limit); check != nil {
		return 0, &recordReadError{cause: check, bytes: uint64(headed)}
	}
	if err != nil {
		return 0, &recordReadError{cause: truncatedCiphertext(err), bytes: uint64(headed)}
	}
	return n, nil
}

func checkCiphertextLen(n, limit int) error {
	if n < sealTag {
		return join.ErrMalformed
	}
	if n-sealTag > limit {
		return ErrMessageTooLarge
	}
	return nil
}

// readSealedBytes preserves a simultaneous terminal error even when the
// requested region is full. An unfilled region can never be published.
func readSealedBytes(r io.Reader, dst []byte) (int, error) {
	read := 0
	for read < len(dst) {
		n, err := r.Read(dst[read:])
		if n < 0 || n > len(dst)-read {
			return read, errors.New("zmux: invalid carrier read count")
		}
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}

func truncatedCiphertext(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *sealedReader) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
	if r.exp == nil {
		return nil, errors.ErrUnsupported
	}
	return r.exp.ExportKeyingMaterial(l, c, n)
}
func (r *sealedReader) RecordMode() RecordMode { return r.mode }

// ReportsFailure forwards the lower carrier's (both halves alike): sealed
// records fail when it fails.
func (r *sealedReader) ReportsFailure() bool { return reportsFailure(r.lower) }

type sealedWriter struct {
	s         RecordSealer
	exp       KeyingMaterialExporter
	closeErr  error // published by closeOnce
	writeErr  error // sticky, guarded by mu
	buf       []byte
	lower     PipeIO
	closeOnce sync.Once
	mu        sync.Mutex
	msgs      bool
	mode      RecordMode
}

func (w *sealedWriter) Write(b []byte) (int, error) {
	if err := w.WriteMessage(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteMessage seals one record and writes it in one write.
func (w *sealedWriter) WriteMessage(m []byte) error {
	return w.WriteMessages([][]byte{m})
}

// WriteMessages serializes sealing and irreversible lower writes. An error
// makes the writer unusable because subsequent nonces would skip a record.
func (w *sealedWriter) WriteMessages(ms [][]byte) error {
	return w.writeMessages(ms, nil)
}

func (w *sealedWriter) writeMessages(ms [][]byte, progress func(int)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if v := recover(); v != nil {
			w.writeErr = &unknownWriteError{cause: errors.New("zmux: sealed writer interrupted")}
			panic(v)
		}
	}()
	if w.writeErr != nil {
		return w.writeErr
	}
	for _, m := range ms {
		if len(m) > join.MaxMessage-w.s.Overhead() {
			return ErrMessageTooLarge
		}
	}
	out := w.buf[:0]
	for _, m := range ms {
		if !w.msgs {
			out = binary.BigEndian.AppendUint16(out, uint16(len(m)+w.s.Overhead()))
		}
		out, w.writeErr = w.s.Seal(out, m)
		if w.writeErr != nil {
			return w.writeErr
		}
		w.buf = out
		if w.msgs {
			w.writeErr = w.lower.W.(MessageWriter).WriteMessage(out)
			if w.writeErr != nil {
				return w.writeErr
			}
			out = out[:0]
		}
	}
	if !w.msgs && len(out) != 0 {
		n, err := writeSuffix(w.lower.W, out, progress)
		if err != nil && n >= 0 && n <= len(out) {
			err = &recordWriteError{cause: err, bytes: uint64(n), complete: n == len(out)}
		}
		w.writeErr = err
	}
	return w.writeErr
}

func (w *sealedWriter) Close() error {
	w.closeOnce.Do(func() { w.closeErr = w.lower.W.Close() })
	return w.closeErr
}

// CloseWrite ends the sending direction where the carrier can. A carrier that
// cannot, and whose halves are one connection, is left open: closing W would
// close the receiving direction too (the rule of halfCloser.shutdownWrite).
func (w *sealedWriter) CloseWrite() error {
	if cw, ok := w.lower.W.(CloseWriter); ok {
		return cw.CloseWrite()
	}
	if carrierio.SameHalf(unprefixed(w.lower.R), unprefixed(w.lower.W)) {
		return nil
	}
	return w.Close()
}

// Abort aborts the carrier where it can.
func (w *sealedWriter) Abort(err error) {
	if a, ok := w.lower.W.(Aborter); ok {
		a.Abort(err)
	}
}

// ReportsFailure forwards the lower carrier's, as the reader does.
func (w *sealedWriter) ReportsFailure() bool { return reportsFailure(w.lower) }

// CarrierSampler is for internal use by zmux modules.
func (w *sealedWriter) CarrierSampler() *carrierstats.Sampler {
	if sampler := carrierstats.ProviderOf(w.lower.W); sampler != nil {
		return sampler
	}
	return carrierstats.SocketSampler(w.lower.W)
}

func (w *sealedWriter) MaxRecord() int { return SealedMaxRecord }

func (w *sealedWriter) framedOverhead() int {
	if w.msgs {
		return sealTag
	}
	return sealTag + sealedLen
}
func (w *sealedWriter) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
	if w.exp == nil {
		return nil, errors.ErrUnsupported
	}
	return w.exp.ExportKeyingMaterial(l, c, n)
}
func (w *sealedWriter) RecordMode() RecordMode { return w.mode }

// CarrierPeer is the value of an exporter that also knows the peer.
func (w *sealedWriter) CarrierPeer() any {
	if p, ok := w.exp.(CarrierPeer); ok {
		return p.CarrierPeer()
	}
	return nil
}

// writeSuffix keeps a single encoded backing until completion. A positive
// short write advances the offset even if the same call returns an error.
func writeSuffix(w io.Writer, encoded []byte, progress func(int)) (int, error) {
	n := 0
	for n < len(encoded) {
		var accepted int
		err := observeWrite(func() error {
			var err error
			accepted, err = w.Write(encoded[n:])
			return err
		})
		if unknown, ok := errors.AsType[*unknownWriteError](err); ok {
			return n, &unknownWriteError{cause: unknown.cause, end: len(encoded)}
		}
		if accepted < 0 || accepted > len(encoded)-n {
			return n, &unknownWriteError{cause: io.ErrShortWrite, end: len(encoded)}
		}
		n += accepted
		if progress != nil && accepted > 0 {
			progress(n)
		}
		if err != nil {
			return n, err
		}
		if accepted == 0 {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}
