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
var errRefusalSent = fmt.Errorf("%w: refusal sent", errRefused)

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
	if !IsCarrierSecure(p) {
		return nil, fmt.Errorf("%w: Bound on a carrier without a Secure declaration", ErrInvalidConfig)
	}
	if p.Carrier == CarrierSplit {
		return nil, fmt.Errorf("%w: Bound on a split carrier", ErrInvalidConfig)
	}
	e, ok := Exporter(p)
	if !ok {
		return nil, fmt.Errorf("%w: Bound on a carrier without an exporter", ErrInvalidConfig)
	}
	b, err := e.ExportKeyingMaterial(join.ExporterLabel, nil, join.BindingLen)
	if err != nil {
		return nil, fmt.Errorf("zmux: carrier exporter: %w", err)
	}
	return b, nil
}

// writeUnit writes a whole handshake unit in one write.
func writeUnit(p PipeIO, b []byte) error {
	if p.Framing == FramingRecords {
		mw, ok := p.W.(MessageWriter)
		if !ok {
			return errors.New("zmux: Records framing needs a MessageWriter")
		}
		return mw.WriteMessage(b)
	}
	_, err := p.W.Write(b)
	return err
}

// unitReader reads what a handshake step asks for from a carrier and keeps the rest.
type unitReader struct {
	buf []byte
	p   PipeIO
}

// more reads at least once and appends. A message carrier gives one unit.
func (u *unitReader) more() error {
	if u.p.Framing == FramingRecords {
		mr, ok := u.p.R.(MessageReader)
		if !ok {
			return errors.New("zmux: Records framing needs a MessageReader")
		}
		m, err := mr.ReadMessage(MaxRecordSize)
		u.buf = append(u.buf[:0], m...)
		return err
	}
	var tmp [firstReadChunk]byte
	n, err := u.p.R.Read(tmp[:])
	u.buf = append(u.buf, tmp[:n]...)
	if n > 0 {
		return nil
	}
	return err
}

// joinCarrier runs the joiner's handshake on a JOINING attachment and starts the pipe.
// Nothing read from the carrier reaches the session before the
// Reply verifies; a failure fails only the attachment.
func (c *Conn) joinCarrier(ctx context.Context, pd *pipeDrv, p PipeIO, dir Direction) error {
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
			markCapacityWait(ctx)
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
	var closed error
	c.locked(func() {
		if c.closing {
			closed = c.errLocked()
			return
		}
		c.s.SetNow(c.clk.now())
		hello, err = c.s.JoinHello(pd.id, core.Framing(p.Framing), p.WriteOverhead, &plan)
	})
	if closed != nil {
		return closed
	}
	if err != nil {
		return apiErr(err)
	}
	if err := writeUnit(p, hello); err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	u := &unitReader{p: p}
	if err := u.more(); err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	for {
		var st core.JoinStep
		var mode join.Mode
		c.locked(func() {
			st = c.s.VerifyReply(pd.id, u.buf, c.clk.now())
			if st.Status == core.JoinDeclined {
				_ = pd.end.join.settleLocked(ErrJoinRefused)
			}
			mode = c.s.JoinMode(pd.id)
		})
		switch st.Status {
		case core.JoinProven:
			var plan shaping.UnitPlan
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
			if len(confirm) > 0 {
				if err := writeUnit(p, confirm); err != nil {
					return c.joinIOFailed(ctx, pd.id, err)
				}
			}
			q := withPrefix(p, u.buf[st.N:])
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
			if dir != DirectionReceiveOnly {
				primeCarrierSample(q.W)
			}
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
				c.startPipeLocked(pd, dir != DirectionSendOnly, dir != DirectionReceiveOnly, carrier)
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
				u.buf = u.buf[st.N:]
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
func (c *Conn) joinPlan(ctx context.Context, p PipeIO, msg string) (shaping.UnitPlan, error) {
	var plan shaping.UnitPlan
	hook := c.cfg.HandshakeShaper
	if hook == nil {
		return plan, nil
	}
	kind := ""
	if p.Carrier != 0 {
		kind = p.Carrier.String()
	}
	planned, ok := shaping.Ask(hook, &shaping.HandshakeView{Kind: kind, Handshake: "join", Message: msg}, &plan)
	var l shaping.Limits
	c.locked(func() {
		if !ok {
			c.s.JoinHookFault()
		} else if !planned {
			c.s.JoinUnplanned()
		}
		l = c.s.ShapeLimitsInForce()
	})
	shaping.ApplyLimits(&plan, &l)
	if plan.Delay > 0 {
		select {
		case <-timeshim.After(c.clk.source(), plan.Delay):
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
func (c *Conn) acceptHello(ctx context.Context, p PipeIO, h join.Hello, binding []byte, admitted any, maxUnconfirmed int, wait *attachment) error {
	// Waiting for establishment or the join keys is bounded even when the
	// handshake is not (Config.joinWaitTimeout).
	parent := ctx
	deadline := c.clk.source().Now().Add(c.cfg.joinTimeout())
	ctx, cancel := timeshim.WithDeadline(ctx, c.clk.source(), deadline)
	defer cancel()
	stopEnd := context.AfterFunc(c.ctx, cancel) // the session's end ends the handshake
	defer stopEnd()
	// The setup owner watches the listener and parent. This bound also
	// interrupts the provisional carrier during the confirmation handshake.
	var pj *core.PeerJoin
	for pj == nil {
		var res core.JoinResult
		closing := false
		c.locked(func() {
			if closing = c.closing; closing {
				return
			}
			c.s.SetNow(c.clk.now())
			pj, res = c.s.OpenHello(h, core.Framing(p.Framing), binding)
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
		case core.JoinClose, core.JoinRefused:
			return errRejected
		}
	}
	id := PipeID(pj.Pipe.ID)
	if auth := c.cfg.AuthorizeJoin; auth != nil {
		var allowed bool
		safeLog(c.cfg.Logger, "Config.AuthorizeJoin", func() { allowed = auth(c, id) })
		if !allowed {
			return errRejected
		}
	}
	out := c.newJoinOutcome(parent, id, true, deadline)
	out.cancellation = func() error { return wait.cancellation(parent) }
	held := &heldJoin{out: out, p: p, cancel: cancel}
	stop := interruptOnCancel(ctx, func() { c.locked(func() { _ = out.settleLocked(ctx.Err()) }) })
	defer stop()
	err := hostCall(c.cfg.Logger, "authorized join", func() error {
		return c.confirmHello(ctx, p, pj, admitted, maxUnconfirmed, wait, held, out, h)
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

func (c *Conn) confirmHello(ctx context.Context, p PipeIO, pj *core.PeerJoin, admitted any, maxUnconfirmed int, wait *attachment, held *heldJoin, out *joinOutcome, h join.Hello) error {
	var edge *nestEdge
	defer func() { edge.drop() }()
	mode := RecordMode(pj.Mode)
	info := joinCarrierInfo(p, false)
	ok := mode == RecordSealed
	if allow := c.cfg.AllowRecordMode; allow != nil {
		ok = false
		safeLog(c.cfg.Logger, "Config.AllowRecordMode", func() { ok = allow(info, mode, c.admitted) })
	}
	if !ok {
		return errRejected
	}
	ended := func(err error) error {
		if ctx.Err() != nil {
			return errRefused
		}
		return err
	}
	stop := interruptOnCancel(ctx, func() { closePipe(p) })
	// A successful commit stops the interrupt below.
	var stopped bool
	defer func() {
		if !stopped {
			stop()
		}
	}()
	dirIn, dirOut := pj.Pipe.Direction != 1, pj.Pipe.Direction != 0
	plan, err := c.joinPlan(ctx, p, core.JoinMsgReply)
	if err != nil {
		return errRefused // the wait was cancelled or the session ended
	}
	var outer outerRef
	if !safeLog(c.cfg.Logger, "CarrierConn", func() { _, outer = carrierIdentity(p) }) {
		return errRejected
	}
	var reply []byte
	var res core.JoinResult
	var evict func()
	closing := false
	c.locked(func() {
		l := wait.listener
		l.mu.RLock()
		defer l.mu.RUnlock()
		if !out.settled {
			if reason := out.reasonLocked(nil); reason != nil {
				out.publishLocked(reason)
			}
		}
		if closing = out.settled; closing {
			return
		}
		edge, err = nestPipe(c, outer)
		loop := err != nil
		c.s.SetNow(c.clk.now())
		reply, res = c.s.ClaimHello(pj, core.Carrier{HasIn: p.R != nil, HasOut: p.W != nil, Refuse: loop}, p.WriteOverhead, h.WireLen(joinFraming(p)), &plan)
		if res != core.JoinAccepted {
			if res == core.JoinRefused && wait.commitLocked(ctx, setupPending, setupRejecting) != nil {
				closing = true
				return
			}
			_ = out.settleLocked(ErrJoinRefused)
		} else if old, ok := c.unconfirmed.Hold(pj, held, maxUnconfirmed); ok {
			_ = old.out.settleLocked(ErrJoinEvicted)
			evict = old.evict
		}
	})
	if closing {
		return errRefused
	}
	if evict != nil {
		evict()
	}
	if res == core.JoinAccepted {
		defer c.locked(func() { c.unconfirmed.Drop(pj) })
	}
	if err := wait.acceptHead(ctx); err != nil {
		return err
	}
	markTaken(p)
	if res == core.JoinClose {
		return errRejected
	}
	if err := writeUnit(p, reply); err != nil {
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
	q := withPrefix(p, u.buf[used:])
	if pj.Mode == join.Sealed {
		if sealedLimitErr(p, sealOverhead(p.Framing)) != nil {
			return errRejected
		}
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
	stopped = true
	if !stop() {
		return errRefused
	}
	if err := c.waitPublication(ctx); err != nil {
		return errRefused
	}
	if dirOut {
		primeCarrierSample(q.W)
	}
	c.locked(func() {
		if out.settled {
			return
		}
		if reason := out.reasonLocked(nil); reason != nil {
			out.publishLocked(reason)
			return
		}
		c.s.SetNow(c.clk.now())
		if !wait.commitCarrier(ctx, func() bool {
			return c.unconfirmed.Confirm(pj) && c.s.ConfirmJoin(pj, core.Carrier{HasIn: dirIn, HasOut: dirOut, In: ci, Out: ci, OutSig: &pd.writer.poll, Kind: uint8(p.Carrier)}) == core.JoinAccepted
		}) {
			_ = out.settleLocked(ErrJoinRefused)
			return
		}
		pd.admitted = admitted
		out.holdLocked(id, pd.end)
		pd.nest, edge = edge, nil // the session's now: its pipe's end drops the edge
		wait.attach(pd)
		c.startPipeLocked(pd, dirIn, dirOut, attachment.carrier)
		out.publishLocked(nil)
		confirmed = true
	})
	if !confirmed {
		return errRefused
	}
	return nil
}

// PairedPipe makes one carrier of two unidirectional ones, the local endpoint's
// uplink and downlink. The attachment fails as one.
func PairedPipe(up, down PipeIO) PipeIO {
	return PipeIO{R: down.R, W: up.W, Framing: up.Framing, Carrier: CarrierSplit, Label: up.Label, FateGroup: up.FateGroup,
		WriteOverhead: up.WriteOverhead, MaxRecord: up.MaxRecord, Variant: up.Variant}
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
	// Open appends the opened ciphertext to dst; an error is an
	// authentication failure, which fails the carrier for good.
	Open(dst, ciphertext []byte) ([]byte, error)
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
		WriteOverhead: overhead, MaxRecord: sealedRecordLimit(p, overhead-p.WriteOverhead), Variant: p.Variant, HandshakePadding: p.HandshakePadding}
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
	o     RecordOpener
	exp   KeyingMaterialExporter
	lower PipeIO
	head  [sealedLen]byte
	msgs  bool
	mode  RecordMode
}

func (r *sealedReader) Read([]byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (r *sealedReader) Close() error {
	if r.lower.R != nil && !carrierio.SameHalf(r.lower.R, r.lower.W) {
		return r.lower.R.Close()
	}
	return nil
}

// ReadMessage returns the next record, of at most limit bytes; a failed
// authentication fails the carrier for good. A sealed message is never
// longer than join.MaxMessage, and a longer record than limit is refused
// after reading at most limit plus the tag of its sealed message.
func (r *sealedReader) ReadMessage(limit int) ([]byte, error) {
	var ct []byte
	if r.msgs {
		m, err := r.lower.R.(MessageReader).ReadMessage(min(limit+sealTag, join.MaxMessage))
		if err != nil {
			return nil, err
		}
		ct = m
	} else {
		if _, err := io.ReadFull(r.lower.R, r.head[:]); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint16(r.head[:]))
		if n > limit+sealTag {
			return nil, ErrMessageTooLarge
		}
		ct = make([]byte, n) // the session keeps a record's bytes
		if _, err := io.ReadFull(r.lower.R, ct); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	pt, err := r.o.Open(ct[:0], ct)
	if err == nil && len(pt) > limit {
		return nil, ErrMessageTooLarge
	}
	return pt, err
}

func (r *sealedReader) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
	return r.exp.ExportKeyingMaterial(l, c, n)
}
func (r *sealedReader) RecordMode() RecordMode { return r.mode }

// ReportsFailure forwards the lower carrier's (both halves alike): sealed
// records fail when it fails.
func (r *sealedReader) ReportsFailure() bool { return reportsFailure(r.lower) }

type sealedWriter struct {
	s         RecordSealer
	exp       KeyingMaterialExporter
	buf       []byte
	lower     PipeIO
	closeErr  error // published by closeOnce
	mu        sync.Mutex
	closeOnce sync.Once
	msgs      bool
	mode      RecordMode
}

func (w *sealedWriter) Write(b []byte) (int, error) {
	if err := w.WriteMessage(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *sealedWriter) sealOne(dst, m []byte) ([]byte, error) {
	if !w.msgs {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(m)+w.s.Overhead()))
	}
	return w.s.Seal(dst, m)
}

// WriteMessage seals one record and writes it in one write.
func (w *sealedWriter) WriteMessage(m []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	out, err := w.sealOne(w.buf[:0], m)
	if err != nil {
		return err
	}
	w.buf = out
	if w.msgs {
		return w.lower.W.(MessageWriter).WriteMessage(out)
	}
	_, err = w.lower.W.Write(out)
	return err
}

// WriteMessages seals a batch into one write of a byte stream.
func (w *sealedWriter) WriteMessages(ms [][]byte) error {
	if w.msgs {
		for _, m := range ms {
			if err := w.WriteMessage(m); err != nil {
				return err
			}
		}
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.buf[:0]
	var err error
	for _, m := range ms {
		if out, err = w.sealOne(out, m); err != nil {
			return err
		}
	}
	w.buf = out
	_, err = w.lower.W.Write(out)
	return err
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
func (w *sealedWriter) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
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
