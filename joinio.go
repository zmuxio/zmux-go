package zmux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/djylb/timeshim"

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

// errJoinHeld marks a join whose bound ended while the attachment cap or
// the refusal backoff still held it: it never sent its Hello, which says
// nothing about its carrier.
var errJoinHeld = errors.New("zmux: join held back by the attachment cap")

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
	lim := 0
	for _, x := range []any{p.W, p.R} {
		if s, ok := x.(CarrierRecordLimit); ok && (lim == 0 || s.MaxRecord() < lim) {
			lim = s.MaxRecord()
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
	p   PipeIO
	buf []byte
}

// more reads at least once and appends. A message carrier gives one unit.
func (u *unitReader) more() error {
	if u.p.Framing == FramingRecords {
		mr, ok := u.p.R.(MessageReader)
		if !ok {
			return errors.New("zmux: Records framing needs a MessageReader")
		}
		m, err := mr.ReadMessage()
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
	defer context.AfterFunc(c.ctx, cancel)() // the session's end ends the handshake
	stop := context.AfterFunc(ctx, func() { closePipeIO(p) })
	defer stop()
	var msg string
	for {
		c.lock()
		if c.closing {
			err := c.errLocked()
			c.unlock()
			return err
		}
		c.s.SetNow(c.clk.now())
		m, ready, err := c.s.JoinPrepare(pd.id)
		c.unlock()
		if err != nil {
			return apiErr(err)
		}
		if ready {
			msg = m
			break
		}
		// The cap or the refusal backoff holds the join.
		select {
		case <-timeshim.After(c.clk.source(), joinHoldPoll):
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errJoinHeld, ctx.Err())
		case <-c.done:
			return c.closedErr()
		}
	}
	plan, err := c.joinPlan(ctx, p, msg)
	if err != nil {
		return c.joinIOFailed(ctx, pd.id, err)
	}
	c.lock()
	if c.closing {
		err := c.errLocked()
		c.unlock()
		return err
	}
	c.s.SetNow(c.clk.now())
	hello, err := c.s.JoinHello(pd.id, core.Framing(p.Framing), p.WriteOverhead, &plan)
	c.unlock()
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
		c.lock()
		st := c.s.VerifyReply(pd.id, u.buf, c.clk.now())
		mode := c.s.JoinMode(pd.id)
		c.unlock()
		switch st.Status {
		case core.JoinProven:
			var plan shaping.UnitPlan
			if mode != join.Bound {
				var err error
				if plan, err = c.joinPlan(ctx, p, core.JoinMsgConfirm); err != nil {
					c.lock()
					c.s.FailJoin(pd.id, err)
					c.unlock()
					return c.joinIOFailed(ctx, pd.id, err)
				}
			}
			c.lock()
			c.s.SetNow(c.clk.now())
			confirm, res, err := c.s.JoinFinish(pd.id, p.WriteOverhead, &plan)
			c.unlock()
			if err != nil {
				closePipeIO(p)
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
				q = sealResult(q, res)
			}
			// The session may have ended, or the attachment failed, while the
			// Confirm was written.
			c.lock()
			if ended := c.closing || c.s.State() >= core.StateClosing; ended || !c.joinedAlive(pd.id) {
				err := fmt.Errorf("zmux: pipe %d not joined: %w", pd.id, ErrPipeGone)
				if ended {
					err = c.errLocked()
				}
				c.unlock()
				closePipeIO(q)
				return err
			}
			pd.adopt(q)
			c.startPipeLocked(pd, dir != DirectionSendOnly, dir != DirectionReceiveOnly)
			c.unlock()
			// Nothing started uses the half a direction leaves unused.
			c.endUnusedHalf(q, dir)
			return nil
		case core.JoinDeclined:
			closePipeIO(p)
			return ErrJoinRefused
		case core.JoinFailedStep:
			closePipeIO(p)
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
	c.lock()
	if !ok {
		c.s.JoinHookFault()
	} else if !planned {
		c.s.JoinUnplanned()
	}
	l := c.s.ShapeLimitsInForce()
	c.unlock()
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

// joinHoldPoll is how often a held join asks again.
const joinHoldPoll = 50 * time.Millisecond

// joinIOFailed fails a JOINING attachment after an error of its carrier.
func (c *Conn) joinIOFailed(ctx context.Context, id uint64, err error) error {
	if c.ctx.Err() != nil {
		return c.closedErr()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return fmt.Errorf("zmux: pipe %d not joined: %w", id, err)
}

func joinFraming(p PipeIO) join.Framing {
	if p.Framing == FramingRecords {
		return join.Message
	}
	return join.Stream
}

// endUnusedHalf ends what a direction does not use after the handshake.
func (c *Conn) endUnusedHalf(p PipeIO, dir Direction) {
	switch dir {
	case DirectionSendOnly:
		if p.R != nil && !sameHalf(p.R, p.W) {
			_ = p.R.Close()
		}
	case DirectionReceiveOnly:
		if cw, ok := p.W.(CloseWriter); ok {
			_ = cw.CloseWrite()
		} else if p.W != nil && !sameHalf(p.R, p.W) {
			_ = p.W.Close()
		}
	case DirectionBidirectional:
	}
}

// helloWithoutBinding counts a Hello in Bound on a carrier that has no Binding.
func (c *Conn) helloWithoutBinding() {
	c.lock()
	defer c.unlock()
	c.s.HelloWithoutBinding()
}

// acceptHello authenticates and confirms a Hello that named this session.
func (c *Conn) acceptHello(ctx context.Context, p PipeIO, h join.Hello, binding []byte, admitted any, maxUnconfirmed int) (uint64, error) {
	// Waiting for establishment or the join keys is bounded even when the
	// handshake is not (Config.joinWaitTimeout).
	ctx, cancel := timeshim.WithTimeout(ctx, c.clk.source(), c.cfg.joinTimeout())
	defer cancel()
	defer context.AfterFunc(c.ctx, cancel)() // the session's end ends the handshake
	stop := context.AfterFunc(ctx, func() { closePipeIO(p) })
	defer stop()
	// ended is the error of a failure caused by this end stopping the wait: no peer's
	// fault, so not an authentication failure.
	ended := func(err error) error {
		if ctx.Err() != nil {
			return errRefused
		}
		return err
	}
	var pj *core.PeerJoin
	for pj == nil {
		c.lock()
		if c.closing {
			c.unlock()
			closePipeIO(p)
			return 0, errRefused
		}
		c.s.SetNow(c.clk.now())
		var res core.JoinResult
		pj, res = c.s.OpenHello(h, core.Framing(p.Framing), binding)
		c.unlock()
		switch res {
		case core.JoinAccepted:
		case core.JoinHold:
			if err := c.waitEstablished(ctx); err != nil {
				closePipeIO(p)
				return 0, err
			}
		case core.JoinKeyWait:
			if err := c.waitJoinKeys(ctx); err != nil {
				closePipeIO(p) // silently
				return 0, err
			}
		case core.JoinUnknownSuite:
			return 0, errUnknownSuite
		case core.JoinClose, core.JoinRefused:
			return 0, errRejected
		}
	}
	mode := RecordMode(pj.Mode)
	info := joinCarrierInfo(p, false)
	allow := c.cfg.AllowRecordMode
	if allow == nil && mode != RecordSealed || allow != nil && !allow(info, mode, c.admitted) ||
		c.cfg.AuthorizeJoin != nil && !c.cfg.AuthorizeJoin(c, PipeID(pj.Pipe.ID)) {
		return 0, errRejected // the Pipe ID's bit stays clear
	}
	dirIn, dirOut := pj.Pipe.Direction != 1, pj.Pipe.Direction != 0
	plan, err := c.joinPlan(ctx, p, core.JoinMsgReply)
	if err != nil {
		return 0, errRefused // the wait was cancelled or the session ended
	}
	outer := outerOf(p)
	c.lock()
	if c.closing {
		c.unlock()
		return 0, errRefused
	}
	loop := nestPipe(c, outer) != nil
	c.s.SetNow(c.clk.now())
	reply, res := c.s.ClaimHello(pj, core.Carrier{HasIn: p.R != nil, HasOut: p.W != nil, Refuse: loop}, p.WriteOverhead, h.WireLen(joinFraming(p)), &plan)
	if res != core.JoinAccepted && !loop {
		unnestPipe(c, outer)
	}
	var evict func()
	if res == core.JoinAccepted {
		evict = c.holdUnconfirmed(p, maxUnconfirmed)
	}
	c.unlock()
	if evict != nil {
		evict()
	}
	markTaken(p)
	if res == core.JoinClose {
		return 0, errRejected
	}
	if err := writeUnit(p, reply); err != nil {
		if res == core.JoinAccepted {
			c.dropUnconfirmed(p)
			unnestPipe(c, outer)
		}
		return 0, ended(errRejected)
	}
	if res == core.JoinRefused {
		return 0, errRefusalSent
	}
	defer c.dropUnconfirmed(p)
	// The acceptor's records wait for Confirmed.
	u := &unitReader{p: p}
	used := 0
	if pj.Mode != join.Bound {
		if err := u.more(); err != nil {
			unnestPipe(c, outer)
			return 0, ended(errRejected)
		}
	}
	for {
		n, _, done, err := pj.ReadConfirm(u.buf)
		if err != nil {
			unnestPipe(c, outer)
			return 0, errRejected
		}
		if done {
			used = n
			break
		}
		if err := u.more(); err != nil {
			unnestPipe(c, outer)
			return 0, ended(errRejected)
		}
	}
	result, err := pj.Result()
	if err != nil {
		unnestPipe(c, outer)
		return 0, errRejected
	}
	q := withPrefix(p, u.buf[used:])
	if pj.Mode == join.Sealed {
		q = sealResult(q, result)
	}
	c.lock()
	if c.closing {
		c.unlock()
		unnestPipe(c, outer)
		return 0, errRefused
	}
	id := pj.Pipe.ID
	pd := c.newPipeDrv(id, q)
	pd.tcp = tcpOf(p.W)
	pd.outer = outer
	low := pd.tcp.setLowWater(multiLowWater)
	ci := carrierInfo(q, pd.tcp, low)
	c.s.SetNow(c.clk.now())
	switch c.s.ConfirmJoin(pj, core.Carrier{HasIn: dirIn, HasOut: dirOut, In: ci, Out: ci, OutSig: pd.wsig, Kind: uint8(p.Carrier)}) {
	case core.JoinAccepted:
	case core.JoinHold, core.JoinKeyWait, core.JoinClose, core.JoinRefused, core.JoinUnknownSuite:
		unnestPipe(c, outer)
		c.unlock()
		return 0, errRefused
	}
	pd.admitted = admitted
	c.startPipeLocked(pd, dirIn, dirOut)
	c.unlock()
	// The acceptor's unused half: nothing started uses it.
	switch pj.Pipe.Direction {
	case 0:
		c.endUnusedHalf(q, DirectionReceiveOnly)
	case 1:
		c.endUnusedHalf(q, DirectionSendOnly)
	}
	return id, nil
}

// holdUnconfirmed records p and returns what closes an evicted carrier (under the
// lock): at most limit Unconfirmed carriers are held (ListenerConfig.MaxPendingPerSession),
// a new one evicting the oldest.
func (c *Conn) holdUnconfirmed(p PipeIO, limit int) (evict func()) {
	if len(c.unconfirmed) >= limit {
		old := c.unconfirmed[0]
		c.unconfirmed = c.unconfirmed[1:]
		evict = func() { closePipeIO(old) }
	}
	c.unconfirmed = append(c.unconfirmed, p)
	return evict
}

func (c *Conn) dropUnconfirmed(p PipeIO) {
	c.lock()
	defer c.unlock()
	c.unconfirmed = slices.DeleteFunc(c.unconfirmed, func(q PipeIO) bool { return sameHalf(q.W, p.W) && sameHalf(q.R, p.R) })
}

// PairedPipe makes one carrier of two unidirectional ones, the local endpoint's
// uplink and downlink. The attachment fails as one.
func PairedPipe(up, down PipeIO) PipeIO {
	return PipeIO{R: down.R, W: up.W, Framing: up.Framing, Carrier: CarrierSplit, Label: up.Label, FateGroup: up.FateGroup,
		WriteOverhead: up.WriteOverhead, Variant: up.Variant}
}

// ---- Sealed records ----

const sealedLen = 2

// SealedMaxRecord is the largest Sealed record plaintext.
const SealedMaxRecord = join.MaxMessage - 16

// sealResult wraps a carrier with the records of a finished Sealed Join.
func sealResult(p PipeIO, r *join.Result) PipeIO {
	return SealRecords(p, r.Sealer, r.Opener, RecordSealed, exportResult{r})
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
// secure carrier.
func SealRecords(p PipeIO, s RecordSealer, o RecordOpener, mode RecordMode, exp KeyingMaterialExporter) PipeIO {
	r := &sealedReader{lower: p, o: o, msgs: p.Framing == FramingRecords, exp: exp, mode: mode}
	w := &sealedWriter{lower: p, s: s, msgs: p.Framing == FramingRecords, exp: exp, mode: mode}
	overhead := p.WriteOverhead + s.Overhead()
	if p.Framing != FramingRecords {
		overhead += sealedLen
	}
	return PipeIO{R: r, W: w, Framing: FramingRecords, Carrier: p.Carrier, Protocol: p.Protocol, Label: p.Label, FateGroup: p.FateGroup,
		WriteOverhead: overhead, Variant: p.Variant, HandshakePadding: p.HandshakePadding}
}

type sealedReader struct {
	lower PipeIO
	o     RecordOpener
	msgs  bool
	head  [sealedLen]byte
	mode  RecordMode
	exp   KeyingMaterialExporter
}

func (r *sealedReader) Read([]byte) (int, error) { return 0, errors.New("zmux: message carrier") }
func (r *sealedReader) Close() error {
	if r.lower.R != nil && !sameHalf(r.lower.R, r.lower.W) {
		return r.lower.R.Close()
	}
	return nil
}

// ReadMessage returns the next record; a failed authentication fails the carrier for good.
func (r *sealedReader) ReadMessage() ([]byte, error) {
	var ct []byte
	if r.msgs {
		m, err := r.lower.R.(MessageReader).ReadMessage()
		if err != nil {
			return nil, err
		}
		ct = m
	} else {
		if _, err := io.ReadFull(r.lower.R, r.head[:]); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint16(r.head[:]))
		ct = make([]byte, n) // the session keeps a record's bytes
		if _, err := io.ReadFull(r.lower.R, ct); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return r.o.Open(ct[:0], ct)
}

func (r *sealedReader) ExportKeyingMaterial(l string, c []byte, n int) ([]byte, error) {
	return r.exp.ExportKeyingMaterial(l, c, n)
}
func (r *sealedReader) RecordMode() RecordMode { return r.mode }

type sealedWriter struct {
	lower PipeIO
	s     RecordSealer
	msgs  bool
	mode  RecordMode
	mu    sync.Mutex
	buf   []byte
	exp   KeyingMaterialExporter
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

func (w *sealedWriter) Close() error { return w.lower.W.Close() }

// CloseWrite ends the sending direction where the carrier can.
func (w *sealedWriter) CloseWrite() error {
	if cw, ok := w.lower.W.(CloseWriter); ok {
		return cw.CloseWrite()
	}
	return w.lower.W.Close()
}

// Abort aborts the carrier where it can.
func (w *sealedWriter) Abort(err error) {
	if a, ok := w.lower.W.(Aborter); ok {
		a.Abort(err)
	}
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
