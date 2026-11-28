package zmux

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/varint"
)

// ErrSpliceClosed reports a direction that has stopped accepting commands.
var ErrSpliceClosed = errors.New("zmux: splice direction closed")

// ErrSplicePaused reports an injection rejected during a pause or a pause
// superseded by Resume before its boundary was reached.
var ErrSplicePaused = errors.New("zmux: splice direction paused or pause superseded")

// SpliceInjection is one unit offered to a splice. It goes through the
// direction's current hooks. Inject copies Data before returning.
type SpliceInjection struct {
	Data            []byte
	Type, ContextID uint64
	Kind            SpliceUnitKind
}

// SpliceDisposition reports the local result of an accepted injection.
type SpliceDisposition struct {
	End                  *SpliceEnd
	Sequence, Generation uint64
	Bytes                uint64 // accepted by the next hop's send path
	Outputs              int    // complete output items accepted
	Status               SpliceReceiptStatus
	Reason               SpliceDropReason
}

// SpliceReceipt acknowledges local admission. Done receives the final
// local disposition once. Cancelling the wait does not retract admission.
type SpliceReceipt struct {
	Reason               SpliceRejection
	Status               SpliceReceiptStatus
	Done                 <-chan SpliceDisposition
	Sequence, Generation uint64
}

// SpliceController controls a running bidirectional splice.
type SpliceController struct {
	AtoB, BtoA *SpliceControl
	done       chan struct{}
	cancel     context.CancelCauseFunc
	result     SpliceResult
}

// Abort terminates both directions with code, including code zero.
func (s *SpliceController) Abort(code StreamErrorCode) { s.cancel(CancelCause(code)) }

// Done closes when both directions have finished and local work is retired.
func (s *SpliceController) Done() <-chan struct{} { return s.done }

// Wait returns the result once both directions finish.
func (s *SpliceController) Wait(ctx context.Context) (SpliceResult, error) {
	select {
	case <-s.done:
		return s.result, nil
	case <-ctx.Done():
		return SpliceResult{}, ctx.Err()
	}
}

// SpliceControl commands are ordered at admission by one direction owner.
type SpliceControl struct {
	cmd      chan *spliceCommand
	slots    chan struct{}
	done     chan struct{}
	final    SpliceDirection // published by done
	limits   SpliceLimits
	sizes    spliceUnitSizes
	submitMu sync.Mutex
	policy   atomic.Pointer[splicePolicy]
	closed   bool
	packet   bool
}

type splicePolicy struct {
	hooks      SpliceHooks
	generation uint64
}

type spliceCommandKind uint8

const (
	spliceAttach spliceCommandKind = iota
	splicePause
	spliceResume
	spliceInspect
	spliceInject
)

type spliceCommand struct {
	ctx       context.Context
	pause     *SplicePause
	reply     chan spliceCommandReply
	hooks     SpliceHooks
	injection SpliceInjection
	claimed   atomic.Bool
	kind      spliceCommandKind
}

type spliceCommandReply struct {
	receipt    SpliceReceipt
	err        error
	pause      *SplicePause
	direction  SpliceDirection
	generation uint64
}

func (s *SpliceControl) run(ctx context.Context, cmd *spliceCommand) (spliceCommandReply, error) {
	if err := ctx.Err(); err != nil {
		return spliceCommandReply{}, err
	}
	select {
	case <-s.done:
		return spliceCommandReply{}, ErrSpliceClosed
	case s.slots <- struct{}{}:
	default:
		return spliceCommandReply{}, ErrSpliceBudget
	}
	cmd.ctx = ctx
	cmd.reply = make(chan spliceCommandReply, 1)
	s.submitMu.Lock()
	if s.closed {
		s.submitMu.Unlock()
		<-s.slots
		return spliceCommandReply{}, ErrSpliceClosed
	}

	s.cmd <- cmd // the slot reserves space, including while the owner runs a command
	s.submitMu.Unlock()
	select {
	case r := <-cmd.reply:
		return r, r.err
	case <-ctx.Done():
		if cmd.claimed.CompareAndSwap(false, true) {
			// The owner can now only retire the command slot.
			cmd.injection.Data = nil
			return spliceCommandReply{}, ctx.Err()
		}
		r := <-cmd.reply
		return r, r.err
	}
}

func (s *SpliceControl) finishCommand(cmd *spliceCommand, r spliceCommandReply) {
	<-s.slots
	cmd.reply <- r
}

// ReplaceHooks freezes and installs hooks at the next admission boundary.
// It returns the new generation. Existing work keeps its earlier generation.
func (s *SpliceControl) ReplaceHooks(ctx context.Context, hooks SpliceHooks) (uint64, error) {
	h, err := hooks.freeze(s.packet, s.limits.HookSteps)
	if err != nil {
		return 0, err
	}
	out, err := s.run(ctx, &spliceCommand{kind: spliceAttach, hooks: h})
	if err != nil {
		return 0, err
	}
	return out.generation, nil
}

// SplicePause identifies one pause generation. It can be used to wait for
// the safe write boundary or to resume the same generation.
type SplicePause struct {
	err        error
	done       chan struct{}
	Generation uint64
	confirmed  bool
}

// Wait completes when the direction stops starting writes. A write that
// already started a capsule may finish that capsule before confirmation.
func (p *SplicePause) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *SplicePause) finish(err error) {
	if p != nil && !p.confirmed {
		p.confirmed, p.err = true, err
		close(p.done)
	}
}

// Done closes when the direction stops accepting control commands.
func (s *SpliceControl) Done() <-chan struct{} { return s.done }

// Stats returns a snapshot of this direction's local work. Once Done closes,
// it returns the snapshot at reliable termination. Outstanding carrier
// handoffs in the snapshot can settle independently.
func (s *SpliceControl) Stats(ctx context.Context) (SpliceDirection, error) {
	select {
	case <-s.done:
		return s.final, nil
	default:
	}
	reply, err := s.run(ctx, &spliceCommand{kind: spliceInspect})
	if errors.Is(err, ErrSpliceClosed) {
		select {
		case <-s.done:
			return s.final, nil
		case <-ctx.Done():
			return SpliceDirection{}, ctx.Err()
		}
	}
	return reply.direction, err
}

// Pause freezes admission and returns the active pause generation. Use
// its Wait method to confirm the safe write boundary. Each direction has
// one Pause/Resume controller. Combine independent pause intents before
// calling these methods.
func (s *SpliceControl) Pause(ctx context.Context) (*SplicePause, error) {
	r, err := s.run(ctx, &spliceCommand{kind: splicePause})
	if err != nil {
		return nil, err
	}
	return r.pause, nil
}

// Resume releases this pause generation. An earlier token is harmless.
func (s *SpliceControl) Resume(ctx context.Context, pause *SplicePause) error {
	_, err := s.run(ctx, &spliceCommand{kind: spliceResume, pause: pause})
	return err
}

// Inject admits one unit, subject to validation, local budget and the
// direction's state. The command context bounds the wait for admission.
func (s *SpliceControl) Inject(ctx context.Context, in SpliceInjection) (SpliceReceipt, error) {
	if !spliceValidInjection(in, s.packet, s.sizes) {
		return rejectedSplice(fmt.Errorf("%w: invalid splice injection", ErrInvalidConfig))
	}
	r, err := s.run(ctx, &spliceCommand{kind: spliceInject, injection: in})
	if err != nil {
		return rejectedSplice(err)
	}
	return r.receipt, nil
}

func spliceValidInjection(i SpliceInjection, packet bool, sizes spliceUnitSizes) bool {
	if i.ContextID > varint.MaxVarint || i.Type > varint.MaxVarint {
		return false
	}
	switch i.Kind {
	case SpliceStream:
		return !packet && i.Type == 0 && i.ContextID == 0 && len(i.Data) <= sizes.chunk
	case SpliceCapsule:
		return packet && (i.Type == capsuleDatagram || i.ContextID == 0) &&
			len(i.Data) <= sizes.limit(i.Kind, i.Type)
	case SpliceDatagram:
		return i.Type == 0 && len(i.Data) <= sizes.packet
	default:
		return false
	}
}

// StartSplice starts a controllable byte-stream splice.
func StartSplice(ctx context.Context, a, b *Stream, opts *SpliceOptions) (*SpliceController, error) {
	return startSplice(ctx, a, b, opts, false, true)
}

// StartPacketSplice starts a controllable packet splice.
func StartPacketSplice(ctx context.Context, a, b *Stream, opts *SpliceOptions) (*SpliceController, error) {
	return startSplice(ctx, a, b, opts, true, true)
}

func startSplice(ctx context.Context, a, b *Stream, opts *SpliceOptions, packet, controlled bool) (*SpliceController, error) {
	if a == nil || b == nil || a.h == nil || b.h == nil {
		return nil, net.ErrClosed
	}
	o, err := normalizeSplice(a.h, b.h, opts)
	if err != nil {
		return nil, err
	}
	sizes := spliceUnitSizes{chunk: o.Chunk, packet: o.MaxPacketPayload, opaque: o.MaxOpaqueValue}
	limits, err := o.Limits.defaults(sizes.largest(packet))
	if err != nil {
		return nil, err
	}
	if !validSpliceBurst(o.AtoB, limits.OutputBytes) || !validSpliceBurst(o.BtoA, limits.OutputBytes) {
		return nil, ErrInvalidConfig
	}
	hooks := [2]SpliceHooks{o.HooksAtoB, o.HooksBtoA}
	for i, h := range hooks {
		hooks[i], err = h.freeze(packet, limits.HookSteps)
		if err != nil {
			return nil, err
		}
	}
	if err := bindSplice(a.h, b.h); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	controller := &SpliceController{done: make(chan struct{}), cancel: cancel}
	var workers sync.WaitGroup
	var reliable atomic.Int32
	reliable.Store(2)
	closed := make(chan struct{})
	for i, reverse := range [...]bool{false, true} {
		r, w, lim, dg, source, target, mirror := a, b, o.AtoB, o.DatagramAtoB, o.TagA, o.TagB, o.MirrorAtoB
		if reverse {
			r, w, lim, dg, source, target, mirror = b, a, o.BtoA, o.DatagramBtoA, o.TagB, o.TagA, o.MirrorBtoA
		}
		onEvent := o.OnEventAtoB
		if reverse {
			onEvent = o.OnEventBtoA
		}
		var control *SpliceControl
		if controlled {
			control = &SpliceControl{sizes: sizes, packet: packet, limits: limits, done: make(chan struct{}),
				cmd: make(chan *spliceCommand, limits.QueueItems), slots: make(chan struct{}, limits.QueueItems)}
		}
		if control != nil {
			control.policy.Store(&splicePolicy{hooks[i], 1})
		}
		result := &controller.result.AtoB
		if reverse {
			controller.BtoA = control
			result = &controller.result.BtoA
		} else {
			controller.AtoB = control
		}
		d := &spliceRelay{input: r.h, target: w.h, packet: packet, limit: lim, options: dg,
			onEvent: onEvent, control: control, hooks: hooks[i], generation: 1, reverse: reverse, sourceTag: source, targetTag: target,
			clock: r.h.c.clk, limits: limits, sizes: sizes, mirror: mirror, bothFinished: closed, reliableDone: func() {
				if reliable.Add(-1) == 0 {
					close(closed)
				}
			}}
		workers.Go(func() { *result = d.run(ctx) })
	}
	go func() {
		defer runtime.KeepAlive(a)
		defer runtime.KeepAlive(b)
		workers.Wait()
		cancel(nil)
		unbindSplice(a.h, b.h)
		close(controller.done)
	}()
	return controller, nil
}

// ErrSpliceOwned reports that a stream already belongs to a splice.
var ErrSpliceOwned = errors.New("zmux: stream already belongs to a splice")

// spliceOwners serializes the short pairwise binding, without taking
// either session lock or publishing a half-bound pair.
var spliceOwners sync.Mutex

func bindSplice(a, b *handle) error {
	spliceOwners.Lock()
	defer spliceOwners.Unlock()
	if a.spliceOwned || b.spliceOwned {
		return ErrSpliceOwned
	}
	for _, h := range [...]*handle{a, b} {
		var available bool
		h.c.peek(func() { available = h.c.s.SpliceBindable(h.st) })
		if !available {
			return net.ErrClosed
		}
	}
	a.spliceOwned, b.spliceOwned = true, true
	return nil
}

func unbindSplice(a, b *handle) {
	spliceOwners.Lock()
	a.spliceOwned, b.spliceOwned = false, false
	spliceOwners.Unlock()
}

func normalizeSplice(a, b *handle, opts *SpliceOptions) (SpliceOptions, error) {
	var o SpliceOptions
	if a == b {
		return o, errSpliceSelf
	}
	if !a.st.Bidi() || !b.st.Bidi() {
		return o, ErrInvalidConfig
	}
	if opts != nil {
		o = *opts
	}
	if o.Chunk < 0 {
		return o, fmt.Errorf("%w: negative SpliceOptions.Chunk", ErrInvalidConfig)
	}
	if o.Chunk == 0 {
		o.Chunk = 64 << 10
	}
	if o.MaxPacketPayload < 0 || o.MaxOpaqueValue < 0 ||
		uint64(o.MaxPacketPayload) > varint.MaxVarint-24 || uint64(o.MaxOpaqueValue) > varint.MaxVarint-24 ||
		o.MaxPacketPayload > math.MaxInt-24 || o.MaxOpaqueValue > math.MaxInt-24 {
		return o, fmt.Errorf("%w: invalid splice capsule limits", ErrInvalidConfig)
	}
	if o.MaxPacketPayload == 0 {
		o.MaxPacketPayload = DefaultMaxPacket
	}
	if o.MaxOpaqueValue == 0 {
		o.MaxOpaqueValue = MaxRecordSize
	}
	for _, dg := range [...]DatagramSendOptions{o.DatagramAtoB, o.DatagramBtoA} {
		if err := dg.validate(); err != nil {
			return o, err
		}
	}
	return o, nil
}

type spliceUnitSizes struct{ chunk, packet, opaque int }

func (s spliceUnitSizes) limit(kind SpliceUnitKind, typ uint64) int {
	switch {
	case kind == SpliceStream:
		return s.chunk
	case kind == SpliceCapsule && typ != capsuleDatagram:
		return s.opaque
	default:
		return s.packet
	}
}

func (s spliceUnitSizes) largest(packetMode bool) int {
	if !packetMode {
		return max(s.chunk, s.packet)
	}
	const widest = 8
	packetValue := s.packet + widest
	packet := 2*widest + packetValue
	opaque := 2*widest + s.opaque
	return max(s.chunk, packet, opaque)
}

type spliceGroup struct {
	permit                            *splicePermit
	hopDeadline                       core.Time
	receipt                           chan SpliceDisposition
	chain                             []SpliceHook
	parts                             []spliceMember
	resumes                           []<-chan struct{}
	raw                               []byte
	disposition                       SpliceDisposition
	meta                              SpliceMeta
	waitStarted                       core.Time
	accepted, targetAt, ready, expiry core.Time
	sourceEnd                         uint64
	backing                           int
	reserved                          int64
	pricedClass                       core.DatagramAdmission
	measured, waiting, awaitTime      bool
	streaming                         bool
	queued                            bool
	encoded                           bool
	downgraded                        bool
}

func (g *spliceGroup) finish(end *SpliceEnd) {
	releaseSpliceMembers(g.parts)
	g.parts = nil
	if g.receipt != nil {
		if end != nil {
			g.disposition.Status = SpliceDirectionEnded
			copyEnd := *end
			g.disposition.End = &copyEnd
		}
		if g.disposition.Status == "" {
			g.disposition.Status = SpliceSubmitted
		}
		g.receipt <- g.disposition
		close(g.receipt)
		g.receipt = nil
	}
}

func (g *spliceGroup) reliableCapsule() bool {
	return g.meta.Kind == SpliceCapsule || g.downgraded
}
