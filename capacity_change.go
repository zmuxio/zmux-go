package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
)

// CandidateID is the host's stable identifier within a controller domain.
type CandidateID string

// MemberID identifies a group of attachments within a session.
type MemberID = core.MemberID

// AttemptID identifies an actual opener invocation.
type AttemptID uint64

// AttachmentSpec declares the session directions carried by one resource.
type AttachmentSpec struct {
	Options    PipeOptions
	Directions Directions
}

// OpenStage identifies the protocol setup that will consume the resources.
type OpenStage uint8

const (
	// OpenBootstrap starts a new session.
	OpenBootstrap OpenStage = iota + 1
	// OpenJoin adds a member to an existing session.
	OpenJoin
)

// AttemptCause identifies the intent that consumes the resources.
type AttemptCause string

// Causes of an actual carrier opener invocation.
const (
	AttemptManual    AttemptCause = "manual"
	AttemptBootstrap AttemptCause = "bootstrap"
	AttemptRecovery  AttemptCause = "recovery"
	AttemptGrowth    AttemptCause = "growth"
	AttemptReplace   AttemptCause = "replace"
)

// OpenRequest describes the exact resources requested by the executor.
type OpenRequest struct {
	Credential CarrierCredential
	Lifetime   context.Context
	Cause      AttemptCause
	Layout     []AttachmentSpec
	Attempt    AttemptID
	Need       Directions
	Stage      OpenStage
}

// CarrierOpener returns ownership of all returned resources including
// partial results returned with an error. Its context is bounded by the
// attempt deadline, which may move earlier if the session suspends.
type CarrierOpener func(context.Context, OpenRequest) ([]PipeIO, error)

// MemberSpec declares one proposed member.
type MemberSpec struct {
	Candidate   CandidateID
	Attachments []AttachmentSpec
}

// ChangeRequest plans a batch against a session capacity observation.
type ChangeRequest struct {
	Add            []MemberSpec
	Drain          []MemberID
	PolicyRevision uint64
	Observed       uint64
	Timeout        time.Duration
	MinEpoch       uint64
}

// ChangeResult is the committed capacity and the members entering drain.
type ChangeResult struct {
	Draining []MemberID
	Capacity CapacitySnapshot
}

var (
	// ErrAttemptIDExhausted means the session has no unused opener identifiers.
	ErrAttemptIDExhausted = errors.New("zmux: attempt ID space exhausted")
	// ErrCapacityBlocked indicates exhausted attachment capacity.
	ErrCapacityBlocked = core.ErrCapacityBlocked
	// ErrChangePending indicates a batch still needs confirmation, stability or epoch evidence.
	ErrChangePending = core.ErrChangePending
	// ErrUnknownMember indicates the member no longer has an indexed attachment.
	ErrUnknownMember = core.ErrUnknownMember
)

// CapacityChange owns a batch until it commits, expires or is canceled.
type CapacityChange struct {
	deadline       time.Time
	trialTimeout   time.Duration
	owner          *SessionControl
	generation     uint64
	targetRevision uint64
	id             uint64
	ctx            context.Context
	c              *Conn
	batch          *core.CapacityBatch
	cancel         context.CancelFunc
	stopEnd        func() bool
	stop           func() bool
	cause          AttemptCause
	plans          []MemberSpec
	attempts       []*Attempt
	done           bool
}

func (m MemberSpec) corePlan() (core.ChangeMember, error) {
	plan := core.ChangeMember{Candidate: string(m.Candidate), Attachments: len(m.Attachments)}
	for _, a := range m.Attachments {
		if a.Directions == 0 || a.Directions&^Both != 0 {
			return plan, fmt.Errorf("%w: invalid attachment directions", ErrInvalidConfig)
		}
		if _, err := a.Options.join(attachmentDirection(a.Directions)); err != nil {
			return plan, err
		}
		plan.Directions |= a.Directions
	}
	return plan, nil
}

func attachmentDirection(mask Directions) Direction {
	switch mask {
	case Send:
		return DirectionSendOnly
	case Receive:
		return DirectionReceiveOnly
	default:
		return DirectionBidirectional
	}
}

// BeginChange reserves the complete layout and claims the affected drain set.
func (c *Conn) BeginChange(ctx context.Context, req ChangeRequest) (*CapacityChange, error) {
	return c.beginChange(ctx, req, AttemptManual, nil, 0)
}

func (c *Conn) beginChange(ctx context.Context, req ChangeRequest, cause AttemptCause, owner *SessionControl, revision uint64) (*CapacityChange, error) {
	if c == nil {
		return nil, net.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Timeout < 0 || req.Timeout > time.Duration(core.MaxJoinDeadline) {
		return nil, fmt.Errorf("%w: invalid change timeout", ErrInvalidConfig)
	}
	plans := slices.Clone(req.Add)
	ready := make([]core.ChangeMember, len(plans))
	for i, m := range plans {
		m.Attachments = slices.Clone(m.Attachments)
		plan, err := m.corePlan()
		if err != nil {
			return nil, err
		}
		plans[i], ready[i] = m, plan
	}

	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	if owner != nil {
		if !owner.currentLocked(revision) || owner.ctx.Err() != nil {
			return nil, ErrStaleBinding
		}
		if cause == AttemptReplace {
			if err := owner.checkQualityLocked(owner.target, false); err != nil {
				return nil, err
			}
		}
		if owner.batch != nil {
			return nil, ErrChangePending
		}
		if len(plans) > 1 {
			return nil, errControllerBatchLayout
		}
	}
	if c.nextCapacityBatch == ^uint64(0) {
		return nil, ErrInvalidConfig
	}
	timeout := req.Timeout
	if timeout == 0 {
		timeout = c.cfg.joinTimeout()
	}
	end := c.s.Now().Add(core.Duration(timeout))
	var b *core.CapacityBatch
	var err error
	if cause == AttemptRecovery {
		if len(ready) != 0 || len(req.Drain) != 0 || req.MinEpoch != 0 {
			return nil, errControllerBatchLayout
		}
		b, err = c.s.BeginRecoveryBatch(req.PolicyRevision)
	} else {
		b, err = c.s.BeginCapacityBatch(req.PolicyRevision, req.MinEpoch, end, ready, req.Drain)
	}
	if err != nil {
		return nil, apiErr(err)
	}
	var deadline time.Time
	var setup context.Context
	var cancel context.CancelFunc
	if cause == AttemptRecovery {
		setup, cancel = context.WithCancel(ctx)
	} else {
		deadline = c.clk.wall(end)
		setup, cancel = timeshim.WithDeadline(ctx, c.clk.source(), deadline)
	}
	tx := &CapacityChange{c: c, batch: b, plans: plans, attempts: make([]*Attempt, len(plans)),
		ctx: setup, cancel: cancel, deadline: deadline, trialTimeout: timeout, cause: cause}
	if owner != nil {
		owner.batch = tx
	}
	c.nextCapacityBatch++
	tx.id = c.nextCapacityBatch
	tx.owner = c.control
	if tx.owner != nil {
		tx.generation = tx.owner.view.BindingGeneration
		tx.targetRevision = tx.owner.target.Revision
	}
	c.changes = append(c.changes, tx)
	tx.stopEnd = context.AfterFunc(c.ctx, cancel)
	tx.stop = context.AfterFunc(setup, func() { _ = tx.Cancel() })
	c.events.push(Event{Kind: EventBatchReserved, BindingGeneration: tx.generation, Batch: tx.id, Revision: tx.targetRevision})
	return tx, nil
}

func (tx *CapacityChange) currentLocked() bool {
	return tx.owner == nil || tx.owner.currentLocked(tx.targetRevision)
}

// Commit revalidates stability, current policy and coverage and starts the
// whole drain set under the session owner.
func (tx *CapacityChange) Commit() (ChangeResult, error) {
	return tx.commit(nil)
}

func (tx *CapacityChange) commit(admitRetirement func([]uint64) error) (ChangeResult, error) {
	if tx == nil {
		return ChangeResult{}, net.ErrClosed
	}
	c := tx.c
	c.lock()
	defer c.unlock()
	c.s.SetNow(c.clk.now())
	if !tx.currentLocked() {
		return ChangeResult{}, ErrStaleBinding
	}
	if tx.ctx.Err() != nil && !tx.done {
		tx.cancelLocked()
		return ChangeResult{}, tx.ctx.Err()
	}
	ids, err := tx.batch.CommitWithRetirement(admitRetirement)
	if err != nil {
		return ChangeResult{Capacity: c.capacityLocked()}, apiErr(err)
	}
	for _, id := range ids {
		c.events.push(Event{Kind: EventDrainCommitted, BindingGeneration: tx.generation, Batch: tx.id, Member: id})
	}
	tx.finishLocked()
	return ChangeResult{Capacity: c.capacityLocked(), Draining: ids}, nil
}

// Cancel withdraws uncommitted work. New members supplying unique coverage
// remain available for later reconciliation.
func (tx *CapacityChange) Cancel() error {
	if tx == nil {
		return net.ErrClosed
	}
	tx.c.locked(tx.cancelLocked)
	return nil
}

func (tx *CapacityChange) cancelLocked() {
	tx.cancelWithCauseLocked(nil)
}

func (tx *CapacityChange) cancelWithCauseLocked(cause error) {
	if tx.done {
		return
	}
	tx.batch.Cancel()
	for _, a := range tx.attempts {
		if a != nil && !a.done {
			if cause != nil {
				a.finishLocked(ReasonCancelled, cause, 0)
			} else {
				a.finishLocked(a.cancellation(), tx.ctx.Err(), 0)
			}
		}
	}
	tx.c.events.push(Event{Kind: EventBatchRolledBack, BindingGeneration: tx.generation, Batch: tx.id})
	tx.finishLocked()
}

func (tx *CapacityChange) finishLocked() {
	tx.done = true
	if sc := tx.owner; sc != nil && sc.batch == tx {
		sc.batch = nil
	}
	if i := slices.Index(tx.c.changes, tx); i >= 0 {
		tx.c.changes = slices.Delete(tx.c.changes, i, i+1)
	}
	tx.stop()
	tx.stopEnd()
	tx.cancel()
}

// AttemptDisposition states whether the complete member entered service.
type AttemptDisposition string

// Terminal member dispositions.
const (
	AttemptAccepted AttemptDisposition = "accepted"
	AttemptRejected AttemptDisposition = "rejected"
)

// AttemptReason classifies a single terminal commit.
type AttemptReason string

// Terminal reasons for a carrier setup or confirmation.
const (
	ReasonDialFailed         AttemptReason = "dial_failed"
	ReasonHookFailed         AttemptReason = "hook_failed"
	ReasonCancelled          AttemptReason = "cancelled"
	ReasonNoLongerNeeded     AttemptReason = "no_longer_needed"
	ReasonSessionClosed      AttemptReason = "session_closed"
	ReasonDeadline           AttemptReason = "deadline"
	ReasonJoinRefused        AttemptReason = "join_refused"
	ReasonConfirmationFailed AttemptReason = "confirmation_failed"
	ReasonConfirmed          AttemptReason = "confirmed"
)

// Attribution identifies whether an outcome describes carrier health.
type Attribution string

// Attribution of a terminal observation.
const (
	AttributionNone    Attribution = "none"
	AttributionCarrier Attribution = "carrier"
)

// AttemptResult is immutable after the confirmation or failure commit.
// Tickets can finish after the logical attempt.
type AttemptResult struct {
	BindingGeneration uint64
	Batch             uint64
	Err               error
	Disposition       AttemptDisposition
	Reason            AttemptReason
	Attribution       Attribution
	Pipes             []PipeID
	Tickets           []AccountingTicket
	ID                AttemptID
	Member            MemberID
}

// AttemptAttachment reports the protocol milestones of one attachment.
type AttemptAttachment struct {
	ReplyAt       time.Time
	Ticket        AccountingTicket
	Pipe          PipeID
	ReplyAccepted bool
	Confirmed     bool
}

// AttemptSnapshot is a committed view of a member attempt.
type AttemptSnapshot struct {
	BindingGeneration  uint64
	Started, Finished  time.Time
	Cause              AttemptCause
	Candidate          CandidateID
	Attachments        []AttemptAttachment
	Result             AttemptResult
	ID                 AttemptID
	Member             MemberID
	Need               Directions
	HandshakeSucceeded bool
}

// Attempt observes one opener invocation and its member confirmation.
type Attempt struct {
	ctx           *attemptDeadline
	deadlineTimer timeshim.Timer
	life          context.Context
	tx            *CapacityChange
	member        *core.Member
	reserve       *core.JoinReservation
	endLife       context.CancelFunc
	ready         chan struct{}
	doneCh        chan struct{}
	closedWork    chan struct{}
	physical      []core.CarrierKeys
	view          AttemptSnapshot
	live          int
	observer      *SessionControl
	work          *controllerWork
	done          bool
}

// StartMember invokes an opener once for the indexed layout.
func (tx *CapacityChange) StartMember(index int, open CarrierOpener) (*Attempt, error) {
	return tx.startMember(index, time.Time{}, open, nil, nil)
}

// beforeStart is an internal fence evaluated by the session owner before
// publishing an attempt or invoking any user work.
func (tx *CapacityChange) startMember(index int, reservedAt time.Time, open CarrierOpener, beforeStart func() error, work *controllerWork) (*Attempt, error) {
	if tx == nil {
		return nil, net.ErrClosed
	}
	if open == nil || index < 0 || index >= len(tx.plans) {
		return nil, ErrInvalidConfig
	}
	return tx.startAttempt(index, tx.plans[index], core.ChangeMember{}, reservedAt, open, beforeStart, work)
}

func (tx *CapacityChange) startAlternative(plan MemberSpec, reservedAt time.Time, open CarrierOpener, beforeStart func() error, work *controllerWork) (*Attempt, error) {
	if tx.cause != AttemptRecovery {
		return nil, errControllerBatchLayout
	}
	ready, err := plan.corePlan()
	if err != nil {
		return nil, err
	}
	return tx.startAttempt(0, plan, ready, reservedAt, open, beforeStart, work)
}

func (tx *CapacityChange) startAttempt(index int, plan MemberSpec, alt core.ChangeMember, reservedAt time.Time,
	open CarrierOpener, beforeStart func() error, work *controllerWork) (*Attempt, error) {
	var a *Attempt
	var err error
	tx.c.locked(func() {
		a, err = tx.startLocked(index, len(plan.Attachments), alt, reservedAt)
		if a != nil {
			a.work = work
		}
	})
	if err != nil {
		return nil, err
	}
	go func() {
		if work != nil {
			defer work.releaseCleanup()
		}
		a.run(plan, open, beforeStart)
	}()
	<-a.ready
	if view := a.Snapshot(); view.ID == 0 {
		if view.Result.Reason == ReasonNoLongerNeeded {
			return nil, errRecoveryNoLongerNeeded
		}
		return nil, view.Result.Err
	}
	return a, nil
}

func (tx *CapacityChange) startLocked(index, attachments int, alt core.ChangeMember, reservedAt time.Time) (*Attempt, error) {
	c := tx.c
	c.s.SetNow(c.clk.now())
	if !tx.currentLocked() {
		return nil, ErrStaleBinding
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	deadline, err := c.attemptDeadlineLocked(tx, reservedAt)
	if err != nil {
		return nil, err
	}
	if c.nextAttempt == ^AttemptID(0) {
		return nil, ErrAttemptIDExhausted
	}
	if !c.s.CanReserveOpener() {
		return nil, ErrCarrierCapacity
	}
	m, r, needed, err := tx.reserveMember(index, alt)
	if err != nil {
		return nil, apiErr(err)
	}
	if tx.owner != nil && m.Candidate != "" {
		if candidate := tx.owner.ctl.candidate(CandidateID(m.Candidate)); candidate.ID != "" {
			candidate.recordAttribution(m)
		}
	}
	c.s.ReserveOpener()
	life, endLife := context.WithCancel(c.ctx)
	a := &Attempt{tx: tx, member: m, reserve: r, ctx: newAttemptDeadline(tx.ctx, deadline), life: life, endLife: endLife,
		ready: make(chan struct{}), doneCh: make(chan struct{}), closedWork: make(chan struct{}), live: 1}
	a.view = AttemptSnapshot{BindingGeneration: tx.generation, Cause: tx.cause, Need: needed, Candidate: CandidateID(m.Candidate), Member: m.ID,
		Attachments: make([]AttemptAttachment, attachments)}
	for i := range a.view.Attachments {
		a.view.Attachments[i].Pipe = UnassignedPipeID
	}
	a.armDeadlineLocked(c.clk.source().Now())
	if tx.cause == AttemptRecovery {
		tx.attempts = slices.DeleteFunc(tx.attempts, func(old *Attempt) bool { return old.done })
		tx.attempts = append(tx.attempts, a)
	} else {
		tx.attempts[index] = a
	}
	return a, nil
}

func (tx *CapacityChange) reserveMember(index int, alt core.ChangeMember) (*core.Member, *core.JoinReservation, Directions, error) {
	if tx.cause == AttemptRecovery {
		need := tx.batch.RecoveryNeed() & alt.Directions
		if need == 0 {
			return nil, nil, 0, errRecoveryNoLongerNeeded
		}
		m, r, err := tx.batch.StartAlternative(alt)
		return m, r, need, err
	}
	m, r, err := tx.batch.StartMember(index)
	if err != nil {
		return nil, nil, 0, err
	}
	return m, r, m.Expected, nil
}

func (a *Attempt) run(plan MemberSpec, open CarrierOpener, beforeStart func() error) {
	c := a.tx.c
	if beforeStart != nil {
		controllerOpenerBarrier(c)
	}
	var request OpenRequest
	c.locked(func() {
		c.s.SetNow(c.clk.now())
		need := a.view.Need
		if a.tx.cause == AttemptRecovery && !a.done {
			need = a.member.Expected & a.tx.batch.RecoveryNeed()
		}
		switch {
		case a.done || !a.tx.currentLocked() || a.tx.ctx.Err() != nil || !c.clk.source().Now().Before(a.deadlineAt()):
			c.s.ReleaseOpener()
			a.finishLocked(a.cancellation(), a.tx.ctx.Err(), 0)
		case need == 0:
			c.s.ReleaseOpener()
			a.finishLocked(ReasonNoLongerNeeded, nil, 0)
		case c.nextAttempt == ^AttemptID(0):
			c.s.ReleaseOpener()
			a.finishLocked(ReasonCancelled, ErrAttemptIDExhausted, 0)
		default:
			if err := a.tx.batch.Valid(); err != nil {
				c.s.ReleaseOpener()
				a.finishLocked(ReasonCancelled, apiErr(err), 0)
				return
			}
			if beforeStart != nil {
				if err := beforeStart(); err != nil {
					c.s.ReleaseOpener()
					a.finishLocked(ReasonCancelled, err, 0)
					return
				}
			}
			c.nextAttempt++
			a.observer = a.tx.owner
			a.view.ID, a.view.Started, a.view.Need = c.nextAttempt, c.clk.source().Now(), need
			c.events.push(Event{Kind: EventRedialCalled, BindingGeneration: a.tx.generation, Batch: a.tx.id, Attempt: a.view.ID, Member: a.member.ID})
			request = OpenRequest{Attempt: a.view.ID, Cause: a.view.Cause, Need: a.view.Need, Stage: OpenJoin,
				Layout: slices.Clone(plan.Attachments), Credential: c.cfg.CarrierCredential, Lifetime: a.life}
		}
	})
	if request.Attempt == 0 {
		c.locked(a.releaseWorkLocked)
		close(a.ready)
		return
	}
	close(a.ready)
	defer c.locked(a.releaseWorkLocked)
	ps, panicked, err := invokeOpener(a.ctx, c.cfg, open, request)
	c.locked(c.s.ReleaseOpener)
	if err == nil {
		err = validateOpened(ps, len(plan.Attachments))
	}
	if err == nil {
		err = a.reservePhysical(ps)
	}
	if err != nil {
		reason := ReasonDialFailed
		if panicked {
			reason = ReasonHookFailed
		}
		retry := peerRetryDelay(err)
		c.locked(func() { a.finishLocked(reason, err, retry) })
		closeReturned(ps)
		return
	}
	for i, p := range ps {
		go a.join(i, p, plan.Attachments[i])
	}
	// Each worker can still dispose of a returned carrier after logical
	// cancellation. Wait for ownership transfer or disposal before refunding.
	for range ps {
		<-a.closedWork
	}
}

func invokeOpener(ctx context.Context, cfg *Config, open CarrierOpener, request OpenRequest) (ps []PipeIO, panicked bool, err error) {
	panicked = !safeLog(cfg.Logger, "CarrierOpener", func() { ps, err = open(ctx, request) })
	if panicked {
		err = errors.New("zmux: carrier opener panicked")
	}
	for i := range ps {
		ps[i] = owned(ps[i])
	}
	return ps, panicked, err
}

func validateOpened(ps []PipeIO, count int) error {
	if len(ps) != count {
		return fmt.Errorf("%w: opener returned %d resources for %d attachments", ErrInvalidConfig, len(ps), count)
	}
	for _, p := range ps {
		if p.R == nil || p.W == nil {
			return fmt.Errorf("%w: protocol setup requires a readable and writable resource", ErrInvalidConfig)
		}
	}
	return nil
}

func closeReturned(ps []PipeIO) {
	for _, p := range ps {
		closePipe(p)
	}
}

func (a *Attempt) join(index int, p PipeIO, spec AttachmentSpec) {
	defer func() { a.closedWork <- struct{}{} }()
	c := a.tx.c
	out := c.newJoinOutcome(a.ctx, UnassignedPipeID, false, time.Time{})
	out.attempt, out.index = a, index
	opts := spec.Options
	opts.Direction, opts.ReturnOnReply = attachmentDirection(spec.Directions), false
	err := hostCall(c.cfg.Logger, "Join member", func() error {
		_, err := c.addPipeCall(a.ctx, p, opts, out)
		return err
	})
	if err != nil {
		c.locked(func() { _ = out.settleLocked(err) })
		p.own.CloseUnreleased()
	}
}

func (a *Attempt) releaseWorkLocked() {
	for _, keys := range a.physical {
		if keys != (core.CarrierKeys{}) {
			a.tx.c.s.ReleaseCarriers(keys)
		}
	}
	clear(a.physical)
	a.releaseLifeLocked()
}

func (a *Attempt) releaseLifeLocked() {
	a.live--
	if a.live == 0 {
		a.endLife()
	}
}

func (a *Attempt) attachedLocked(i int, err error) {
	if a.done {
		return
	}
	if err != nil {
		reason := ReasonConfirmationFailed
		if errors.Is(err, ErrJoinRefused) {
			reason = ReasonJoinRefused
		}
		a.finishLocked(reason, err, 0)
		return
	}
	a.view.Attachments[i].Confirmed = true
	for _, x := range a.view.Attachments {
		if !x.Confirmed {
			return
		}
	}
	if a.tx.cause == AttemptRecovery {
		a.tx.confirmAlternativeLocked(a)
	} else {
		a.finishLocked(ReasonConfirmed, nil, 0)
	}
}

func (tx *CapacityChange) confirmAlternativeLocked(winner *Attempt) {
	if !tx.currentLocked() || winner.ctx.Err() != nil || tx.ctx.Err() != nil || tx.c.closing || !tx.c.clk.source().Now().Before(winner.deadlineAt()) {
		winner.finishLocked(winner.cancellation(), nil, 0)
		return
	}
	if !winner.stillNeededLocked() {
		winner.finishLocked(ReasonNoLongerNeeded, nil, 0)
		return
	}
	if err := tx.batch.CommitAlternative(winner.member); err != nil {
		winner.finishLocked(ReasonCancelled, apiErr(err), 0)
		return
	}
	winner.finishLocked(ReasonConfirmed, nil, 0)
	need := tx.batch.RecoveryNeed()
	var pending bool
	for _, a := range tx.attempts {
		if a.done {
			continue
		}
		if a.canSupply(need) {
			pending = true
		} else {
			a.finishLocked(ReasonNoLongerNeeded, nil, 0)
		}
	}
	if !pending {
		tx.batch.FinishRecovery()
		tx.finishLocked()
	}
}

func (a *Attempt) canSupply(need Directions) bool { return a.member.Expected&need != 0 }

func (a *Attempt) stillNeededLocked() bool {
	return a.tx.cause != AttemptRecovery || a.canSupply(a.tx.batch.RecoveryNeed())
}

func (a *Attempt) cancellation() AttemptReason {
	c := a.tx.c
	switch {
	case errors.Is(a.tx.ctx.Err(), context.DeadlineExceeded) || !c.clk.source().Now().Before(a.deadlineAt()):
		return ReasonDeadline
	case c.closing || c.s.Err() != nil:
		return ReasonSessionClosed
	case a.tx.ctx.Err() == nil && a.tx.cause != AttemptManual:
		return ReasonNoLongerNeeded
	default:
		return ReasonCancelled
	}
}

func (a *Attempt) finishLocked(reason AttemptReason, err error, peerRetry time.Duration) {
	if a.done {
		return
	}
	c := a.tx.c
	sourceReason, sourceErr := reason, err
	current := a.tx.currentLocked()
	if !current {
		reason, err = ReasonCancelled, ErrStaleBinding
	} else if c.closing || c.s.Err() != nil || a.ctx.Err() != nil || a.tx.ctx.Err() != nil || !c.clk.source().Now().Before(a.deadlineAt()) {
		reason = a.cancellation()
	}
	switch reason {
	case ReasonDeadline:
		err = context.DeadlineExceeded
	case ReasonConfirmed, ReasonNoLongerNeeded:
		err = nil
	case ReasonSessionClosed:
		if err == nil {
			err = c.errLocked()
		}
	default:
		if err == nil {
			err = context.Canceled
		}
	}
	r := AttemptResult{ID: a.view.ID, BindingGeneration: a.tx.generation, Batch: a.tx.id, Reason: reason, Err: err, Disposition: AttemptRejected, Attribution: AttributionNone}
	for _, x := range a.view.Attachments {
		if x.Pipe != UnassignedPipeID {
			r.Pipes = append(r.Pipes, x.Pipe)
		}
		if x.Ticket != nil {
			r.Tickets = append(r.Tickets, x.Ticket)
		}
	}
	a.done = true
	if reason == ReasonConfirmed {
		r.Disposition = AttemptAccepted
		a.member.JoinedAt = c.clk.now()
	} else {
		if a.tx.cause == AttemptRecovery {
			a.tx.batch.DiscardAlternative(a.member)
		} else {
			c.s.RollbackMember(a.member)
		}
		if current && (reason == sourceReason || reason == ReasonDeadline) {
			r.Attribution = failureAttribution(reason, sourceReason, sourceErr)
		}
	}
	if _, ok := c.s.MemberPipes(a.member.ID, nil); ok {
		r.Member = a.member.ID
	}
	c.s.ReleaseJoins(a.reserve)
	a.view.Finished, a.view.Result = c.clk.source().Now(), r
	if r.ID != 0 {
		c.events.push(Event{Kind: EventRedialFinished, BindingGeneration: a.tx.generation, Batch: a.tx.id, Attempt: r.ID, Member: r.Member, Outcome: r.Reason})
		if current && a.observer != nil && a.view.Candidate != "" {
			if reason == ReasonConfirmed {
				a.observeHandshakeLocked()
			}
			a.observer.observeAttemptLocked(a.view, peerRetry)
		}
	}
	if a.work != nil {
		a.work.releaseKey()
	}
	a.deadlineTimer.Stop()
	a.deadlineTimer = nil
	if reason == ReasonDeadline {
		a.ctx.end(context.DeadlineExceeded)
	} else {
		a.ctx.end(context.Canceled)
	}
	c.s.NotifyCapacity()
	close(a.doneCh)
}

// Snapshot returns an independent copy of the attempt's stages and result.
func (a *Attempt) Snapshot() AttemptSnapshot {
	if a == nil {
		return AttemptSnapshot{}
	}
	var v AttemptSnapshot
	a.tx.c.peek(func() {
		v = a.view
		v.Attachments = slices.Clone(v.Attachments)
		v.Result.Pipes = slices.Clone(v.Result.Pipes)
		v.Result.Tickets = slices.Clone(v.Result.Tickets)
	})
	return v
}

// Wait observes a terminal result. Cancellation only stops the observation.
func (a *Attempt) Wait(ctx context.Context) (AttemptResult, error) {
	if a == nil {
		return AttemptResult{}, net.ErrClosed
	}
	select {
	case <-a.doneCh:
		return a.Snapshot().Result, nil
	case <-ctx.Done():
		select {
		case <-a.doneCh:
			return a.Snapshot().Result, nil
		default:
			return AttemptResult{}, ctx.Err()
		}
	}
}

// Cancel stops this attempt's unconfirmed work.
func (a *Attempt) Cancel() error {
	if a == nil {
		return net.ErrClosed
	}
	a.tx.c.locked(func() { a.finishLocked(ReasonCancelled, context.Canceled, 0) })
	return nil
}

func (a *Attempt) reservePhysical(ps []PipeIO) error {
	keys := make([]core.CarrierKeys, len(ps))
	c := a.tx.c
	inspectErr := hostCall(c.cfg.Logger, "physical carrier identity", func() error {
		for i, p := range ps {
			keys[i] = physicalCarrierKeys(p)
		}
		return nil
	})
	if inspectErr != nil {
		return inspectErr
	}
	var err error
	c.locked(func() {
		if a.done || a.tx.ctx.Err() != nil {
			err = context.Canceled
			return
		}
		if !c.clk.source().Now().Before(a.deadlineAt()) {
			a.finishLocked(ReasonDeadline, context.DeadlineExceeded, 0)
			err = context.DeadlineExceeded
			return
		}
		if !a.stillNeededLocked() {
			a.finishLocked(ReasonNoLongerNeeded, nil, 0)
			err = errRecoveryNoLongerNeeded
			return
		}
		for i, k := range keys {
			if !c.s.CanAcquireCarriers(k) {
				for _, acquired := range keys[:i] {
					c.s.ReleaseCarriers(acquired)
				}
				err = ErrCarrierCapacity
				return
			}
			c.s.AcquireCarriers(k)
		}
		a.physical = keys
	})
	return err
}

// Retain the stage and evidence separately from the terminal deadline. A
// timer firing, a policy refusal, or a cancelled host call is not a dial fault.
func failureAttribution(outcome, stage AttemptReason, err error) Attribution {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidConfig) ||
		errors.Is(err, ErrCarrierCapacity) || errors.Is(err, ErrCapacityBlocked) ||
		errors.Is(err, ErrSessionPolicyConflict) || errors.Is(err, ErrJoinAuthorizationChanged) || errors.Is(err, ErrTopologyExcluded) {
		return AttributionNone
	}
	var operation *net.OpError
	var physical *CarrierFaultError
	transport := errors.As(err, &operation) || errors.As(err, &physical)
	switch outcome {
	case ReasonDeadline:
		var timeout net.Error
		if (stage == ReasonDialFailed || stage == ReasonConfirmationFailed) && transport && errors.As(err, &timeout) && timeout.Timeout() {
			return AttributionCarrier
		}
	case ReasonDialFailed:
		return AttributionCarrier
	case ReasonJoinRefused, ReasonConfirmationFailed:
		if transport || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
			return AttributionCarrier
		}
	case ReasonHookFailed, ReasonCancelled, ReasonNoLongerNeeded, ReasonSessionClosed, ReasonConfirmed:
		return AttributionNone
	}
	return AttributionNone
}

func (a *Attempt) observeHandshakeLocked() {
	if a.view.HandshakeSucceeded || a.observer == nil {
		return
	}
	for _, x := range a.view.Attachments {
		if !x.ReplyAccepted {
			return
		}
	}
	a.view.HandshakeSucceeded = true
	ctl := a.observer.ctl
	if i, ok := ctl.index[a.view.Candidate]; ok {
		c := ctl.opts.Candidates[i]
		_ = ctl.cache.update(candKey(c.CacheKey, c), CacheUsable, CarrierObservation{At: a.tx.c.clk.source().Now()})
	}
}
