package zmux

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/core"
)

// GroupID identifies a group of quotas in a session target.
type GroupID string

// QuotaID identifies a quota in its group.
type QuotaID string

// PathPreference supplies the local direction's scheduling preference.
type PathPreference struct {
	Tier    uint64
	Standby bool
	Weight  uint64
}

// TargetCandidate names a registered candidate and per-direction preferences.
type TargetCandidate struct {
	Send, Receive *PathPreference
	ID            CandidateID
}

// MemberQuota describes interchangeable slots for complete members.
type MemberQuota struct {
	ID         QuotaID
	Candidates []TargetCandidate
	Count      int
	Directions Directions
}

// RotationPolicy bounds a member's service age or first carrier handoff bytes.
type RotationPolicy struct {
	MaxAge                  time.Duration
	SendBytes, ReceiveBytes uint64
}

// GroupTarget describes one ordered set of quotas and its rotation policy.
type GroupTarget struct {
	ID       GroupID
	Quotas   []MemberQuota
	Rotation RotationPolicy
}

// ReplaceIntent names members that must retire after replacements settle.
type ReplaceIntent struct {
	Observations []QualityReference
	ID           string
	Members      []MemberID
	MinEpoch     uint64
}

// SessionTarget is a versioned desired set of members.
type SessionTarget struct {
	Groups   []GroupTarget
	Replace  []ReplaceIntent
	Revision uint64
}

// ChoicePolicy selects the candidate ranking algorithm.
type ChoicePolicy uint8

const (
	// ChoiceStatic uses standby, tier and registration order for ordinary starts.
	ChoiceStatic ChoicePolicy = iota
	// ChoiceD160 ranks comparable candidates by the domain's cache.
	ChoiceD160
)

// ControllerLimits bounds actual host work and logical attempts.
type ControllerLimits struct {
	ConcurrentOpeners  int
	PendingOwners      int
	CleanupAttachments int
	AttemptTimeout     time.Duration
}

// ControllerOptions registers one isolated set of candidate resources.
type ControllerOptions struct {
	ClockDomain *ClockDomain
	Cache       *CarrierCache
	Domain      string
	// DialAfter sets the earliest host start for a candidate on Open and
	// supplies the initial schedule for managed sessions.
	DialAfter    map[CandidateID]time.Time
	Candidates   []Candidate
	Limits       ControllerLimits
	Choice       ChoicePolicy
	UpgradeRatio Change[float64]
}

const (
	controllerStagger               = 250 * time.Millisecond
	defaultOpeners                  = 4
	defaultPendingOwners            = 64
	defaultCleanupAttachments       = 8
	defaultControllerAttemptTimeout = 10 * time.Second
)

var (
	// ErrStaleBinding identifies a handle whose session has a different owner.
	ErrStaleBinding = errors.New("zmux: stale_binding")
	// ErrControllerBound indicates that a session or domain already has an owner.
	ErrControllerBound = errors.New("zmux: controller already bound")
	// ErrTargetConflict indicates an obsolete or conflicting target revision.
	ErrTargetConflict = errors.New("zmux: stale_target")
	// ErrNoEligibleCandidate indicates that the target has no usable candidate.
	ErrNoEligibleCandidate = errors.New("zmux: no_eligible_candidate")
	// ErrControllerExecution indicates exhaustion of the actual host work budget.
	ErrControllerExecution = errors.New("zmux: blocked_execution")
	// ErrControllerCleanup indicates that late carrier ownership is exhausted.
	ErrControllerCleanup = errors.New("zmux: blocked_cleanup")
	// ErrControllerQueueFull rejects a new owner when the pending queue is full.
	ErrControllerQueueFull = errors.New("zmux: queue_full")
	// ErrControllerDialAfter indicates a candidate blocked by the host's schedule.
	ErrControllerDialAfter  = errors.New("zmux: dial_after")
	errControllerStaleStart = errors.New("zmux: stale opener start reservation")
)

var controllerDomains = struct {
	active map[string]*SessionController
	sync.Mutex
}{active: make(map[string]*SessionController)}

// SessionController reconciles one or more sessions in the same domain.
type SessionController struct {
	ctx                context.Context
	index              map[CandidateID]int
	recoveryDirections map[CarrierCacheKey]Directions
	cache              *CarrierCache
	cancel             context.CancelFunc
	done               chan struct{}
	changed            chan struct{}
	changeSequence     uint64
	opts               ControllerOptions
	preparing          int
	controls           int
	work               int
	cleanup            int
	pending            []*controllerDemand
	mu                 sync.Mutex
	closed             bool
}

// NewSessionController registers candidates and binds the single domain owner.
func NewSessionController(opts ControllerOptions) (*SessionController, error) {
	if opts.Domain == "" || opts.Choice > ChoiceD160 || opts.Limits.ConcurrentOpeners < 0 || opts.Limits.PendingOwners < 0 || opts.Limits.CleanupAttachments < 0 ||
		opts.Limits.AttemptTimeout < 0 || opts.Limits.AttemptTimeout > time.Duration(core.MaxJoinDeadline) {
		return nil, ErrInvalidConfig
	}
	if ratio, ok := opts.UpgradeRatio.get(); ok {
		if !(ratio > 1) || math.IsInf(ratio, 0) {
			return nil, ErrInvalidConfig
		}
	} else {
		opts.UpgradeRatio = Set(1.25)
	}
	if opts.Limits.ConcurrentOpeners == 0 {
		opts.Limits.ConcurrentOpeners = defaultOpeners
	}
	if opts.Limits.PendingOwners == 0 {
		opts.Limits.PendingOwners = defaultPendingOwners
	}
	if opts.Limits.CleanupAttachments == 0 {
		opts.Limits.CleanupAttachments = defaultCleanupAttachments
	}
	if opts.Limits.AttemptTimeout == 0 {
		opts.Limits.AttemptTimeout = defaultControllerAttemptTimeout
	}
	opts.Candidates = slices.Clone(opts.Candidates)
	index := make(map[CandidateID]int, len(opts.Candidates))
	directions := make(map[CarrierCacheKey]Directions, len(opts.Candidates))
	for i, c := range opts.Candidates {
		key := candKey(c.CacheKey, c)
		_, duplicate := index[c.ID]
		if c.ID == "" || c.CacheKey == "" || c.Open == nil || c.Kind == 0 || int(c.Kind) >= len(carrierKindNames) || duplicate ||
			len(c.Attachments) > opts.Limits.CleanupAttachments {
			return nil, fmt.Errorf("%w: invalid candidate at index %d", ErrInvalidConfig, i)
		}
		if len(c.Attachments) != 0 {
			if _, err := (MemberSpec{Candidate: c.ID, Attachments: c.Attachments}).corePlan(); err != nil {
				return nil, err
			}
		}
		c.Attachments = slices.Clone(c.Attachments)
		opts.Candidates[i], index[c.ID] = c, i
		directions[key] |= candidateDirections(c)
	}
	opts.DialAfter = maps.Clone(opts.DialAfter)
	for id := range opts.DialAfter {
		if _, ok := index[id]; !ok {
			return nil, ErrNoEligibleCandidate
		}
	}
	switch {
	case opts.Cache == nil:
		var err error
		opts.Cache, err = NewCarrierCache(CarrierCacheOptions{ClockDomain: opts.ClockDomain})
		if err != nil {
			return nil, err
		}
	case opts.Cache.params == nil:
		return nil, fmt.Errorf("%w: initialize the controller cache with NewCarrierCache and set CarrierCacheOptions.ClockDomain", ErrInvalidConfig)
	case opts.Cache.clockDomain.resolved() != opts.ClockDomain.resolved():
		return nil, fmt.Errorf("%w: set CarrierCacheOptions.ClockDomain in NewCarrierCache to the controller's clock domain", ErrClockDomainMismatch)
	}
	controllerDomains.Lock()
	defer controllerDomains.Unlock()
	if controllerDomains.active[opts.Domain] != nil {
		return nil, ErrControllerBound
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctl := &SessionController{opts: opts, cache: opts.Cache, index: index, recoveryDirections: directions, ctx: ctx, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{})}
	controllerDomains.active[opts.Domain] = ctl
	return ctl, nil
}

func (ctl *SessionController) signalLocked() {
	ctl.changeSequence++
	close(ctl.changed)
	ctl.changed = make(chan struct{})
}

func (ctl *SessionController) watch() <-chan struct{} {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	return ctl.changed
}

// A demand is present at most once, including while its target is replaced.
type controllerDemand struct {
	billedOpeners, billedCleanup int // guarded by ctl.mu
	session                      *Conn
	queued                       bool
	permits                      controllerPermits
	starts                       dueStarts // owned by the scheduling goroutine
}

type controllerPermits struct {
	opener  bool
	cleanup int
}

type permitClasses uint8

const (
	executionPermit permitClasses = 1 << iota
	cleanupPermit
)

func (p controllerPermits) classes() (classes permitClasses) {
	if p.opener {
		classes |= executionPermit
	}
	if p.cleanup != 0 {
		classes |= cleanupPermit
	}
	return classes
}

func (ctl *SessionController) setDemandPermitsLocked(d *controllerDemand, p controllerPermits) {
	if d.permits != p {
		d.permits = p
		ctl.signalLocked()
	}
}

// grantPermitsLocked preserves queue order for each independently ready
// permit set. A request blocked behind a protected class cannot itself
// monopolize unrelated classes ahead of otherwise-ready work.
func (ctl *SessionController) grantPermitsLocked(d *controllerDemand, permits controllerPermits) error {
	if ctl.closed {
		return net.ErrClosed
	}
	if permits.cleanup > ctl.opts.Limits.CleanupAttachments {
		return ErrControllerCleanup
	}
	if err := ctl.queueLocked(d); err != nil {
		return err
	}
	ctl.setDemandPermitsLocked(d, permits)
	var reserved permitClasses
	for _, earlier := range ctl.pending {
		if earlier == d {
			break
		}
		classes := earlier.permits.classes()
		if classes&reserved == 0 {
			reserved |= classes
		}
	}
	contested := permits.classes() & reserved
	if permits.opener && (contested&executionPermit != 0 || ctl.work >= ctl.opts.Limits.ConcurrentOpeners) {
		return ErrControllerExecution
	}
	if contested&cleanupPermit != 0 || permits.cleanup > ctl.opts.Limits.CleanupAttachments-ctl.cleanup {
		return ErrControllerCleanup
	}
	if permits.opener {
		ctl.work++
		d.billedOpeners++
	}
	ctl.cleanup += permits.cleanup
	d.billedCleanup += permits.cleanup
	ctl.dropDemandLocked(d)
	return nil
}

func (ctl *SessionController) queueLocked(d *controllerDemand) error {
	if d.queued {
		return nil
	}
	if len(ctl.pending)+ctl.preparing >= ctl.opts.Limits.PendingOwners {
		return ErrControllerQueueFull
	}
	d.queued = true
	ctl.pending = append(ctl.pending, d)
	ctl.signalLocked()
	return nil
}

func (ctl *SessionController) dropDemandLocked(d *controllerDemand) {
	if !d.queued {
		return
	}
	i := slices.Index(ctl.pending, d)
	ctl.pending = slices.Delete(ctl.pending, i, i+1)
	d.queued = false
	d.permits = controllerPermits{}
	ctl.signalLocked()
	ctl.finishLocked()
}

func (ctl *SessionController) dropDemand(d *controllerDemand) {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	ctl.dropDemandLocked(d)
}

func sharedControllerBlock(err error) bool {
	return errors.Is(err, ErrControllerExecution) || errors.Is(err, ErrControllerCleanup)
}

func (ctl *SessionController) finishLocked() {
	if !ctl.closed || ctl.preparing != 0 || ctl.controls != 0 || ctl.work != 0 || ctl.cleanup != 0 || len(ctl.pending) != 0 {
		return
	}
	select {
	case <-ctl.done:
		return
	default:
	}
	controllerDomains.Lock()
	delete(controllerDomains.active, ctl.opts.Domain)
	controllerDomains.Unlock()
	close(ctl.done)
}

// Close cancels the controller's uncommitted work and releases its bindings.
func (ctl *SessionController) Close() error {
	if ctl == nil {
		return net.ErrClosed
	}
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	if !ctl.closed {
		ctl.closed = true
		ctl.cancel()
		ctl.signalLocked()
	}
	ctl.finishLocked()
	return nil
}

// Done closes after all controller work has returned.
func (ctl *SessionController) Done() <-chan struct{} {
	if ctl == nil {
		return closedChan
	}
	return ctl.done
}

func (ctl *SessionController) candidate(id CandidateID) Candidate {
	if i, ok := ctl.index[id]; ok {
		return ctl.opts.Candidates[i]
	}
	return Candidate{}
}

func (ctl *SessionController) order(allowed []CandidateID, boot bool) []Candidate {
	list := make([]Candidate, 0, len(ctl.opts.Candidates))
	for _, c := range ctl.opts.Candidates {
		if boot && (!c.CanStart || !c.bootstrapLayout(Both)) || !boot && len(c.Attachments) == 0 ||
			len(allowed) != 0 && !slices.Contains(allowed, c.ID) {
			continue
		}
		list = append(list, c)
	}
	scores := make(map[CandidateID]CarrierScore, len(list))
	if ctl.opts.Choice == ChoiceD160 {
		for _, c := range list {
			scores[c.ID] = ctl.cache.Score(c.CacheKey, c.Kind, c.Variant)
		}
	}
	slices.SortStableFunc(list, func(a, b Candidate) int {
		if a.Standby != b.Standby {
			if a.Standby {
				return 1
			}
			return -1
		}
		if n := cmp.Compare(a.Tier, b.Tier); n != 0 {
			return n
		}
		return cmp.Compare(carrierScoreOrder(scores[b.ID].Score), carrierScoreOrder(scores[a.ID].Score))
	})
	return list
}

func (ctl *SessionController) acquire(c Candidate, intent AttemptCause, demand *controllerDemand, attachments int) (*controllerWork, time.Time, error) {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	key := candKey(c.CacheKey, c)
	if ctl.closed {
		ctl.suppressStart(demand, key, time.Time{}, net.ErrClosed)
		return nil, time.Time{}, net.ErrClosed
	}
	var reservedAt time.Time
	token, next, err := ctl.cache.admitChecked(key, intent, demand.session, func() error {
		if err := ctl.grantPermitsLocked(demand, controllerPermits{opener: true, cleanup: attachments}); err != nil {
			return err
		}
		reservedAt = ctl.opts.ClockDomain.clockSource().Now()
		return nil
	})
	if err != nil {
		if !sharedControllerBlock(err) {
			ctl.setDemandPermitsLocked(demand, controllerPermits{})
		}
		ctl.suppressStart(demand, key, next, err)
		return nil, next, err
	}
	return &controllerWork{ctl: ctl, key: token, demand: demand, attachments: attachments, reservedAt: reservedAt}, time.Time{}, nil
}

type controllerWork struct {
	reservedAt         time.Time
	ctl                *SessionController
	round              *recoveryRound
	key                keyReservation
	demand             *controllerDemand
	terminal, returned bool // guarded by ctl.mu
	cleanupOnce        sync.Once
	attachments        int
}

// The attempt and its callback may finish in either order.
func (w *controllerWork) releaseKey() {
	w.ctl.mu.Lock()
	defer w.ctl.mu.Unlock()
	if !w.terminal {
		w.terminal = true
		if w.returned {
			w.key.release()
			w.ctl.signalLocked()
		}
	}
}

func (w *controllerWork) releaseWork() {
	w.ctl.mu.Lock()
	defer w.ctl.mu.Unlock()
	if w.returned {
		return
	}
	w.returned = true
	if w.terminal {
		w.key.release()
	}
	w.ctl.work--
	w.demand.billedOpeners--
	w.ctl.signalLocked()
	w.ctl.finishLocked()
}

func (w *controllerWork) releaseCleanup() {
	w.cleanupOnce.Do(func() { w.ctl.releaseCleanup(w.attachments, w.round, w.demand) })
}

func (ctl *SessionController) releaseCleanup(attachments int, round *recoveryRound, billed *controllerDemand) {
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	if round != nil {
		round.cleanupSignals++
	}
	ctl.cleanup -= attachments
	billed.billedCleanup -= attachments
	ctl.signalLocked()
	ctl.finishLocked()
}

func (ctl *SessionController) validateTarget(t SessionTarget) (SessionTarget, error) {
	t.Groups = slices.Clone(t.Groups)
	t.Replace = slices.Clone(t.Replace)
	for i, g := range t.Groups {
		if g.ID == "" || g.Rotation.MaxAge < 0 {
			return t, ErrInvalidConfig
		}
		g.Quotas = slices.Clone(g.Quotas)
		for j, q := range g.Quotas {
			if q.ID == "" || q.Count < 0 || q.Directions == 0 || q.Directions&^Both != 0 || q.Count != 0 && len(q.Candidates) == 0 {
				return t, ErrInvalidConfig
			}
			q.Candidates = slices.Clone(q.Candidates)
			for k, candidate := range q.Candidates {
				if _, ok := ctl.index[candidate.ID]; !ok {
					return t, ErrNoEligibleCandidate
				}
				if candidate.Send != nil {
					candidate.Send = new(*candidate.Send)
				}
				if candidate.Receive != nil {
					candidate.Receive = new(*candidate.Receive)
				}
				q.Candidates[k] = candidate
			}
			g.Quotas[j] = q
		}
		slices.SortFunc(g.Quotas, func(a, b MemberQuota) int { return cmp.Compare(a.ID, b.ID) })
		for j := 1; j < len(g.Quotas); j++ {
			if g.Quotas[j-1].ID == g.Quotas[j].ID {
				return t, ErrInvalidConfig
			}
		}
		t.Groups[i] = g
	}
	slices.SortFunc(t.Groups, func(a, b GroupTarget) int { return cmp.Compare(a.ID, b.ID) })
	for i := 1; i < len(t.Groups); i++ {
		if t.Groups[i-1].ID == t.Groups[i].ID {
			return t, ErrInvalidConfig
		}
	}
	for i, r := range t.Replace {
		if r.ID == "" || len(r.Members) == 0 {
			return t, ErrInvalidConfig
		}
		r.Observations = slices.Clone(r.Observations)
		r.Members = slices.Clone(r.Members)
		t.Replace[i] = r
	}
	return t, nil
}

func hasSessionTarget(t SessionTarget) bool {
	return len(t.Replace) != 0 || slices.ContainsFunc(t.Groups, func(g GroupTarget) bool {
		return slices.ContainsFunc(g.Quotas, func(q MemberQuota) bool { return q.Count > 0 })
	})
}

// SessionControl binds a session's versioned target to its controller.
type SessionControl struct {
	identity      *byte
	reservation   *controllerWork
	ctx           context.Context
	feedbackTimer timeshim.Timer
	ctl           *SessionController
	c             *Conn
	cancel        context.CancelFunc
	stopEnd       func() bool
	done          chan struct{}
	preferences   map[MemberID]installedPreference
	feedback      map[MemberID]*memberFeedback
	batch         *CapacityChange
	// The connection owner guards the target and current batch.
	target       SessionTarget
	view         ControllerSnapshot
	dialAfter    map[CandidateID]time.Time
	demand       controllerDemand
	recoveryNeed Directions
	lastGroup    GroupID // written by run after a grant
}

// BindingCause identifies the commit which installed the target.
type BindingCause string

// Target publication causes.
const (
	BindingManage    BindingCause = "manage"
	BindingTakeOver  BindingCause = "takeover"
	BindingSetTarget BindingCause = "set_target"
)

// ControllerSnapshot combines one session observation and its current plan.
type ControllerSnapshot struct {
	BindingGeneration, PreviousGeneration uint64
	BindingCause                          BindingCause
	NextRetry                             time.Time
	Block                                 error
	LastRefusal                           error // latest refusal at the opener fence
	Attempts                              []AttemptID
	Capacity                              CapacitySnapshot
	TargetRevision                        uint64
	Unfilled                              int
	ExecutingOpeners, CleanupAttachments  int
}

// ControllerDomainSnapshot observes shared limits and pending host work.
type ControllerDomainSnapshot struct {
	ProvisionalEntries int
	Changed            <-chan struct{}
	Limits             ControllerLimits
	RaceStagger        time.Duration
	PendingOwners      int
	ExecutingOpeners   int
	CleanupAttachments int
}

// Snapshot observes domain resources at one commit and subscribes for changes.
func (ctl *SessionController) Snapshot() ControllerDomainSnapshot {
	if ctl == nil {
		return ControllerDomainSnapshot{Changed: closedChan}
	}
	ctl.mu.Lock()
	defer ctl.mu.Unlock()
	return ControllerDomainSnapshot{
		ProvisionalEntries: ctl.preparing, Changed: ctl.changed, Limits: ctl.opts.Limits, RaceStagger: controllerStagger,
		PendingOwners: len(ctl.pending), ExecutingOpeners: ctl.work, CleanupAttachments: ctl.cleanup,
	}
}

// ManageOptions controls how an established session is registered.
type ManageOptions struct {
	// BootstrapCandidate optionally registers the existing pipe 0 member
	// as this candidate. Its carrier kind and variant must match. Manage
	// returns ErrNoEligibleCandidate for an unregistered ID and
	// ErrInvalidConfig for an incompatible carrier or conflicting binding.
	BootstrapCandidate CandidateID
}

// Manage binds an established multipath session to a target.
// At most one options value may be supplied.
func (ctl *SessionController) Manage(c *Conn, initial SessionTarget, options ...ManageOptions) (*SessionControl, error) {
	return ctl.bind(c, nil, initial, options...)
}

// TakeOver transfers management of the session identified by its current handle.
func (ctl *SessionController) TakeOver(previous *SessionControl, initial SessionTarget, options ...ManageOptions) (*SessionControl, error) {
	if previous == nil {
		return nil, ErrStaleBinding
	}
	return ctl.bind(previous.c, previous, initial, options...)
}

func (ctl *SessionController) bind(c *Conn, previous *SessionControl, initial SessionTarget, options ...ManageOptions) (*SessionControl, error) {
	if ctl == nil || c == nil {
		return nil, net.ErrClosed
	}
	if c.clk.domain != ctl.opts.ClockDomain.resolved() {
		return nil, ErrClockDomainMismatch
	}
	var boot CandidateID
	switch len(options) {
	case 0:
	case 1:
		boot = options[0].BootstrapCandidate
	default:
		return nil, fmt.Errorf("%w: Manage takes at most one options value", ErrInvalidConfig)
	}
	target, err := ctl.validateTarget(initial)
	if err != nil {
		return nil, err
	}
	// Reserve new-domain admission before the final session comparison.
	// The reservation has no right to start an opener.
	provisional := previous != nil
	if provisional {
		ctl.mu.Lock()
		if ctl.closed {
			ctl.mu.Unlock()
			return nil, net.ErrClosed
		}
		if ctl.preparing+len(ctl.pending) >= ctl.opts.Limits.PendingOwners {
			ctl.mu.Unlock()
			return nil, ErrControllerQueueFull
		}
		ctl.preparing++
		ctl.signalLocked()
		ctl.mu.Unlock()
		defer func() {
			if provisional {
				ctl.mu.Lock()
				ctl.preparing--
				ctl.signalLocked()
				ctl.finishLocked()
				ctl.mu.Unlock()
			}
		}()
	}
	c.lock()
	defer c.unlock()
	switch {
	case c.closing || c.s.State() >= core.StateClosing:
		return nil, c.errLocked()
	case !c.s.Features().Multipath:
		return nil, ErrNotNegotiated
	case previous == nil && c.control != nil:
		return nil, ErrControllerBound
	case c.control != previous:
		return nil, ErrStaleBinding
	case previous != nil && previous.ctl == ctl:
		return nil, ErrInvalidConfig
	case c.bindingGeneration == math.MaxUint64:
		return nil, ErrInvalidConfig
	}
	if slices.ContainsFunc(target.Replace, func(r ReplaceIntent) bool { return len(r.Observations) > 0 }) {
		return nil, ErrStaleDecision
	}
	if err := ctl.validateBindingLocked(c, target); err != nil {
		return nil, err
	}
	ctl.mu.Lock()
	if ctl.closed {
		ctl.mu.Unlock()
		return nil, net.ErrClosed
	}
	if provisional {
		ctl.preparing--
		provisional = false
		ctl.signalLocked()
	}
	sc := &SessionControl{identity: new(byte), ctl: ctl, c: c, done: make(chan struct{}), target: target, dialAfter: maps.Clone(ctl.opts.DialAfter)}
	if hasSessionTarget(target) {
		if err := ctl.queueLocked(&sc.demand); err != nil {
			ctl.mu.Unlock()
			return nil, err
		}
	}
	if boot != "" {
		if err := ctl.bindBootstrapLocked(c, boot); err != nil {
			ctl.dropDemandLocked(&sc.demand)
			ctl.mu.Unlock()
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctl.ctx)
	sc.ctx, sc.cancel = ctx, cancel
	sc.demand.session = c
	sc.stopEnd = context.AfterFunc(c.ctx, cancel)
	ctl.controls++
	c.control = sc
	ctl.mu.Unlock()
	c.bindingGeneration++
	sc.view.BindingGeneration = c.bindingGeneration
	sc.view.BindingCause = BindingManage
	if previous != nil {
		sc.view.PreviousGeneration = previous.view.BindingGeneration
		sc.view.BindingCause = BindingTakeOver
		if previous.ctl.cache == ctl.cache &&
			reflect.DeepEqual(previous.target.Groups, target.Groups) &&
			reflect.DeepEqual(previous.target.Replace, target.Replace) {
			previous.demand.starts.transfer(&sc.demand.starts, target.Revision)
		}
		previous.observeQualityLocked()
		previous.cancel()
		previous.cancelBatchesLocked(nil)
		previous.observeMembersLocked(c.s.TakeMemberChanges())
		previous.ctl.dropDemand(&previous.demand)
		previous.transferFeedbackLocked(sc)
		close(previous.done)
	}
	sc.updateRecoveryLocked()
	if previous != nil {
		previous.withdrawRecoveryLocked(sc)
	}
	c.s.EnableMemberFeedback()
	for _, m := range c.s.Members(nil) {
		if candidate := ctl.candidate(CandidateID(m.Candidate)); candidate.ID != "" {
			if member := c.s.CandidateIdentity(m.ID); member != nil {
				candidate.recordAttribution(member)
			}
		}
		if m.Complete && sc.feedback[m.ID] == nil {
			sc.trackMemberLocked(m.ID, CandidateID(m.Candidate), m.Expected, m.Covered, m.Stable)
		}
	}
	sc.publishTargetLocked()
	c.s.NotifyCapacity()
	go sc.run()
	return sc, nil
}

// bindBootstrapLocked validates the host's attribution before using the same
// member update as a controller-opened bootstrap.
func (ctl *SessionController) bindBootstrapLocked(c *Conn, id CandidateID) error {
	index, ok := ctl.index[id]
	if !ok {
		return fmt.Errorf("%w: bootstrap candidate %q is not registered", ErrNoEligibleCandidate, id)
	}
	member := c.s.MemberOf(0)
	carrier := c.pipes[0]
	if member == 0 || carrier == nil {
		return fmt.Errorf("%w: no bootstrap member for candidate %q", ErrUnknownMember, id)
	}
	candidate := ctl.opts.Candidates[index]
	actual := &carrier.hc.io
	if actual.Carrier != candidate.Kind || actual.Variant != candidate.Variant {
		return fmt.Errorf("%w: bootstrap candidate %q uses %v variant %q, pipe 0 uses %v variant %q",
			ErrInvalidConfig, id, candidate.Kind, candidate.Variant, actual.Carrier, actual.Variant)
	}
	for _, m := range c.s.Members(nil) {
		if m.ID != member {
			continue
		}
		if !m.Complete || m.Draining {
			return fmt.Errorf("%w: bootstrap member is not confirmed and available", ErrInvalidConfig)
		}
		if !candidate.bootstrapLayout(m.Expected) {
			return ErrInvalidConfig
		}
		if m.Candidate == string(id) {
			if actual := c.s.CandidateIdentity(m.ID); actual != nil && actual.AttributionLayout != "" && !ctl.indexedMember(c.s, m.ID) {
				return ErrStaleDecision
			}

			return nil
		}
		if m.Candidate != "" {
			return fmt.Errorf("%w: bootstrap member already belongs to candidate %q", ErrInvalidConfig, m.Candidate)
		}
	}
	c.s.SetNow(c.clk.now())
	c.s.SetBootstrapCandidate(string(id))
	return nil
}

// SetTarget atomically installs a validated replacement target.
func (sc *SessionControl) SetTarget(target SessionTarget) error {
	if sc == nil {
		return net.ErrClosed
	}
	sc.c.lock()
	defer sc.c.unlock()
	if sc.c.control != sc {
		return ErrStaleBinding
	}
	if sc.ctx.Err() != nil {
		return net.ErrClosed
	}
	target, err := sc.ctl.validateTarget(target)
	if err != nil {
		return err
	}
	switch {
	case target.Revision < sc.target.Revision:
		return fmt.Errorf("%w: current revision is %d", ErrTargetConflict, sc.target.Revision)
	case target.Revision == sc.target.Revision:
		if reflect.DeepEqual(target, sc.target) {
			return nil
		}
		return fmt.Errorf("%w: current revision is %d", ErrTargetConflict, sc.target.Revision)
	}
	if err := sc.checkQualityLocked(target, true); err != nil {
		return err
	}
	if err := sc.ctl.validateBindingLocked(sc.c, target); err != nil {
		return err
	}
	sc.ctl.mu.Lock()
	if hasSessionTarget(target) {
		if err := sc.ctl.queueLocked(&sc.demand); err != nil {
			sc.ctl.mu.Unlock()
			return err
		}
		sc.ctl.setDemandPermitsLocked(&sc.demand, controllerPermits{})
	} else {
		sc.ctl.dropDemandLocked(&sc.demand)
	}
	sc.ctl.mu.Unlock()
	var keepBatch *CapacityChange
	if sc.renewsReplacementLocked(target) {
		keepBatch = sc.batch
		keepBatch.targetRevision = target.Revision
	}
	sc.target = target
	sc.view.BindingCause = BindingSetTarget
	sc.view.PreviousGeneration = 0
	sc.publishTargetLocked()
	sc.cancelBatchesLocked(keepBatch)
	sc.c.s.NotifyCapacity()
	return nil
}

// Refreshing only the evidence for the same retirement preserves a confirmed
// replacement and its stability interval. Other target edits cancel the batch.
func (sc *SessionControl) renewsReplacementLocked(next SessionTarget) bool {
	b := sc.batch
	if b == nil || b.cause != AttemptReplace || len(b.attempts) == 0 || !reflect.DeepEqual(sc.target.Groups, next.Groups) || len(next.Replace) == 0 || len(next.Replace) != len(sc.target.Replace) {
		return false
	}
	for _, a := range b.attempts {
		if a == nil || !a.done || a.view.Result.Reason != ReasonConfirmed {
			return false
		}
	}
	for i, old := range sc.target.Replace {
		fresh := next.Replace[i]
		if old.ID != fresh.ID || old.MinEpoch != fresh.MinEpoch || !slices.Equal(old.Members, fresh.Members) {
			return false
		}
	}
	return true
}

// SetDialAfter changes a candidate's earliest start for this session. A zero
// time clears the restriction. It has no effect on a running opener.
func (sc *SessionControl) SetDialAfter(candidate CandidateID, after time.Time) error {
	if sc == nil {
		return net.ErrClosed
	}
	if _, ok := sc.ctl.index[candidate]; !ok {
		return ErrNoEligibleCandidate
	}
	sc.c.lock()
	defer sc.c.unlock()
	if sc.c.control != sc {
		return ErrStaleBinding
	}
	if sc.ctx.Err() != nil {
		return net.ErrClosed
	}
	if after.IsZero() {
		delete(sc.dialAfter, candidate)
	} else {
		if sc.dialAfter == nil {
			sc.dialAfter = make(map[CandidateID]time.Time)
		}
		sc.dialAfter[candidate] = after
	}
	sc.c.s.NotifyCapacity()
	return nil
}

// Snapshot captures target progress and subscribes to its next commit.
func (sc *SessionControl) Snapshot() (ControllerSnapshot, error) {
	if sc == nil {
		return ControllerSnapshot{}, net.ErrClosed
	}
	var v ControllerSnapshot
	sc.c.peek(func() {
		v = sc.view
		sc.ctl.mu.Lock()
		v.ExecutingOpeners, v.CleanupAttachments = sc.demand.billedOpeners, sc.demand.billedCleanup
		sc.ctl.mu.Unlock()
		v.TargetRevision = sc.target.Revision
		v.Capacity = sc.c.capacityLocked()
		if tx := sc.batch; tx != nil {
			for _, a := range tx.attempts {
				if a != nil && a.view.ID != 0 && !a.done {
					v.Attempts = append(v.Attempts, a.view.ID)
				}
			}
		}
	})
	return v, nil
}

// Release ends management if this handle still owns the binding.
func (sc *SessionControl) Release() error {
	if sc == nil {
		return net.ErrClosed
	}
	sc.c.lock()
	defer sc.c.unlock()
	if sc.c.control != sc {
		return ErrStaleBinding
	}
	sc.detachLocked()
	return nil
}

func (sc *SessionControl) detachLocked() {
	sc.cancel()
	sc.c.control = nil
	sc.cancelBatchesLocked(nil)
	sc.observeQualityLocked()
	sc.observeMembersLocked(sc.c.s.TakeMemberChanges())
	sc.stopFeedbackLocked()
	sc.withdrawRecoveryLocked(nil)
	sc.ctl.dropDemand(&sc.demand)
	close(sc.done)
	sc.c.s.DisableMemberFeedback()
	sc.c.s.NotifyCapacity()
}

func (sc *SessionControl) publishTargetLocked() {
	v := sc.view
	sc.c.events.push(Event{Kind: EventTargetChanged, Revision: sc.target.Revision, BindingGeneration: v.BindingGeneration, PreviousGeneration: v.PreviousGeneration, BindingCause: v.BindingCause})
}

// Done closes after the binding is removed.
func (sc *SessionControl) Done() <-chan struct{} {
	if sc == nil {
		return closedChan
	}
	return sc.done
}

func (sc *SessionControl) wait(ch, domain, cache <-chan struct{}, until time.Time) {
	var timer *timeshim.ChanTimer
	var tick <-chan time.Time
	if !until.IsZero() {
		clk := sc.c.clk.source()
		timer = timeshim.NewTimer(clk, max(until.Sub(clk.Now()), 0))
		defer timer.Stop()
		tick = timer.C
	}
	select {
	case <-sc.ctx.Done():
	case <-ch:
	case <-domain:
	case <-cache:
	case <-tick:
	}
}

func (sc *SessionControl) currentLocked(revision uint64) bool {
	return sc.c.control == sc && sc.target.Revision == revision
}

func (sc *SessionControl) cancelBatchesLocked(keep *CapacityChange) {
	if w := sc.reservation; w != nil {
		w.releaseKey()
		w.releaseWork()
		w.releaseCleanup()
		sc.reservation = nil
	}
	for _, tx := range slices.Clone(sc.c.changes) {
		if tx.owner == sc && tx != keep {
			tx.cancelLocked()
		}
	}
}

func (ctl *SessionController) validateBindingLocked(c *Conn, t SessionTarget) error {
	remaining := c.s.SessionPolicy().Limits.Envelope()
	for _, g := range t.Groups {
		for _, q := range g.Quotas {
			least := math.MaxInt
			for _, choice := range q.Candidates {
				candidate := ctl.candidate(choice.ID)
				dirs, n := candidateDirections(candidate), len(candidate.Attachments)
				if n == 0 && candidate.CanStart {
					dirs, n = Both, 1
				}
				if dirs&q.Directions != q.Directions {
					return ErrNoDirectionCoverage
				}
				least = min(least, n)
			}
			if q.Count != 0 {
				if q.Count > remaining/least {
					return ErrCapacityBlocked
				}
				remaining -= q.Count * least
			}
		}
	}
	for _, m := range c.s.Members(nil) {
		id := CandidateID(m.Candidate)
		if _, ok := ctl.index[id]; !ok {
			continue
		}
		actual := c.s.CandidateIdentity(m.ID)
		if actual == nil || actual.AttributionLayout == "" {
			continue
		}
		if !ctl.indexedMember(c.s, m.ID) {
			return ErrInvalidConfig
		}
	}
	return nil
}

func (c Candidate) bootstrapLayout(directions Directions) bool {
	return len(c.Attachments) == 0 && directions == Both ||
		len(c.Attachments) == 1 && c.Attachments[0].Directions == directions
}

func (c Candidate) registeredLayout() string {
	layout := make([]byte, len(c.Attachments))
	for i := range c.Attachments {
		layout[i] = byte(c.Attachments[i].Directions)
	}
	if len(layout) == 0 {
		return string([]byte{byte(Both)})
	}
	return string(layout)
}

func (c Candidate) recordAttribution(m *core.Member) {
	m.AttributionKey, m.AttributionKind, m.AttributionVariant, m.AttributionLayout = c.CacheKey, uint32(c.Kind), c.Variant, c.registeredLayout()
}

func (ctl *SessionController) indexedMember(s *core.Session, id MemberID) bool {
	m := s.CandidateIdentity(id)
	if m == nil {
		return false
	}
	c := ctl.candidate(CandidateID(m.Candidate))
	return c.ID != "" && m.AttributionKey == c.CacheKey && m.AttributionKind == uint32(c.Kind) && m.AttributionVariant == c.Variant && m.AttributionLayout == c.registeredLayout()
}
