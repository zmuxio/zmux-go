package zmux

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

type bootstrapResource struct {
	attempt   context.Context
	cancel    context.CancelFunc
	stop      func() bool
	candidate Candidate
	err       error
	work      *controllerWork
	endLife   context.CancelFunc
	pipe      PipeIO
}

func (b bootstrapResource) discard() {
	b.releaseAttempt()
	closePipe(b.pipe)
	b.endLife()
	b.work.releaseCleanup()
}

func (b bootstrapResource) releaseAttempt() {
	b.stop()
	b.cancel()
	b.work.releaseKey()
}

// bootstrapStarts serializes the invocation fence with selection of a winner.
type bootstrapStarts struct {
	mu     sync.Mutex
	next   AttemptID
	closed bool
}

func (b *bootstrapStarts) start(ctx context.Context, clk *ClockDomain, token keyReservation) (AttemptID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if deadline, ok := ctx.Deadline(); ok && !clk.clockSource().Now().Before(deadline) {
		return 0, context.DeadlineExceeded
	}
	if b.closed {
		return 0, context.Canceled
	}
	if b.next == ^AttemptID(0) {
		return 0, ErrAttemptIDExhausted
	}
	if err := token.start(); err != nil {
		return 0, err
	}
	b.next++
	return b.next, nil
}

func (b *bootstrapStarts) finish() AttemptID {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return b.next
}

type bootstrapResult struct {
	err      error
	c        *Conn
	resource bootstrapResource
}

// Open prepares candidates in bounded staggered order and runs one protocol
// bootstrap at a time. Successful resources transfer directly to the session.
func (ctl *SessionController) Open(ctx context.Context, cfg *Config, initial SessionTarget) (*Conn, *SessionControl, error) {
	if ctl == nil {
		return nil, nil, net.ErrClosed
	}
	cfg = cfg.Clone()
	if cfg.ClockDomain != nil && cfg.ClockDomain.resolved() != ctl.opts.ClockDomain.resolved() {
		return nil, nil, ErrClockDomainMismatch
	}
	target, err := ctl.validateTarget(initial)
	if err != nil {
		return nil, nil, err
	}
	cfg.ClockDomain = ctl.opts.ClockDomain
	cfg.EarlyData, cfg.ReturnBeforeEstablished = EarlyData{}, false
	c, err := ctl.openBootstrap(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	if !c.ConnectionState().Multipath {
		if hasSessionTarget(target) {
			c.Abort(0, "multipath not negotiated")
			return nil, nil, ErrNotNegotiated
		}
		return c, nil, nil
	}
	sc, err := ctl.Manage(c, target)
	if err != nil {
		c.Abort(0, "controller setup")
		return nil, nil, err
	}
	return c, sc, nil
}

func (ctl *SessionController) openBootstrap(ctx context.Context, cfg *Config) (*Conn, error) {
	var demand controllerDemand
	ctl.mu.Lock()
	if ctl.closed {
		ctl.mu.Unlock()
		return nil, net.ErrClosed
	}
	err := ctl.queueLocked(&demand)
	ctl.mu.Unlock()
	if err != nil {
		return nil, err
	}
	defer ctl.dropDemand(&demand)
	clk := cfg.ClockDomain.clockSource()
	setup, cancel := timeshim.WithTimeout(ctx, clk, cfg.handshakeTimeout())
	defer cancel()
	stop := context.AfterFunc(ctl.ctx, cancel)
	defer stop()
	var starts bootstrapStarts
	defer starts.finish()
	candidates := ctl.order(nil, true)
	prepared := make(chan bootstrapResource)
	handshake := make(chan bootstrapResult)
	used := make(map[CandidateID]bool, len(candidates))
	var queue []bootstrapResource
	defer func() {
		for _, resource := range queue {
			resource.discard()
		}
	}()
	var (
		startAfter time.Time
		tails      int
		booting    bool
		outcome    error
	)
	for setup.Err() == nil {
		domain := ctl.watch()
		now := clk.Now()
		var next time.Time
		var blocked error
		if !now.Before(startAfter) {
			for _, c := range candidates {
				if used[c.ID] {
					continue
				}
				after := ctl.opts.DialAfter[c.ID]
				if now.Before(after) {
					ctl.suppressStart(&demand, candKey(c.CacheKey, c), after, ErrControllerDialAfter)
					next = earliest(next, after)
					continue
				}
				work, at, err := ctl.acquire(c, AttemptBootstrap, &demand, 1, time.Time{})
				if err != nil {
					next = earliest(next, at)
					if sharedControllerBlock(err) {
						blocked = err
						break
					}
					if errors.Is(err, ErrControllerQueueFull) {
						return nil, err
					}
					continue
				}
				tails++
				go ctl.prepareBootstrap(setup, cfg, c, work, &starts, prepared)
				used[c.ID] = true
				startAfter = now.Add(controllerStagger)
				break
			}
		}
		if blocked == nil {
			ctl.dropDemand(&demand)
		}
		if len(used) < len(candidates) && now.Before(startAfter) {
			next = earliest(next, startAfter)
		}
		if !booting && len(queue) != 0 {
			resource := queue[0]
			queue = queue[1:]
			booting = true
			go ctl.bootstrap(setup, cfg, resource, handshake)
		}
		if tails == 0 && !booting && len(queue) == 0 && len(used) == len(candidates) {
			return nil, errors.Join(ErrNoEligibleCandidate, outcome)
		}
		var timer *timeshim.ChanTimer
		var tick <-chan time.Time
		if !next.IsZero() {
			timer = timeshim.NewTimer(clk, max(next.Sub(now), 0))
			tick = timer.C
		}
		select {
		case <-setup.Done():
		case <-domain:
		case <-tick:
		case ready := <-prepared:
			tails--
			if ready.err == nil {
				queue = append(queue, ready)
			} else {
				outcome, startAfter = ready.err, time.Time{}
			}
		case result := <-handshake:
			booting = false
			if result.err == nil {
				if timer != nil {
					timer.Stop()
				}
				result.c.locked(func() {
					result.c.nextAttempt = starts.finish()
					result.c.s.SetBootstrapCandidate(string(result.resource.candidate.ID))
				})
				return result.c, nil
			}
			outcome, startAfter = result.err, time.Time{}
		}
		if timer != nil {
			timer.Stop()
		}
	}
	return nil, setup.Err()
}

func (ctl *SessionController) prepareBootstrap(setup context.Context, cfg *Config, candidate Candidate, work *controllerWork, starts *bootstrapStarts, results chan<- bootstrapResource) {
	life, endLife := context.WithCancel(context.WithoutCancel(setup))
	deadline := boundedAttemptDeadline(setup, work.reservedAt, work.reservedAt, ctl.opts.Limits.AttemptTimeout)
	attempt, cancel := timeshim.WithDeadline(setup, cfg.ClockDomain.clockSource(), deadline)
	stop := context.AfterFunc(attempt, work.releaseKey)
	req := OpenRequest{Cause: AttemptBootstrap, Need: Both, Stage: OpenBootstrap,
		Layout: []AttachmentSpec{{Directions: Both}}, Credential: cfg.CarrierCredential, Lifetime: life}
	started := false
	ps, panicked, err := invokeOpener(attempt, cfg, func(ctx context.Context, r OpenRequest) ([]PipeIO, error) {
		id, err := starts.start(attempt, cfg.ClockDomain, work.key)
		if err != nil {
			return nil, err
		}
		work.demand.starts.started(candKey(candidate.CacheKey, candidate))
		r.Attempt, started = id, true
		return openCandidate(ctx, r, candidate)
	}, req)
	work.releaseWork()
	if err == nil {
		err = attempt.Err()
	}
	if err == nil {
		err = validateOpened(ps, 1)
	}
	ready := bootstrapResource{candidate: candidate, work: work, endLife: endLife, err: err,
		attempt: attempt, cancel: cancel, stop: stop}
	if err == nil {
		ready.pipe = ps[0]
	} else {
		key := candKey(candidate.CacheKey, candidate)
		if !started {
			ctl.suppressStart(work.demand, key, time.Time{}, err)
		} else if !panicked && attempt.Err() == nil && !errors.Is(err, ErrInvalidConfig) {
			ctl.reportAttemptFailure(key, peerRetryDelay(err))
		}
		ready.releaseAttempt()
		closeReturned(ps)
		endLife()
		work.releaseCleanup()
	}
	select {
	case results <- ready:
	case <-setup.Done():
		if ready.err == nil {
			ready.discard()
		}
	}
}

func (ctl *SessionController) bootstrap(ctx context.Context, cfg *Config, resource bootstrapResource, results chan<- bootstrapResult) {
	c, err := Start(resource.attempt, resource.pipe, RoleClient, cfg)
	if deadline, ok := resource.attempt.Deadline(); err == nil && ok && !cfg.ClockDomain.clockSource().Now().Before(deadline) {
		c.Abort(0, "bootstrap deadline")
		err = context.DeadlineExceeded
	}
	key := candKey(resource.candidate.CacheKey, resource.candidate)
	if err == nil {
		_ = ctl.cache.update(key, CacheUsable, CarrierObservation{})
		ctl.cache.confirmRecovery(key)
		var pd *pipeDrv
		c.peek(func() { pd = c.pipes[0] })
		if pd == nil {
			resource.endLife()
		} else {
			pd.gs.addAfterIO(resource.endLife)
		}
	} else {
		resource.endLife()
		if resource.attempt.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			ctl.reportAttemptFailure(key, 0)
		}
	}
	resource.releaseAttempt()
	resource.work.releaseCleanup()
	result := bootstrapResult{resource: resource, c: c, err: err}
	select {
	case results <- result:
	case <-ctx.Done():
		if c != nil {
			c.Abort(0, "bootstrap canceled")
		}
	}
}
