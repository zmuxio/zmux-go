package zmux

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// attemptDeadline is shared by the opener, Join and the confirmation fence.
// The session owner writes the deadline and publishes its terminal state.
type attemptDeadline struct {
	values context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}
	at     atomic.Pointer[time.Time]
	err    error // published by closing done
}

func newAttemptDeadline(parent context.Context, deadline time.Time) *attemptDeadline {
	values, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	ctx := &attemptDeadline{values: values, cancel: cancel, done: make(chan struct{})}
	ctx.at.Store(&deadline)
	return ctx
}

func (ctx *attemptDeadline) Deadline() (time.Time, bool) { return *ctx.at.Load(), true }
func (ctx *attemptDeadline) Done() <-chan struct{}       { return ctx.done }
func (ctx *attemptDeadline) Value(key any) any           { return ctx.values.Value(key) }

func (ctx *attemptDeadline) Err() error {
	select {
	case <-ctx.done:
		return ctx.err
	default:
		return nil
	}
}

// AfterFunc propagates Err to context children after the cause is published.
func (ctx *attemptDeadline) AfterFunc(f func()) func() bool {
	return context.AfterFunc(ctx.values, func() {
		<-ctx.done
		f()
	})
}

func (ctx *attemptDeadline) end(err error) {
	ctx.err = err
	ctx.cancel(err)
	close(ctx.done)
}

func boundedAttemptDeadline(ctx context.Context, origin, reservedAt time.Time, timeout time.Duration) time.Time {
	const latest = core.Never - 1
	end := core.Time(reservedAt.Sub(origin)).Add(core.Duration(timeout))
	deadline := origin.Add(time.Duration(min(end, latest)))
	if caller, ok := ctx.Deadline(); ok {
		deadline = earliest(deadline, caller)
	}
	return deadline
}

func (c *Conn) attemptDeadlineLocked(tx *CapacityChange, reservedAt time.Time) (time.Time, error) {
	now := c.clk.source().Now()
	if reservedAt.IsZero() {
		reservedAt = now
	}
	due := earliest(boundedAttemptDeadline(tx.ctx, c.clk.base, reservedAt, tx.trialTimeout), tx.deadline)
	if grace := c.s.RecoveryGraceDeadline(); grace != core.Never {
		due = earliest(due, c.clk.wall(grace.Add(-core.Duration(c.clk.domain.resolution()))))
	}
	if due.IsZero() || !due.After(now) {
		return time.Time{}, context.DeadlineExceeded
	}
	return due, nil
}

func (a *Attempt) deadlineAt() time.Time {
	deadline, _ := a.ctx.Deadline()
	return deadline
}

func (a *Attempt) armDeadlineLocked(now time.Time) {
	c := a.tx.c
	deadline := a.deadlineAt()
	if a.deadlineTimer != nil {
		a.deadlineTimer.Stop()
	}
	a.deadlineTimer = c.clk.afterFunc(deadline.Sub(now), func() {
		c.locked(func() {
			if a.done || !a.deadlineAt().Equal(deadline) {
				return
			}
			if now := c.clk.source().Now(); now.Before(deadline) {
				a.armDeadlineLocked(now)
				return
			}
			a.finishLocked(ReasonDeadline, context.DeadlineExceeded, 0)
		})
	})
}

// An attempt can keep its earlier bound through any subsequent resume.
func (c *Conn) tightenAttemptDeadlinesLocked(grace core.Time) {
	if grace == core.Never {
		return
	}
	end := grace.Add(-core.Duration(c.clk.domain.resolution()))
	deadline := c.clk.wall(end)
	now := c.clk.source().Now()
	for _, tx := range c.changes {
		for _, a := range tx.attempts {
			if a == nil || a.done || !deadline.Before(a.deadlineAt()) {
				continue
			}
			a.ctx.at.Store(&deadline)
			if !deadline.After(now) {
				a.finishLocked(ReasonDeadline, context.DeadlineExceeded, 0)
				continue
			}
			for _, attachment := range a.view.Attachments {
				if attachment.Pipe != UnassignedPipeID {
					c.s.TightenJoinDeadline(uint64(attachment.Pipe), end)
				}
			}
			a.armDeadlineLocked(now)
		}
	}
}
