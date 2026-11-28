package zmux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// A join owns one immutable result and its absolute confirmation deadline.
// All competitors settle under the session lock. Hook delivery owns no
// session worker and releases its private event keys on return.
type joinOutcome struct {
	deadline         time.Time
	parent           context.Context
	err              error
	attempt          *Attempt
	keys             *joinEventHold
	c                *Conn
	end              *pipeEnd
	done             chan struct{}
	cancellation     func() error
	index            int
	id               PipeID
	settled, inbound bool
}

type joinEventHold struct {
	queue           eventQueue
	retired, failed bool
}

func (c *Conn) newJoinOutcome(parent context.Context, id PipeID, inbound bool, deadline time.Time) *joinOutcome {
	return &joinOutcome{c: c, id: id, parent: parent, inbound: inbound, done: make(chan struct{}), deadline: deadline}
}
func (o *joinOutcome) holdLocked(id uint64, end *pipeEnd) {
	o.id, o.end, end.join = PipeID(id), end, o
	if o.c.cfg.JoinOutcome == nil {
		return
	}
	if o.c.events.held == nil {
		o.c.events.held = make(map[uint64]*joinEventHold)
	}
	o.keys = new(joinEventHold)
	o.c.events.held[id] = o.keys
}
func (o *joinOutcome) deadlineAt() time.Time {
	if o.attempt != nil {
		return o.attempt.deadlineAt()
	}
	return o.deadline
}

func (o *joinOutcome) reasonLocked(err error) error {
	return o.reasonAtLocked(err, o.c.clk.source().Now())
}

func (o *joinOutcome) reasonAtLocked(err error, now time.Time) error {
	c := o.c
	switch {
	case o.inbound && c.lst != nil && c.lst.stopCtx.Err() != nil:
		return net.ErrClosed
	case c.closing || c.s.Err() != nil:
		return c.errLocked()
	}
	if o.cancellation != nil {
		if cancel := o.cancellation(); cancel != nil {
			return cancel
		}
	}
	switch {
	case o.parent != nil && o.parent.Err() != nil:
		err = o.parent.Err()
	case errors.Is(err, ErrJoinTimeout) || !now.Before(o.deadlineAt()):
		err = fmt.Errorf("%w: %w", ErrJoinTimeout, context.DeadlineExceeded)
	}

	return err
}
func (o *joinOutcome) settleLocked(err error) error {
	if !o.settled {
		err = o.reasonLocked(err)
		o.finishLocked(err)
	}
	return o.err
}
func (o *joinOutcome) finishLocked(err error) {
	if err != nil && o.end != nil && !o.inbound {
		o.c.s.AbortUnconfirmed(uint64(o.id))
	}
	o.publishLocked(err)
}

// publishLocked records the result without changing protocol state.
func (o *joinOutcome) publishLocked(err error) {
	o.settled, o.err, o.parent, o.cancellation = true, err, nil, nil
	close(o.done)
	if o.attempt != nil {
		o.attempt.attachedLocked(o.index, err)
	}
	if o.c.cfg.JoinOutcome != nil {
		go o.deliver()
	}
}

// A reply-only caller releases its context at handoff. The pipe keeps
// the original deadline even without an observer.
func (o *joinOutcome) repliedLocked() error {
	if o.settled {
		return o.err
	}
	if err := o.reasonLocked(nil); err != nil {
		o.finishLocked(err)
		return err
	}
	o.parent = nil
	return nil
}
func (o *joinOutcome) deliver() {
	c := o.c
	if keys := o.keys; keys != nil {
		defer c.locked(func() {
			q := &c.events
			delete(q.held, uint64(o.id))
			o.keys = nil
			for n := keys.queue.head; n != nil; n = n.next {
				q.merge(n.key, n.ev)
			}
			if keys.retired {
				q.foldRetired(uint64(o.id), keys.failed)
			}
			for _, w := range q.waiters {
				w.Fire()
			}
			clear(q.waiters)
			q.waiters = q.waiters[:0]
		})
	}
	safeLog(c.cfg.Logger, "Config.JoinOutcome", func() { c.cfg.JoinOutcome(c, o.id, o.err) })
}
