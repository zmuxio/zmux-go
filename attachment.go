package zmux

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

const (
	setupPending   uint32 = iota
	setupDeciding         // validating under the session lock
	setupBootstrap        // provisional driver owns I/O until publication or refusal
	setupAttached
	setupRejecting
	setupStalled
	setupFallback
	setupClosing
)

// attachment owns one setup reference. Children reserve before taking a
// resource and release after their final access. A failed setup keeps the
// pending slot until the last child (including refusal I/O) retires.
type attachment struct {
	pipe          PipeIO
	handoff       *admissionHandoff // preparation and host dispatch belong to this carrier
	listener      *Listener
	ctx           context.Context
	parentsEnded  func() bool
	stopHandshake <-chan struct{} // Shutdown interrupts a bootstrap already negotiating at the drain cutoff
	state         atomic.Uint32
	refs          atomic.Int32
	slot          *core.PendingSlot
	onEnd         func()
	head          func(context.Context) error
	pair          *attachment // aggregate physical lifetime after split pairing
	done          chan struct{}
}

func newAttachment(l *Listener, observe bool) *attachment {
	a := &attachment{listener: l}
	a.refs.Store(1)
	if observe {
		a.done = make(chan struct{})
	}
	return a
}

func (a *attachment) commit(ctx context.Context, state uint32) error {
	if a.listener != nil {
		a.listener.mu.RLock()
		defer a.listener.mu.RUnlock()
	}
	return a.commitLocked(ctx, setupPending, state)
}

// commitLocked runs while the listener stop cutoff is read- or write-locked.
func (a *attachment) commitLocked(ctx context.Context, from, to uint32) error {
	if err := a.cancellation(ctx); err != nil {
		return err
	}
	if !a.state.CompareAndSwap(from, to) {
		return errRejected
	}
	return nil
}

// commitCarrier runs the pure protocol decision under the caller's
// session lock. Once reserved, setup cancellation cannot close the
// carrier between validation and installation of the driver.
func (a *attachment) commitCarrier(ctx context.Context, accept func() bool) bool {
	if a.commit(ctx, setupDeciding) != nil {
		return false
	}
	if !accept() {
		a.state.Store(setupClosing)
		return false
	}
	a.state.Store(setupAttached)
	return true
}

func (a *attachment) cancellation(ctx context.Context) error {
	if a.listener != nil && a.listener.stopCtx.Err() != nil {
		return net.ErrClosed
	}
	if a.ctx != nil {
		if err := a.ctx.Err(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.parentsEnded != nil && a.parentsEnded() {
		return context.Canceled
	}
	return nil
}

func (a *attachment) commitFallback(ctx context.Context) error {
	if err := a.commit(ctx, setupFallback); err != nil {
		return err
	}
	a.leave()
	return nil
}

// inspect binds even validation and admission to the listener stop scope.
// A callback which blocks on carrier I/O is interrupted through the same
// closer as a later classification failure.
func (a *attachment) inspect(ctx context.Context, f func() error) error {
	bound, unlink := a.listener.stopContext(ctx)
	defer unlink()
	if a.listener.stopCtx.Err() != nil {
		return net.ErrClosed
	}
	if bound.Err() != nil {
		return bound.Err()
	}
	stop := a.watch(bound, a.pipe)
	defer stop()
	err := f()
	if state := a.state.Load(); state != setupFallback && state != setupStalled {
		if a.listener.stopCtx.Err() != nil {
			return net.ErrClosed
		}
		if bound.Err() != nil {
			return bound.Err()
		}
	}
	return err
}

func (a *attachment) rollback() bool {
	if a.state.CompareAndSwap(setupPending, setupClosing) {
		return true
	}
	// A provisional driver sends the close on its own.
	a.state.CompareAndSwap(setupBootstrap, setupClosing)
	return false
}

// watch interrupts setup I/O only while this attempt still owns it.
// Joining an already running cancellation avoids publishing completion
// during a host Close.
func (a *attachment) watch(ctx context.Context, p PipeIO) func() {
	cancel := func() {
		if a.state.CompareAndSwap(setupPending, setupClosing) {
			closePipe(p)
		}
	}

	a.reserve()
	ended := make(chan struct{})
	s := context.AfterFunc(ctx, func() { defer close(ended); defer a.release(); cancel() })
	return func() {
		if s() {
			a.release()
		} else {
			<-ended
		}
	}
}

// interruptOnCancel borrows the setup reference for an I/O interrupt.
// Stopping it joins a callback that already started, so setup never
// releases the last reference while the interrupt is in host code.
func interruptOnCancel(ctx context.Context, f func()) func() bool {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); f() })
	return func() bool {
		if stop() {
			return true
		}
		<-done
		return false
	}
}

// reserve borrows a live reference. Zero is terminal, even for late observers.
func (a *attachment) reserve() bool { return a.reserveRefs(1) }

func (a *attachment) reserveRefs(count int32) bool {
	for n := a.refs.Load(); n > 0; n = a.refs.Load() {
		if a.refs.CompareAndSwap(n, n+count) {
			return true
		}
	}
	return false
}
func (a *attachment) leave() {
	if a.slot != nil {
		a.listener.leave(a.slot)
		a.slot = nil
	}
}
func (a *attachment) release() {
	if a.refs.Add(-1) != 0 {
		return
	}
	if a.handoff != nil {
		a.handoff.end(a.listener)
	}
	a.leave()
	if a.onEnd != nil {
		a.onEnd()
	}
	if a.done != nil {
		close(a.done)
	}
}
func (a *attachment) attach(pd *pipeDrv) {
	if a != nil {
		a.reserve()
		pd.hasOwner = true
		pd.gs.add() // close owner; handed back once host teardown returns
		pd.gs.afterIO = a.release
	}
}

// stall transfers the reserved capacity to bounded work. Eviction only
// cancels its context. All carrier access takes place outside quota locks.
func (a *attachment) stall(ctx context.Context, d time.Duration, variant *string, async bool, work func(context.Context)) bool {
	l := a.listener
	lifetime, unlink := l.stopContext(ctx)
	bounded, cancel := timeshim.WithTimeout(lifetime, l.clk, d)
	l.pmu.Lock()
	held := l.pending.Stall(a.slot, cancel)
	l.pmu.Unlock()
	if !held || a.commit(ctx, setupStalled) != nil {
		if held {
			l.pmu.Lock()
			l.pending.Unstall(a.slot)
			l.pmu.Unlock()
		}
		if variant != nil {
			if held || ctx.Err() != nil || l.stopCtx.Err() != nil {
				l.st.adm.count(Decision{Verdict: VerdictClose, Variant: *variant})
			} else {
				l.st.adm.stallOverflow.Add(1)
				l.st.adm.variant(*variant, VerdictStall)
			}
		}
		cancel()
		unlink()
		return false
	}
	if variant != nil {
		l.st.adm.stalled.Add(1)
		l.st.adm.variant(*variant, VerdictStall)
	}
	a.reserve()
	run := func() {
		defer a.release()
		defer unlink()
		defer cancel()
		defer func() { l.pmu.Lock(); l.pending.Unstall(a.slot); l.pmu.Unlock() }()
		work(bounded)
	}
	if async {
		labels.Go(ctx, "stall", run)
	} else {
		run()
	}
	return true
}

// acceptHead commits a lazy HTTP response only once classification has
// taken this carrier. The caller still owns the setup reference.
func (a *attachment) acceptHead(ctx context.Context) error {
	if a == nil || a.head == nil {
		return nil
	}
	f := a.head
	a.head = nil
	bound, cancel := timeshim.WithTimeout(ctx, a.listener.clk, a.listener.cfg.Config.handshakeTimeout())
	defer cancel()
	if err := f(bound); err != nil {
		return err
	}
	return bound.Err()
}
