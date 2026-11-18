package zmux

import (
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

// deadline is the per-handle deadline of net.Conn semantics: a channel that
// is closed when the deadline passes and replaced when it is moved (the
// pipeDeadline pattern of package net). Its lock is a leaf (L3).
type deadline struct {
	timer  timeshim.Timer
	cancel chan struct{}
	// expired, if set, is called once the deadline passes, outside mu
	// (the read deadline's: NotifyReadable). Set before the deadline is
	// published and never changed.
	expired func()
	mu      sync.Mutex
}

// set moves the deadline to t on clk.
func (d *deadline) set(t time.Time, clk timeshim.Clock) {
	if d.setLocked(t, clk) && d.expired != nil {
		d.expired()
	}
}

// setLocked moves the deadline under mu and reports whether it has passed
// already.
func (d *deadline) setLocked(t time.Time, clk timeshim.Clock) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // wait for the timer callback to close it
	}
	d.timer = nil
	closed := d.cancel != nil && isClosed(d.cancel)
	if t.IsZero() {
		if closed || d.cancel == nil {
			d.cancel = make(chan struct{})
		}
		return false
	}
	if closed || d.cancel == nil {
		d.cancel = make(chan struct{})
	}
	if dur := timeshim.Until(clk, t); dur > 0 {
		ch, expired := d.cancel, d.expired
		d.timer = clk.AfterFunc(dur, func() {
			close(ch)
			if expired != nil {
				expired()
			}
		})
		return false
	}
	close(d.cancel)
	return true
}

// wait returns the channel to select on.
func (d *deadline) wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel == nil {
		d.cancel = make(chan struct{})
	}
	return d.cancel
}

// stop releases the timer.
func (d *deadline) stop() {
	d.mu.Lock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.mu.Unlock()
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
