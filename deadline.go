package zmux

import (
	"sync"
	"time"
)

// deadline is the per-handle deadline of net.Conn semantics: a channel that
// is closed when the deadline passes and replaced when it is moved (the
// pipeDeadline pattern of package net). Its lock is a leaf (L3).
type deadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
}

func (d *deadline) set(t time.Time) {
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
		return
	}
	if closed || d.cancel == nil {
		d.cancel = make(chan struct{})
	}
	if dur := time.Until(t); dur > 0 {
		ch := d.cancel
		d.timer = time.AfterFunc(dur, func() { close(ch) })
		return
	}
	close(d.cancel)
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
