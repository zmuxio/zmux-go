package zmux

import (
	"net"

	"github.com/zmuxio/zmux-go/internal/core"
)

type streamHandler struct {
	call           func(*handle)
	limit, running int
}

type connHandlers struct {
	event                          func(Event)
	bidi, uni                      streamHandler
	eventsRegistered, eventRunning bool
}

func (h *connHandlers) stream(bidi bool) *streamHandler {
	if bidi {
		return &h.bidi
	}
	return &h.uni
}

func (c *Conn) handlersLocked() *connHandlers {
	if c.handlers == nil {
		c.handlers = new(connHandlers)
	}
	return c.handlers
}

// HandleStreams assigns queued and future peer bidirectional streams to f in
// accept order. Each call runs in its own goroutine, with at most maxConcurrent
// calls outstanding. The callback owns its stream and may retain it after return. A panic cancels
// both directions with StreamCancelled. Registration is permanent and excludes
// AcceptStream. Repeated registration and blocked or later AcceptStream calls
// return ErrHandlerRegistered. Wait does not wait for callbacks.
func (c *Conn) HandleStreams(f func(*Stream), maxConcurrent int) error {
	if c == nil {
		return net.ErrClosed
	}
	if f == nil || maxConcurrent < 1 {
		return ErrInvalidConfig
	}
	return c.registerStreams(true, maxConcurrent, func(h *handle) {
		st := newStream(h)
		if hostCall(c.cfg.Logger, "HandleStreams", func() error { f(st); return nil }) != nil {
			st.CancelRead(StreamCancelled)
			st.CancelWrite(StreamCancelled)
		}
	})
}

// HandleUniStreams is HandleStreams for incoming unidirectional streams. It
// activates staged uni credit as AcceptUniStream does and excludes that method.
// A panic cancels the read direction with StreamCancelled.
func (c *Conn) HandleUniStreams(f func(*ReceiveStream), maxConcurrent int) error {
	if c == nil {
		return net.ErrClosed
	}
	if f == nil || maxConcurrent < 1 {
		return ErrInvalidConfig
	}
	return c.registerStreams(false, maxConcurrent, func(h *handle) {
		st := newReceiveStream(h)
		if hostCall(c.cfg.Logger, "HandleUniStreams", func() error { f(st); return nil }) != nil {
			st.CancelRead(StreamCancelled)
		}
	})
}

func (c *Conn) registerStreams(bidi bool, limit int, call func(*handle)) error {
	c.lock()
	defer c.unlock()
	if c.handlers != nil && c.handlers.stream(bidi).limit != 0 {
		return ErrHandlerRegistered
	}
	if c.closing || c.s.State() >= core.StateClosing {
		return c.errLocked()
	}
	h := c.handlersLocked().stream(bidi)
	h.call, h.limit = call, limit
	c.s.WakeAcceptWaiters(bidi)
	return nil
}

func (c *Conn) collectStreamsLocked() (work []func()) {
	for _, bidi := range [...]bool{true, false} {
		h := c.handlers.stream(bidi)
		for h.running < h.limit {
			st, ok := c.s.AcceptStream(bidi)
			if !ok {
				break
			}
			handle, call := c.newHandle(st), h.call
			h.running++
			work = append(work, func() {
				defer c.locked(func() { h.running-- })
				call(handle)
			})
		}
	}
	return work
}

// HandleEvents exclusively consumes NextEvent's keyed queue. It calls f
// serially, removing one event immediately before each call. A panic consumes
// that event and delivery continues. After close, buffered events can still be
// claimed and drained. Once the frozen queue is empty, a first registration
// returns the terminal error. Repeated registration and blocked or later NextEvent
// calls return ErrHandlerRegistered. Wait does not wait for f.
func (c *Conn) HandleEvents(f func(Event)) error {
	if c == nil {
		return net.ErrClosed
	}
	if f == nil {
		return ErrInvalidConfig
	}
	c.lock()
	defer c.unlock()
	if c.handlers != nil && c.handlers.eventsRegistered {
		return ErrHandlerRegistered
	}
	if c.events.frozen && c.events.head == nil && len(c.events.held) == 0 {
		return c.errLocked()
	}
	h := c.handlersLocked()
	h.event, h.eventsRegistered = f, true
	c.s.EnablePathAdvice()
	return nil
}

// StopEvents unregisters HandleEvents and releases the advice subscription
// established by NextEvent. A dispatched callback may finish, and the keyed
// queue remains available. A new NextEvent or HandleEvents call subscribes again.
// Independently configured Redial and Tracer callbacks keep receiving advice.
func (c *Conn) StopEvents() error {
	if c == nil {
		return net.ErrClosed
	}
	c.lock()
	defer c.unlock()
	c.events.pollAdvice = false
	if h := c.handlers; h != nil {
		h.event, h.eventsRegistered = nil, false
	}
	if c.cfg.Redial == nil && (c.cfg.Tracer == nil || c.cfg.Tracer.Event == nil) {
		c.s.DisablePathAdvice()
	}
	return nil
}

func (c *Conn) prepareEventsLocked() bool {
	h := c.handlers
	if h == nil || h.event == nil {
		return false
	}
	if c.events.head == nil {
		if c.events.frozen && len(c.events.held) == 0 {
			h.event = nil
		}
		return false
	}
	if h.eventRunning {
		return false
	}
	h.eventRunning = true
	return true
}

func (c *Conn) deliverEvents() {
	// Goexit in a callback also returns the worker slot. The unlock checks
	// the queue again while deciding whether to start a successor.
	defer c.locked(func() { c.handlers.eventRunning = false })
	for {
		c.lock()
		f := c.handlers.event
		if f == nil {
			c.unlock()
			return
		}
		ev, ok := c.events.pop()
		if !ok {
			c.unlock()
			return
		}
		c.unlock()
		_ = hostCall(c.cfg.Logger, "HandleEvents", func() error { f(ev); return nil })
	}
}
