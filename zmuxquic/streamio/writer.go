// Package streamio coordinates write and close on QUIC-based byte streams.
package streamio

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// Writer orders graceful close after the current write. Abort can interrupt
// a blocked write without waiting for the writer, including while Close waits
// for that write. The zero value is ready to use on one stream.
type Writer struct {
	writing sync.Mutex
	control sync.Mutex
	stopped atomic.Bool
	closed  bool // under control
	aborted bool // under control
}

func (w *Writer) Write(stream io.Writer, p []byte) (int, error) {
	w.writing.Lock()
	defer w.writing.Unlock()
	if w.stopped.Load() {
		return 0, net.ErrClosed
	}
	n, err := stream.Write(p)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// Close waits for the current write and sends the stream FIN once.
func (w *Writer) Close(stream io.Closer) error {
	return w.CloseInterrupting(stream, nil)
}

// CloseInterrupting interrupts a pending write before closing the stream.
// interrupt must be safe to call concurrently with a write or abort.
func (w *Writer) CloseInterrupting(stream io.Closer, interrupt func()) error {
	w.stopped.Store(true)
	if interrupt == nil {
		w.writing.Lock()
	} else if !w.writing.TryLock() {
		interrupt()
		w.writing.Lock()
	}
	defer w.writing.Unlock()
	w.control.Lock()
	defer w.control.Unlock()
	if w.closed || w.aborted {
		return nil
	}
	w.closed = true
	return stream.Close()
}

// Abort runs cancel at most once, even if the send side already sent a FIN.
// cancel must interrupt the underlying stream and return promptly.
func (w *Writer) Abort(cancel func()) { w.abort(cancel, true) }

// AbortOpen interrupts the stream unless it was already closed.
func (w *Writer) AbortOpen(cancel func()) { w.abort(cancel, false) }

func (w *Writer) abort(cancel func(), afterClose bool) {
	w.stopped.Store(true)
	w.control.Lock()
	defer w.control.Unlock()
	if !w.aborted && (afterClose || !w.closed) {
		w.aborted = true
		cancel()
	}
}
