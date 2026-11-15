package zmuxhttp

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// An uplink whose response never comes is cancelled once its body ended
// and the linger bound passed; a bidirectional carrier's request, whose
// response is its downlink, is left to its read half.
func TestReqWriterLingerOnlyForUplinks(t *testing.T) {
	for _, linger := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			var cancelled atomic.Bool
			call := &httpCall{done: make(chan struct{})}
			w := &httpReqWriter{p: newBodyPipe(), call: call, cancel: func() { cancelled.Store(true) }, linger: linger}
			_ = w.Close()
			time.Sleep(reqLinger + time.Second)
			synctest.Wait()
			if cancelled.Load() != linger {
				t.Fatalf("linger=%v: cancelled=%v", linger, cancelled.Load())
			}
			close(call.done)
		})
	}
}
