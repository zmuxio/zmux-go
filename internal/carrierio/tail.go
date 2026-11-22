package carrierio

import "context"

// RequestTail is owned by an adapter until its response handler and
// transport-specific stream teardown return. Bind and Finish run in
// order on the request goroutine.
type RequestTail struct {
	// Accept commits a lazily accepted request head during classification.
	Accept   func(context.Context) error
	release  func()
	finished bool
}

func (t *RequestTail) Bind(release func()) {
	if t.finished {
		release()
		return
	}
	t.release = release
}
func (t *RequestTail) Finish() {
	t.finished = true
	if t.release != nil {
		t.release()
		t.release = nil
	}
}
