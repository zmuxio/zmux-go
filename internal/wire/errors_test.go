package wire

import "testing"

type cyclicError struct {
	next error
}

func (e *cyclicError) Error() string {
	return "cyclic"
}

func (e *cyclicError) Unwrap() error {
	return e.next
}

func TestErrorCodeOfHandlesCyclicUnwrap(t *testing.T) {
	cyclic := &cyclicError{}
	cyclic.next = cyclic

	if code, ok := ErrorCodeOf(cyclic); ok {
		t.Fatalf("ErrorCodeOf(cyclic) = (%v, true), want no code", code)
	}
}
