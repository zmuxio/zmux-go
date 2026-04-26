package zmux

import (
	"errors"
	"fmt"
	"testing"
)

type cyclicError struct {
	next error
}

func (e *cyclicError) Error() string {
	return "cyclic error"
}

func (e *cyclicError) Unwrap() error {
	return e.next
}

func TestFindErrorHandlesCyclicUnwrap(t *testing.T) {
	cyclic := &cyclicError{}
	cyclic.next = cyclic

	if appErr, ok := findError[*ApplicationError](cyclic); ok {
		t.Fatalf("findError returned %v, want no match for cyclic unwrap", appErr)
	}
}

func TestIsErrorHandlesCyclicUnwrap(t *testing.T) {
	cyclic := &cyclicError{}
	cyclic.next = cyclic

	if isError(cyclic, ErrSessionClosed) {
		t.Fatal("isError matched ErrSessionClosed through cyclic unwrap")
	}
}

func TestFindErrorFindsWrappedApplicationError(t *testing.T) {
	want := &ApplicationError{Code: uint64(CodeCancelled), Reason: "stop"}
	err := fmt.Errorf("outer: %w", errors.Join(errors.New("noise"), want))

	got, ok := findError[*ApplicationError](err)
	if !ok {
		t.Fatal("findError did not find wrapped ApplicationError")
	}
	if got.Code != want.Code || got.Reason != want.Reason {
		t.Fatalf("findError = %#v, want %#v", got, want)
	}
}
